//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
//
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Moved from `src/catalog.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{
    BlockId, CatalogVersion, ColumnId, ErrorKind, GenerationId, Lsn, ObjectId, PackId, PageId,
    RowId, SchemaId, SegmentId,
};
use plomid_storage::catalog::{
    catalog_file_name, generation_from_catalog_file_name, CatalogState, ObjectRecord,
    PhysicalReference, PhysicalStructure, PublicationState, SchemaColumn, SchemaMetadata,
    CATALOG_HEADER_SIZE, CATALOG_MAGIC, MAX_SCHEMA_COLUMNS, OFF_CATALOG_VERSION, OFF_CHECKSUM,
    OFF_HEADER_LEN, OFF_PUBLICATION, OFF_RECORDS_LEN, OFF_RECORD_COUNT, OFF_RESERVED,
    OFF_STORAGE_GENERATION,
};

fn structure(page: u64) -> PhysicalStructure {
    PhysicalStructure::new(
        SegmentId::new(1),
        PackId::new(1),
        BlockId::new(1),
        PageId::new(page),
        Some(RowId::new(page)),
    )
}

fn reference(object: u64, generation: u64, page: u64) -> PhysicalReference {
    PhysicalReference::new(
        ObjectId::new(object),
        GenerationId::new(generation),
        structure(page),
    )
}

fn schema(version: u64) -> SchemaMetadata {
    SchemaMetadata::new(
        SchemaId::new(7),
        CatalogVersion::new(version),
        vec![
            SchemaColumn {
                column_id: ColumnId::new(1),
                type_code: 23,
            },
            SchemaColumn {
                column_id: ColumnId::new(2),
                type_code: 25,
            },
        ],
    )
    .expect("schema")
}

fn record(object: u64, current: u64, retained: &[u64]) -> ObjectRecord {
    ObjectRecord::new(
        ObjectId::new(object),
        schema(1),
        GenerationId::new(current),
        retained.iter().copied().map(GenerationId::new).collect(),
        vec![reference(object, current, 1)],
        PublicationState::Published,
    )
    .expect("record")
}

fn state(records: Vec<ObjectRecord>) -> CatalogState {
    CatalogState::staged(
        CatalogVersion::new(1),
        GenerationId::new(9),
        GenerationId::new(1),
        Lsn::new(3),
        records,
    )
    .expect("state")
    .as_published()
}

#[test]
fn records_normalize_into_deterministic_order() {
    let first = ObjectRecord::new(
        ObjectId::new(1),
        schema(1),
        GenerationId::new(5),
        vec![
            GenerationId::new(3),
            GenerationId::new(2),
            GenerationId::new(3),
            GenerationId::new(5),
        ],
        vec![reference(1, 5, 2), reference(1, 5, 1), reference(1, 5, 2)],
        PublicationState::Published,
    )
    .expect("record");
    assert_eq!(
        first
            .retained_generations
            .iter()
            .map(|generation| generation.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(first.references.len(), 2);
    assert_eq!(first.references[0].structure.page_id.get(), 1);
    assert_eq!(first.generation_ids().len(), 3);
}

#[test]
fn encoding_is_deterministic_and_round_trips() {
    let catalog = state(vec![record(2, 4, &[3]), record(1, 7, &[])]);
    let encoded = catalog.encode().expect("encode");
    assert_eq!(encoded, catalog.encode().expect("encode"));
    assert_eq!(CatalogState::decode(&encoded).expect("decode"), catalog);
    // Records are ordered by object id regardless of insertion order.
    let first = encoded[CATALOG_HEADER_SIZE + 4..CATALOG_HEADER_SIZE + 12].to_vec();
    assert_eq!(u64::from_le_bytes(first.try_into().expect("bytes")), 1);
}

#[test]
fn staged_and_published_states_are_distinguishable() {
    let staged = state(vec![record(1, 2, &[])]).as_staged();
    assert!(!staged.is_published());
    let encoded = staged.encode().expect("encode");
    assert_eq!(
        CatalogState::decode(&encoded)
            .expect("decode")
            .publication(),
        PublicationState::Staged
    );
}
#[test]
fn rejects_corruption_matrix() {
    let valid = state(vec![record(1, 2, &[])]).encode().expect("encode");
    let cases: [(&str, usize, u8); 6] = [
        ("magic", 0, b'X'),
        ("version", OFF_CATALOG_VERSION, 0xFF),
        ("header length", OFF_HEADER_LEN, 0x7F),
        ("publication", OFF_PUBLICATION, 0x7F),
        ("record count", OFF_RECORD_COUNT, 0xFF),
        ("records length", OFF_RECORDS_LEN, 0xFF),
    ];
    for (label, offset, value) in cases {
        let mut bytes = valid.clone();
        bytes[offset] = value;
        assert!(
            CatalogState::decode(&bytes).is_err(),
            "corrupt {label} must be rejected"
        );
    }
    let mut bytes = valid.clone();
    bytes[OFF_CHECKSUM] ^= 0xFF;
    assert!(CatalogState::decode(&bytes).is_err());
    let mut bytes = valid.clone();
    bytes[OFF_RESERVED] = 1;
    assert!(CatalogState::decode(&bytes).is_err());
    let mut bytes = valid.clone();
    bytes[OFF_STORAGE_GENERATION..OFF_STORAGE_GENERATION + 8].copy_from_slice(&0_u64.to_le_bytes());
    assert!(CatalogState::decode(&bytes).is_err());
    assert!(CatalogState::decode(&valid[..10]).is_err());
    assert!(CatalogState::decode(&valid[..CATALOG_HEADER_SIZE + 1]).is_err());
    let mut bytes = valid.clone();
    bytes.push(0);
    assert!(CatalogState::decode(&bytes).is_err());
    // A record whose declared length disagrees with its content.
    let mut bytes = valid.clone();
    let record_len = CATALOG_HEADER_SIZE + 4;
    bytes[record_len - 4..record_len].copy_from_slice(&16_u32.to_le_bytes());
    assert!(CatalogState::decode(&bytes).is_err());
}

#[test]
fn inconsistent_record_relationships_are_rejected() {
    let mut corrupted = record(1, 2, &[]);
    corrupted.current_generation = GenerationId::new(0);
    assert!(corrupted.validate().is_err());

    let mut corrupted = record(1, 2, &[]);
    corrupted.retained_generations = vec![GenerationId::new(2)];
    assert!(corrupted.validate().is_err());

    let mut corrupted = record(1, 2, &[3]);
    corrupted.references = vec![reference(1, 4, 1)];
    assert!(corrupted.validate().is_err());

    let mut corrupted = record(1, 2, &[]);
    corrupted.references = vec![reference(2, 2, 1)];
    assert!(corrupted.validate().is_err());

    let mut corrupted = record(1, 2, &[]);
    corrupted.publication = PublicationState::Staged;
    assert!(corrupted.validate().is_err());

    let updated = state(vec![record(1, 2, &[])])
        .upsert(record(1, 5, &[2]))
        .expect("upsert");
    assert_eq!(
        updated.generation_of(ObjectId::new(1)),
        Some(GenerationId::new(5))
    );
    assert!(updated.owns_generation(GenerationId::new(2)));
}
#[test]
fn schema_validation_rejects_duplicate_columns() {
    let error = SchemaMetadata::new(
        SchemaId::new(1),
        CatalogVersion::new(1),
        vec![
            SchemaColumn {
                column_id: ColumnId::new(4),
                type_code: 1,
            },
            SchemaColumn {
                column_id: ColumnId::new(4),
                type_code: 2,
            },
        ],
    )
    .expect_err("duplicate column identifiers must be rejected");
    assert!(matches!(
        error.kind(),
        ErrorKind::InvalidArgument | ErrorKind::Corruption
    ));

    let columns = (0..(MAX_SCHEMA_COLUMNS as u64 + 1))
        .map(|index| SchemaColumn {
            column_id: ColumnId::new(index + 1),
            type_code: 1,
        })
        .collect::<Vec<_>>();
    assert!(SchemaMetadata::new(SchemaId::new(1), CatalogVersion::new(1), columns).is_err());
}

#[test]
fn physical_reference_validation_matches_physical_rules() {
    assert!(structure(1).validate().is_ok());
    assert!(PhysicalStructure::new(
        SegmentId::new(0),
        PackId::new(1),
        BlockId::new(1),
        PageId::new(1),
        None,
    )
    .validate()
    .is_err());
    assert!(PhysicalStructure::new(
        SegmentId::new(1),
        PackId::new(1),
        BlockId::new(1),
        PageId::new(1),
        Some(RowId::new(0)),
    )
    .validate()
    .is_err());
}

#[test]
fn release_and_retirement_are_explicit_state_changes() {
    let catalog = state(vec![record(1, 5, &[3, 4])]);
    let released = catalog
        .clone()
        .release_generation(GenerationId::new(4))
        .expect("release");
    assert_eq!(
        released
            .object(ObjectId::new(1))
            .expect("object")
            .retained_generations
            .len(),
        1
    );
    assert!(catalog
        .clone()
        .release_generation(GenerationId::new(5))
        .is_err());
    assert!(catalog.release_generation(GenerationId::new(8)).is_err());
    let retired = state(vec![record(1, 5, &[])])
        .retire_object(ObjectId::new(1))
        .expect("retire");
    assert!(!retired
        .object(ObjectId::new(1))
        .expect("object")
        .is_published());
    assert!(state(vec![record(1, 5, &[])])
        .remove_object(ObjectId::new(1))
        .expect("remove")
        .records()
        .is_empty());
}

#[test]
fn catalog_file_names_are_strict() {
    assert_eq!(catalog_file_name(3), "catalog-00000000000000000003.cat");
    assert_eq!(
        generation_from_catalog_file_name(&catalog_file_name(3)),
        Some(3)
    );
    for name in [
        "catalog-00000000000000000003.cat.tmp",
        "catalog-3.cat",
        "catalog-00000000000000000000.cat",
        "catalog-00000000000000000003.ckpt",
        "notes.txt",
    ] {
        assert_eq!(generation_from_catalog_file_name(name), None, "{name}");
    }
    assert_eq!(&CATALOG_MAGIC, b"PLCT");
}

use plomid_storage::catalog::{checksum_of, CHECKSUMMED_PREFIX_LEN};
use plomid_storage::catalog::{GenerationLifecycle, LIFECYCLE_TRAILER_VERSION};
use plomid_storage::codec::{put_u32, put_u64, Cursor};
use std::collections::BTreeMap;

#[test]
fn lifecycle_walks_the_full_chain_and_rejects_illegal_jumps() {
    let mut tracked = record(1, 7, &[5, 6]);
    let current = GenerationId::new(7);
    let retained = GenerationId::new(6);
    // Legacy defaults before any entry is materialized.
    assert_eq!(
        tracked.lifecycle_of(current).expect("current"),
        GenerationLifecycle::Active
    );
    assert_eq!(
        tracked.lifecycle_of(retained).expect("retained"),
        GenerationLifecycle::Sealed
    );
    // Full legal walk to the terminal state.
    for state in [
        GenerationLifecycle::Sealed,
        GenerationLifecycle::Columnized,
        GenerationLifecycle::Compactable,
        GenerationLifecycle::Compact,
        GenerationLifecycle::Retire,
        GenerationLifecycle::Reclaim,
    ] {
        tracked.set_lifecycle(retained, state).expect("legal step");
        assert_eq!(tracked.lifecycle_of(retained).expect("state"), state);
    }
    assert!(GenerationLifecycle::Reclaim.is_terminal());
    assert!(!GenerationLifecycle::Active.is_terminal());
    // The terminal state accepts nothing further.
    assert!(tracked
        .set_lifecycle(retained, GenerationLifecycle::Active)
        .is_err());
    // Illegal jumps are rejected and change nothing.
    let mut fresh = record(1, 7, &[5]);
    assert!(fresh
        .set_lifecycle(GenerationId::new(7), GenerationLifecycle::Compact)
        .is_err());
    assert_eq!(
        fresh.lifecycle_of(GenerationId::new(7)).expect("unchanged"),
        GenerationLifecycle::Active
    );
    assert!(fresh
        .set_lifecycle(GenerationId::new(5), GenerationLifecycle::Compact)
        .is_err());
    assert!(fresh
        .set_lifecycle(GenerationId::new(5), GenerationLifecycle::Retire)
        .is_err());
    // Unknown generations are rejected, not defaulted.
    assert!(fresh.lifecycle_of(GenerationId::new(99)).is_err());
    assert!(fresh
        .set_lifecycle(GenerationId::new(99), GenerationLifecycle::Sealed)
        .is_err());
    // Display names every state for operator-facing output.
    assert_eq!(GenerationLifecycle::Compactable.to_string(), "Compactable");
}

#[test]
fn lifecycle_fast_path_and_convenience_methods_agree() {
    let mut tracked = record(1, 7, &[5]);
    let retained = GenerationId::new(5);
    // Sealed → Compactable skips Columnized through the documented fast path.
    tracked.seal_generation(retained).expect("seal");
    tracked.mark_compactable(retained).expect("fast path");
    assert_eq!(
        tracked.lifecycle_of(retained).expect("state"),
        GenerationLifecycle::Compactable
    );
    // Every convenience method enforces the same transition table.
    let mut stepped = record(1, 7, &[5]);
    stepped.seal_generation(retained).expect("seal");
    assert!(stepped.mark_compact(retained).is_err());
    assert!(stepped.mark_retire(retained).is_err());
    assert!(stepped.mark_reclaim(retained).is_err());
    stepped.mark_columnized(retained).expect("columnize");
    stepped.mark_compactable(retained).expect("compactable");
    stepped.mark_compact(retained).expect("compact");
    stepped.mark_retire(retained).expect("retire");
    stepped.mark_reclaim(retained).expect("reclaim");
    assert_eq!(
        stepped.lifecycle_of(retained).expect("terminal"),
        GenerationLifecycle::Reclaim
    );
}

#[test]
fn lifecycle_round_trips_through_encode_and_decode() {
    let mut original = record(1, 7, &[5, 6]);
    original
        .seal_generation(GenerationId::new(5))
        .expect("seal");
    original
        .set_lifecycle(GenerationId::new(6), GenerationLifecycle::Columnized)
        .expect("columnize");
    let catalog = state(vec![original.clone()]);
    let encoded = catalog.encode().expect("encode");
    let decoded = CatalogState::decode(&encoded).expect("decode");
    assert_eq!(decoded, catalog);
    let rec = decoded.object(ObjectId::new(1)).expect("record");
    assert_eq!(
        rec.lifecycle_of(GenerationId::new(5)).expect("sealed"),
        GenerationLifecycle::Sealed
    );
    assert_eq!(
        rec.lifecycle_of(GenerationId::new(6)).expect("columnized"),
        GenerationLifecycle::Columnized
    );
    // A record without lifecycle state encodes exactly like a legacy image.
    let legacy = state(vec![record(1, 7, &[5])]);
    let bytes = legacy.encode().expect("encode");
    let revived = CatalogState::decode(&bytes).expect("decode");
    assert!(revived
        .object(ObjectId::new(1))
        .expect("record")
        .generation_lifecycle
        .is_empty());
}

#[test]
fn images_without_a_lifecycle_trailer_decode_as_legacy() {
    // Hand-builds a record body in the pre-lifecycle layout (no trailer
    // section at all) and proves the new decoder reads it as an empty map
    // with legacy defaults, instead of rejecting the image.
    let mut body = Vec::new();
    put_u64(&mut body, 1);
    put_u64(&mut body, 1);
    put_u64(&mut body, 7);
    put_u64(&mut body, 1);
    schema(1).encode_into(&mut body).expect("schema");
    put_u64(&mut body, 7);
    put_u32(&mut body, 1);
    put_u64(&mut body, 5);
    put_u32(&mut body, 1);
    reference(1, 7, 1).encode_into(&mut body);
    put_u32(&mut body, PublicationState::Published.as_u32());
    let total = u32::try_from(body.len() + 4).expect("length");
    let mut image = Vec::new();
    put_u32(&mut image, total);
    image.extend_from_slice(&body);
    let mut cursor = Cursor::new(&image);
    let record = ObjectRecord::decode_versioned(&mut cursor, false).expect("legacy image decodes");
    assert!(cursor.is_empty());
    assert!(record.generation_lifecycle.is_empty());
    assert_eq!(
        record.lifecycle_of(GenerationId::new(7)).expect("current"),
        GenerationLifecycle::Active
    );
    assert_eq!(
        record.lifecycle_of(GenerationId::new(5)).expect("retained"),
        GenerationLifecycle::Sealed
    );
}

#[test]
fn lifecycle_trailer_corruption_is_rejected() {
    // Recomputes the catalog checksum after damage so the failure under
    // test comes from trailer validation rather than the checksum gate.
    fn rechecksum(bytes: &mut [u8]) {
        let checksum = checksum_of(
            &bytes[..CHECKSUMMED_PREFIX_LEN],
            &bytes[CATALOG_HEADER_SIZE..],
        );
        bytes[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&checksum.to_le_bytes());
    }
    let mut tracked = record(1, 7, &[5]);
    tracked.seal_generation(GenerationId::new(5)).expect("seal");
    let catalog = state(vec![tracked]);
    // The trailer ends the record body; flipping its version guard must fail.
    let mut bytes = catalog.encode().expect("encode");
    let version_offset = bytes.len() - (1 + 4 + 9);
    bytes[version_offset] ^= 0xFF;
    rechecksum(&mut bytes);
    assert!(CatalogState::decode(&bytes).is_err());
    // An unknown lifecycle state byte must fail as well.
    let mut bytes = catalog.encode().expect("encode");
    let state_offset = bytes.len() - 1;
    bytes[state_offset] = 0x7F;
    rechecksum(&mut bytes);
    assert!(CatalogState::decode(&bytes).is_err());
    assert_ne!(LIFECYCLE_TRAILER_VERSION, 0);
}

#[test]
fn retired_objects_default_retained_generations_to_retire() {
    let source = record(1, 7, &[5]);
    let retired = source.as_retired().expect("retire");
    assert_eq!(
        retired
            .lifecycle_of(GenerationId::new(5))
            .expect("retained"),
        GenerationLifecycle::Retire
    );
    assert_eq!(
        retired.lifecycle_of(GenerationId::new(7)).expect("current"),
        GenerationLifecycle::Active
    );
    // Releasing retention drops the lifecycle entry with the references.
    let mut tracked = record(1, 7, &[5, 6]);
    tracked.seal_generation(GenerationId::new(5)).expect("seal");
    let released = tracked
        .without_retention(GenerationId::new(5))
        .expect("release");
    assert!(!released.generation_lifecycle.contains_key(&5));
    assert!(released.lifecycle_of(GenerationId::new(5)).is_err());
    let empty: BTreeMap<u64, GenerationLifecycle> = BTreeMap::new();
    assert!(empty.is_empty());
}
