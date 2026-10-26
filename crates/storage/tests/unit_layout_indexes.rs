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
// Moved from `src/layout/indexes.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{DatabaseId, GenerationId, IndexId, SchemaId, TableId};
use plomid_storage::layout::indexes::{
    index_dir_name, index_from_dir_name, index_generation_file_name,
    index_generation_from_file_name, index_payload_file_name,
    index_payload_generation_from_file_name, IndexGenerationMetadata, IndexGenerationState,
    IndexGenerationTrigger, IndexMetadata, INDEX_GENERATION_META_HEADER_LEN, INDEX_META_HEADER_LEN,
};

#[test]
fn directory_and_generation_names_round_trip() {
    assert_eq!(index_dir_name(IndexId::new(5)), "I-00000000000000000005");
    assert_eq!(
        index_from_dir_name("I-00000000000000000005"),
        Some(IndexId::new(5))
    );
    assert_eq!(index_from_dir_name("T-00000000000000000005"), None);
    assert_eq!(
        index_generation_file_name(GenerationId::new(9)),
        "GEN-00000000000000000009.dat"
    );
    assert_eq!(
        index_generation_from_file_name("GEN-00000000000000000009.dat"),
        Some(GenerationId::new(9))
    );
    assert_eq!(index_generation_from_file_name("META.dat"), None);
    assert_eq!(index_generation_from_file_name("GEN-9.dat"), None);
}

#[test]
fn metadata_round_trips_and_detects_corruption() {
    let record = IndexMetadata::new(IndexId::new(2), TableId::new(1)).expect("record");
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), INDEX_META_HEADER_LEN);
    assert_eq!(IndexMetadata::decode(&bytes).expect("decode"), record);
    let mut bad = bytes.clone();
    bad[20] ^= 0x01;
    assert!(IndexMetadata::decode(&bad).is_err());
    assert!(IndexMetadata::decode(&bytes[..12]).is_err());
}

#[test]
fn index_generation_metadata_round_trips_and_detects_corruption() {
    let record = IndexGenerationMetadata::new(
        IndexId::new(2),
        GenerationId::new(9),
        DatabaseId::new(1),
        SchemaId::new(3),
        TableId::new(1),
        GenerationId::new(7),
        IndexGenerationTrigger::Manual,
        IndexGenerationState::Current,
    )
    .expect("record");
    let bytes = record.encode().expect("encode");
    assert_eq!(bytes.len(), INDEX_GENERATION_META_HEADER_LEN);
    assert_eq!(
        IndexGenerationMetadata::decode(&bytes).expect("decode"),
        record
    );

    // Every checksummed region rejects a single flipped byte.
    for offset in [12usize, 28, 44, 61] {
        let mut bad = bytes.clone();
        bad[offset] ^= 0x01;
        assert!(
            IndexGenerationMetadata::decode(&bad).is_err(),
            "corruption at offset {offset} must be rejected"
        );
    }

    // Truncation and a bad magic are rejected too.
    assert!(IndexGenerationMetadata::decode(&bytes[..20]).is_err());
    let mut bad_magic = bytes.clone();
    bad_magic[0] ^= 0x01;
    assert!(IndexGenerationMetadata::decode(&bad_magic).is_err());

    // The lifecycle transition preserves identity and provenance.
    let retained = record
        .with_state(IndexGenerationState::Retained)
        .expect("transition");
    assert_eq!(retained.generation_id, record.generation_id);
    assert_eq!(
        retained.source_data_generation_id,
        record.source_data_generation_id
    );
    assert_eq!(retained.trigger, record.trigger);
    assert_eq!(retained.state, IndexGenerationState::Retained);
    assert!(!retained.state.is_current());
    assert!(record.is_compatible(GenerationId::new(7)));
    assert!(!record.is_compatible(GenerationId::new(8)));
}

#[test]
fn index_generation_metadata_rejects_impossible_identity() {
    let zero_table = IndexGenerationMetadata::new(
        IndexId::new(1),
        GenerationId::new(1),
        DatabaseId::new(1),
        SchemaId::new(1),
        TableId::new(0),
        GenerationId::new(1),
        IndexGenerationTrigger::Automatic,
        IndexGenerationState::Current,
    );
    assert!(zero_table.is_err());
}

#[test]
fn index_payload_names_round_trip_through_identity() {
    assert_eq!(
        index_payload_file_name(GenerationId::new(4)),
        "IDX-00000000000000000004.dat"
    );
    assert_eq!(
        index_payload_generation_from_file_name("IDX-00000000000000000004.dat"),
        Some(GenerationId::new(4))
    );
    // A generation record is not a payload, and neither is a stray name.
    assert_eq!(
        index_payload_generation_from_file_name("GEN-00000000000000000004.dat"),
        None
    );
    assert_eq!(index_payload_generation_from_file_name("IDX-4.dat"), None);
    assert_eq!(index_payload_generation_from_file_name("META.dat"), None);
}
