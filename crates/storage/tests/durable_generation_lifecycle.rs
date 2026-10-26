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
//! Durable per-generation lifecycle tests.
//!
//! These tests exercise the [`GenerationLifecycle`] state machine end to end
//! against real filesystem state: publication seals the superseded generation,
//! explicit transitions walk a retained generation to `Reclaim`, illegal jumps
//! are rejected without publishing anything, a live reader lease blocks
//! `Reclaim`, and legacy catalog images without a lifecycle trailer keep
//! decoding with legacy defaults.
use plomid_core::{
    BlockId, CatalogVersion, ColumnId, DatabaseId, ErrorKind, GenerationId, Lsn, ObjectId, PackId,
    PageId, RowId, SchemaId, SegmentId,
};
use plomid_storage::{
    generation_path_in_schema, GenerationLifecycle, GenerationManager, ObjectChange,
    PhysicalReference, PhysicalStructure, PublicationRequest, SchemaColumn, SchemaMetadata,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Scratch-seed identities, matching the hierarchy convention of the sibling
/// lifecycle suite: `ObjectChange::new` derives database 1 / schema 7.
const SEED_DB: DatabaseId = DatabaseId::new(1);
const PUB_SCHEMA: SchemaId = SchemaId::new(7);

fn scratch(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "plomid-durable-lifecycle-{label}-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("scratch");
    path
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

fn open_manager(root: &Path) -> Arc<GenerationManager> {
    Arc::new(GenerationManager::open(root).expect("manager"))
}

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

fn change(object: u64, generation: u64) -> ObjectChange {
    ObjectChange::new(
        ObjectId::new(object),
        schema(1),
        GenerationId::new(generation),
        vec![reference(object, generation, 1)],
    )
    .expect("change")
}

fn write(storage: u64, lsn: u64, changes: Vec<ObjectChange>) -> PublicationRequest {
    PublicationRequest::write(GenerationId::new(storage), Lsn::new(lsn), changes).expect("request")
}

fn lifecycle_of(
    manager: &Arc<GenerationManager>,
    object: u64,
    generation: u64,
) -> GenerationLifecycle {
    manager
        .load()
        .expect("catalog")
        .object(ObjectId::new(object))
        .expect("record")
        .lifecycle_of(GenerationId::new(generation))
        .expect("lifecycle")
}

#[test]
fn publication_seals_the_superseded_generation() {
    let dir = scratch("seal-on-publish");
    let result = (|| {
        let manager = open_manager(&dir);
        let first = manager.publish(write(1, 1, vec![change(1, 5)]))?;
        let first_record = first.catalog.object(ObjectId::new(1)).expect("record");
        // A fresh generation is live with no explicit entry (legacy default).
        assert!(first_record.generation_lifecycle.is_empty());
        assert_eq!(
            first_record
                .lifecycle_of(GenerationId::new(5))
                .expect("lifecycle"),
            GenerationLifecycle::Active
        );

        // Superseding it seals the previous generation durably in the new state.
        let second = manager.publish(write(1, 2, vec![change(1, 6)]))?;
        let record = second.catalog.object(ObjectId::new(1)).expect("record");
        assert_eq!(
            record.lifecycle_of(GenerationId::new(5)).expect("sealed"),
            GenerationLifecycle::Sealed
        );
        assert_eq!(
            record.lifecycle_of(GenerationId::new(6)).expect("active"),
            GenerationLifecycle::Active
        );
        // The superseded catalog file on disk still carries the legacy image,
        // which keeps decoding with legacy defaults.
        let superseded = plomid_storage::load_catalog_for_generation(&dir, GenerationId::new(1))?
            .expect("superseded catalog state");
        let old = superseded.object(ObjectId::new(1)).expect("record");
        assert!(old.generation_lifecycle.is_empty());
        assert_eq!(
            old.lifecycle_of(GenerationId::new(5)).expect("legacy"),
            GenerationLifecycle::Active
        );
        // A reopened manager observes the sealed state.
        let reopened = open_manager(&dir);
        assert_eq!(lifecycle_of(&reopened, 1, 5), GenerationLifecycle::Sealed);
        assert_eq!(lifecycle_of(&reopened, 1, 6), GenerationLifecycle::Active);
        reopened.validate()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn transition_walks_a_retained_generation_to_reclaim() {
    let dir = scratch("walk");
    let result = (|| {
        let manager = open_manager(&dir);
        manager.publish(write(1, 1, vec![change(1, 5)]))?;
        manager.publish(write(1, 2, vec![change(1, 6)]))?;
        let object = ObjectId::new(1);
        let generation = GenerationId::new(5);
        for state in [
            GenerationLifecycle::Columnized,
            GenerationLifecycle::Compactable,
            GenerationLifecycle::Compact,
            GenerationLifecycle::Retire,
        ] {
            let published = manager.transition_lifecycle(object, generation, state)?;
            assert_eq!(
                published
                    .catalog
                    .object(object)
                    .expect("record")
                    .lifecycle_of(generation)
                    .expect("state"),
                state
            );
        }
        // Every transition is its own durable publication: reopening observes
        // the walked state and the manager still validates.
        let reopened = open_manager(&dir);
        assert_eq!(lifecycle_of(&reopened, 1, 5), GenerationLifecycle::Retire);
        reopened.validate()?;
        let outcome =
            manager.transition_lifecycle(object, generation, GenerationLifecycle::Reclaim)?;
        assert_eq!(
            outcome
                .catalog
                .object(object)
                .expect("record")
                .lifecycle_of(generation)
                .expect("terminal"),
            GenerationLifecycle::Reclaim
        );
        assert!(outcome.generations.is_empty());
        assert!(outcome.superseded_catalog.is_some());
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn illegal_jumps_are_rejected_without_publishing() {
    let dir = scratch("illegal");
    let result = (|| {
        let manager = open_manager(&dir);
        manager.publish(write(1, 1, vec![change(1, 5)]))?;
        manager.publish(write(1, 2, vec![change(1, 6)]))?;
        let before = manager.pointer()?;
        let object = ObjectId::new(1);
        // Active straight to Compact skips the chain and must fail.
        let error = manager
            .transition_lifecycle(object, GenerationId::new(6), GenerationLifecycle::Compact)
            .expect_err("Active → Compact must be rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        // Sealed straight to Compact skips Columnized/Compactable.
        let error = manager
            .transition_lifecycle(object, GenerationId::new(5), GenerationLifecycle::Compact)
            .expect_err("Sealed → Compact must be rejected");
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
        // Unknown generations and objects are rejected, not defaulted.
        let error = manager
            .transition_lifecycle(object, GenerationId::new(99), GenerationLifecycle::Sealed)
            .expect_err("unknown generation must be rejected");
        assert_eq!(error.kind(), ErrorKind::NotFound);
        let error = manager
            .transition_lifecycle(
                ObjectId::new(99),
                GenerationId::new(5),
                GenerationLifecycle::Sealed,
            )
            .expect_err("unknown object must be rejected");
        assert_eq!(error.kind(), ErrorKind::NotFound);
        // Nothing was published: the pointer and catalog are untouched.
        assert_eq!(manager.pointer()?, before);
        assert_eq!(lifecycle_of(&manager, 1, 5), GenerationLifecycle::Sealed);
        assert_eq!(lifecycle_of(&manager, 1, 6), GenerationLifecycle::Active);
        manager.validate()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn a_live_reader_lease_blocks_reclaim() {
    let dir = scratch("lease-blocks-reclaim");
    let result = (|| {
        let manager = open_manager(&dir);
        manager.publish(write(1, 1, vec![change(1, 5)]))?;
        manager.publish(write(1, 2, vec![change(1, 6)]))?;
        let object = ObjectId::new(1);
        let retained = GenerationId::new(5);
        // A reader that still observes the retained generation pins it.
        let reader = manager.reader()?;
        assert!(reader.generation(retained).is_some());
        // Intermediate steps stay legal while the lease is held.
        for state in [
            GenerationLifecycle::Columnized,
            GenerationLifecycle::Compactable,
            GenerationLifecycle::Compact,
            GenerationLifecycle::Retire,
        ] {
            manager.transition_lifecycle(object, retained, state)?;
        }
        // Marking reclaimable out from under the live reader is a conflict.
        let error = manager
            .transition_lifecycle(object, retained, GenerationLifecycle::Reclaim)
            .expect_err("a held lease must block Reclaim");
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(lifecycle_of(&manager, 1, 5), GenerationLifecycle::Retire);
        // Once the reader goes away the same transition succeeds, and the
        // normal release + GC path still reclaims the file afterwards.
        drop(reader);
        manager.transition_lifecycle(object, retained, GenerationLifecycle::Reclaim)?;
        assert_eq!(lifecycle_of(&manager, 1, 5), GenerationLifecycle::Reclaim);
        manager.release(retained)?;
        let collected = manager.gc()?;
        assert!(collected.reclaimed.contains(&retained), "{collected:?}");
        assert!(!generation_path_in_schema(
            &dir,
            SEED_DB,
            PUB_SCHEMA,
            plomid_core::TableId::new(1),
            retained,
        )
        .exists());
        manager.validate()?;
        Ok::<(), plomid_core::PlomidError>(())
    })();
    cleanup(&dir);
    assert!(result.is_ok(), "{result:?}");
}
