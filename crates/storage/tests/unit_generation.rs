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
// Moved from `src/generation.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::{
    BlockId, CatalogVersion, ColumnId, ErrorKind, GenerationId, Lsn, ObjectId, PackId, PageId,
    Result, RowId, SchemaId, SegmentId,
};
use plomid_storage::catalog::{
    CatalogState, ObjectRecord, PhysicalReference, PhysicalStructure, PublicationState,
    SchemaColumn, SchemaMetadata,
};
use plomid_storage::generation::{
    validate_generation_relationship, GenerationManager, GenerationMetadata, ObjectChange,
    PublicationPointer, PublicationRequest, PUBLICATION_POINTER_SIZE,
};

/// Builds a physical structure inside one object's data.
fn structure_for_test(page: u64) -> PhysicalStructure {
    PhysicalStructure::new(
        SegmentId::new(1),
        PackId::new(1),
        BlockId::new(1),
        PageId::new(page),
        Some(RowId::new(page)),
    )
}

/// Builds a physical reference for `object` generation `generation`.
fn reference_for_test(object: u64, generation: u64, page: u64) -> PhysicalReference {
    PhysicalReference::new(
        ObjectId::new(object),
        GenerationId::new(generation),
        structure_for_test(page),
    )
}

/// Builds versioned schema metadata for `version`.
fn schema_for_test(version: u64) -> SchemaMetadata {
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

/// Builds an object change writing generation `generation` of `object`.
fn change_for_test(object: u64, generation: u64) -> ObjectChange {
    ObjectChange::new(
        ObjectId::new(object),
        schema_for_test(1),
        GenerationId::new(generation),
        vec![reference_for_test(object, generation, 1)],
    )
    .expect("change")
}

/// Builds a publication request with a durable boundary.
fn request_for_test(storage: u64, lsn: u64, changes: Vec<ObjectChange>) -> PublicationRequest {
    PublicationRequest::write(GenerationId::new(storage), Lsn::new(lsn), changes).expect("request")
}

#[test]
fn publication_pointer_round_trips_deterministically() {
    let pointer = PublicationPointer::new(
        GenerationId::new(4),
        CatalogVersion::new(9),
        GenerationId::new(2),
        Lsn::new(31),
    )
    .expect("pointer");
    let bytes = pointer.encode().expect("encode");
    assert_eq!(bytes.len(), PUBLICATION_POINTER_SIZE);
    assert_eq!(pointer.encode().expect("encode"), bytes);
    assert_eq!(PublicationPointer::decode(&bytes).expect("decode"), pointer);
    assert_eq!(pointer.publication_state(), PublicationState::Published);
}

#[test]
fn corrupted_publication_pointers_are_rejected() {
    let pointer = PublicationPointer::new(
        GenerationId::new(4),
        CatalogVersion::new(9),
        GenerationId::new(2),
        Lsn::new(31),
    )
    .expect("pointer");
    let valid = pointer.encode().expect("encode");
    let cases: [(usize, u8); 5] = [(0, b'X'), (4, 0xFF), (8, 0x7F), (44, 0x7F), (48, 0xFF)];
    for (offset, value) in cases {
        let mut bytes = valid.clone();
        bytes[offset] = value;
        assert!(
            PublicationPointer::decode(&bytes).is_err(),
            "corrupt pointer byte {offset} must be rejected"
        );
    }
    let mut bytes = valid.clone();
    bytes[52] = 1;
    assert!(PublicationPointer::decode(&bytes).is_err());
    assert!(PublicationPointer::decode(&valid[..10]).is_err());
    let mut bytes = valid.clone();
    bytes.push(0);
    assert!(PublicationPointer::decode(&bytes).is_err());
}

#[test]
fn unpublished_publication_pointers_are_rejected() {
    let error = PublicationPointer {
        catalog_generation: GenerationId::new(4),
        catalog_version: CatalogVersion::new(1),
        storage_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
        state: PublicationState::Staged,
    }
    .validate()
    .expect_err("a staged pointer must be rejected");
    assert_eq!(error.kind(), ErrorKind::Corruption);
}

#[test]
fn generation_metadata_round_trips_and_rejects_corruption() {
    let metadata = GenerationMetadata::new(
        GenerationId::new(5),
        ObjectId::new(1),
        CatalogVersion::new(2),
        GenerationId::new(3),
        GenerationId::new(1),
        Lsn::new(9),
        Some(GenerationId::new(4)),
        PublicationState::Published,
        vec![reference_for_test(1, 5, 1)],
    )
    .expect("generation");
    let bytes = metadata.encode().expect("encode");
    assert_eq!(metadata.encode().expect("encode"), bytes);
    assert_eq!(
        GenerationMetadata::decode(&bytes).expect("decode"),
        metadata
    );
    for offset in [
        0,
        plomid_storage::generation::OFF_VERSION,
        plomid_storage::generation::OFF_HEADER_LEN,
        plomid_storage::generation::OFF_GENERATION_ID,
        plomid_storage::generation::OFF_OBJECT_ID,
        plomid_storage::generation::OFF_CATALOG_VERSION,
        plomid_storage::generation::OFF_PUBLICATION_GENERATION,
        plomid_storage::generation::OFF_STORAGE_GENERATION,
        plomid_storage::generation::OFF_CHECKPOINT_LSN,
        plomid_storage::generation::OFF_PREVIOUS_GENERATION,
        plomid_storage::generation::OFF_REFERENCE_COUNT,
        plomid_storage::generation::OFF_REFERENCES_LEN,
        plomid_storage::generation::OFF_STATE,
        plomid_storage::generation::OFF_CHECKSUM,
        plomid_storage::generation::OFF_RESERVED,
    ] {
        let mut corrupted = bytes.clone();
        corrupted[offset] ^= 0xFF;
        assert!(
            GenerationMetadata::decode(&corrupted).is_err(),
            "corrupt generation byte {offset} must be rejected"
        );
    }
    assert!(GenerationMetadata::decode(
        &bytes[..plomid_storage::generation::GENERATION_HEADER_SIZE - 1]
    )
    .is_err());
    let mut corrupted = bytes.clone();
    corrupted.push(0);
    assert!(GenerationMetadata::decode(&corrupted).is_err());
}

#[test]
fn object_change_validation_rejects_mismatched_references() {
    assert!(change_for_test(1, 2).validate().is_ok());
    let error = ObjectChange::new(
        ObjectId::new(1),
        schema_for_test(1),
        GenerationId::new(2),
        vec![reference_for_test(2, 2, 1)],
    )
    .expect_err("a reference to another object must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = ObjectChange::new(
        ObjectId::new(1),
        schema_for_test(1),
        GenerationId::new(2),
        vec![reference_for_test(1, 3, 1)],
    )
    .expect_err("a reference to another generation must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let error = ObjectChange::new(
        ObjectId::new(0),
        schema_for_test(1),
        GenerationId::new(2),
        Vec::new(),
    )
    .expect_err("a zero object ID must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn publication_requests_require_deterministic_ordering() {
    assert!(request_for_test(1, 1, vec![change_for_test(1, 1)])
        .validate()
        .is_ok());
    assert!(PublicationRequest::write(GenerationId::new(0), Lsn::new(1), Vec::new()).is_err());
    assert!(
        PublicationRequest::write(GenerationId::new(1), Lsn::new(u64::MAX), Vec::new()).is_err()
    );
    let error = PublicationRequest::write(
        GenerationId::new(1),
        Lsn::new(1),
        vec![change_for_test(2, 1), change_for_test(1, 1)],
    )
    .expect_err("unordered object changes must be rejected");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    let request = PublicationRequest::write(
        GenerationId::new(1),
        Lsn::new(1),
        vec![change_for_test(1, 1)],
    )
    .expect("request")
    .with_releases(vec![GenerationId::new(3), GenerationId::new(3)])
    .expect("releases");
    assert_eq!(request.releases, vec![GenerationId::new(3)]);
}

#[test]
fn publication_rejects_repeated_generation_identities() {
    let error = PublicationRequest::write(
        GenerationId::new(1),
        Lsn::new(1),
        vec![
            change_for_test(1, 5),
            ObjectChange::new(
                ObjectId::new(2),
                schema_for_test(1),
                GenerationId::new(5),
                vec![reference_for_test(2, 5, 1)],
            )
            .expect("change"),
        ],
    )
    .expect_err("two objects cannot share one generation identity");
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);
}

#[test]
fn generation_relationships_require_ordering_not_equality() {
    let record = ObjectRecord::new(
        ObjectId::new(1),
        schema_for_test(1),
        GenerationId::new(6),
        vec![GenerationId::new(5)],
        vec![reference_for_test(1, 6, 1), reference_for_test(1, 5, 1)],
        PublicationState::Published,
    )
    .expect("record");
    let state = CatalogState::staged(
        CatalogVersion::new(3),
        GenerationId::new(3),
        GenerationId::new(2),
        Lsn::new(7),
        vec![record.clone()],
    )
    .expect("state")
    .as_published();

    // The object's current generation was written by publication one, which
    // is older than the state that currently publishes it.
    let current = GenerationMetadata::new(
        GenerationId::new(6),
        ObjectId::new(1),
        CatalogVersion::new(2),
        GenerationId::new(2),
        GenerationId::new(2),
        Lsn::new(5),
        Some(GenerationId::new(5)),
        PublicationState::Published,
        vec![reference_for_test(1, 6, 1)],
    )
    .expect("generation");
    assert!(validate_generation_relationship(&current, &record, &state).is_ok());

    // A retained generation is owned by its object and is strictly older.
    let retained = GenerationMetadata::new(
        GenerationId::new(5),
        ObjectId::new(1),
        CatalogVersion::new(1),
        GenerationId::new(1),
        GenerationId::new(1),
        Lsn::new(3),
        None,
        PublicationState::Published,
        vec![reference_for_test(1, 5, 1)],
    )
    .expect("generation");
    assert!(validate_generation_relationship(&retained, &record, &state).is_ok());

    // A generation may not claim a newer durable boundary than the state
    // that references it.
    let future_boundary = GenerationMetadata::new(
        GenerationId::new(6),
        ObjectId::new(1),
        CatalogVersion::new(4),
        GenerationId::new(4),
        GenerationId::new(2),
        Lsn::new(5),
        Some(GenerationId::new(5)),
        PublicationState::Published,
        vec![reference_for_test(1, 6, 1)],
    )
    .expect("generation");
    assert!(validate_generation_relationship(&future_boundary, &record, &state).is_err());

    // A retained generation must come from a strictly older publication.
    let same_publication = GenerationMetadata::new(
        GenerationId::new(5),
        ObjectId::new(1),
        CatalogVersion::new(3),
        GenerationId::new(3),
        GenerationId::new(1),
        Lsn::new(3),
        None,
        PublicationState::Published,
        vec![reference_for_test(1, 5, 1)],
    )
    .expect("generation");
    assert!(validate_generation_relationship(&same_publication, &record, &state).is_err());

    // A predecessor whose retention has been released stays valid provenance.
    let released_record = ObjectRecord::new(
        ObjectId::new(1),
        schema_for_test(1),
        GenerationId::new(6),
        Vec::new(),
        vec![reference_for_test(1, 6, 1)],
        PublicationState::Published,
    )
    .expect("record");
    let released_state = CatalogState::staged(
        CatalogVersion::new(3),
        GenerationId::new(3),
        GenerationId::new(2),
        Lsn::new(7),
        vec![released_record.clone()],
    )
    .expect("state")
    .as_published();
    assert!(validate_generation_relationship(&current, &released_record, &released_state).is_ok());

    // A predecessor that is not older than the generation it names is not.
    let newer_predecessor = GenerationMetadata::new(
        GenerationId::new(6),
        ObjectId::new(1),
        CatalogVersion::new(2),
        GenerationId::new(2),
        GenerationId::new(2),
        Lsn::new(5),
        Some(GenerationId::new(7)),
        PublicationState::Published,
        vec![reference_for_test(1, 6, 1)],
    )
    .expect("generation");
    assert!(validate_generation_relationship(&newer_predecessor, &record, &state).is_err());
}

/// A recovery walk that starts from a superseded pointer snapshot must
/// heal by re-reading the current pointer instead of dying on files a
/// concurrent collector reclaimed.
///
/// Deterministic reproduction of the bulk-load kill: a fresh generation
/// manager per connection (executor setup opens one per session) walks
/// pointer → catalog → every referenced generation with no reader lease,
/// while a concurrent maintenance pass publishes and reclaims. If the
/// collector deletes a referenced generation directory mid-walk, resolving
/// the stale snapshot fails `NotFound`; recovery must then observe the
/// new pointer rather than propagate the stale failure.
#[test]
fn stale_pointer_snapshot_heals_through_recovery_retry() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "plomid-generation-recover-retry-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    let result: Result<()> = (|| {
        use plomid_storage::generation::{
            discover_generations, load_publication_pointer, resolve_publication, GenerationManager,
            PublicationPointerSource,
        };
        use std::sync::Arc;
        let manager = Arc::new(GenerationManager::open(&dir)?);
        // Publish generation 5, then snapshot its pointer.
        manager.publish(request_for_test(1, 5, vec![change_for_test(1, 5)]))?;
        let stale = load_publication_pointer(&dir)?;
        // Supersede twice, releasing generation 5's retention on the
        // second publish so it is collectable (but do NOT run GC yet).
        manager.publish(request_for_test(2, 6, vec![change_for_test(1, 6)]))?;
        let mut release = request_for_test(3, 7, vec![change_for_test(1, 7)]);
        release.releases.push(GenerationId::new(5));
        manager.publish(release)?;

        // Variant B (production kill): the generation directory is gone
        // but its catalog survives — exactly the mid-GC observation (GC
        // reclaims generation directories before catalog files, so this
        // intermediate state always exists transiently).
        let gen5_dir = discover_generations(&dir)?
            .into_iter()
            .find(|(id, _)| *id == GenerationId::new(5))
            .map(|(_, path)| path.parent().expect("generation dir").to_path_buf())
            .expect("generation 5 on disk");
        std::fs::remove_dir_all(&gen5_dir).expect("remove gen dir");
        let stale_err = resolve_publication(&dir, stale, PublicationPointerSource::Pointer)
            .expect_err("stale snapshot must fail after reclamation");
        assert_eq!(
            stale_err.kind(),
            ErrorKind::NotFound,
            "variant B must be the production NotFound, got {stale_err:?}"
        );
        let recovered =
            GenerationManager::recover_if_present(&dir)?.expect("live state must recover");
        assert_eq!(recovered.source, PublicationPointerSource::Pointer);
        assert_eq!(recovered.pointer.catalog_generation, GenerationId::new(3));

        // Variant A (sibling race): the catalog is reclaimed too.
        // Generation 5's directory is already gone from variant B above;
        // running GC now also reclaims the stale catalog, completing the
        // fully-collected state.
        let outcome = manager.gc()?;
        assert!(
            outcome.reclaimed_catalogs.contains(&GenerationId::new(1)),
            "precondition: GC must reclaim the stale catalog, reclaimed_catalogs={:?}",
            outcome.reclaimed_catalogs
        );
        resolve_publication(&dir, stale, PublicationPointerSource::Pointer)
            .expect_err("fully superseded snapshot must fail");
        let recovered = GenerationManager::recover_if_present(&dir)?
            .expect("live state must recover after full reclaim");
        assert_eq!(recovered.source, PublicationPointerSource::Pointer);
        assert_eq!(recovered.pointer.catalog_generation, GenerationId::new(3));
        // Idempotent: a second recovery observes the same state.
        let again = GenerationManager::recover_if_present(&dir)?.expect("recovery must repeat");
        assert_eq!(again.pointer, recovered.pointer);
        Ok(())
    })();
    std::fs::remove_dir_all(&dir).ok();
    result.expect("recovery retry test");
}

/// A recovery walk that starts several publications behind the live pointer
/// must still heal by re-reading the current pointer instead of dying on
/// files a collector reclaimed across many supersessions.
///
/// Deterministic, single-threaded form of the concurrent publish/reclaim/
/// recover soak (`concurrent_publish_reclaim_and_recover_never_tears`,
/// Tier D): snapshot the pointer, advance + release + collect past it three
/// times so none of the snapshot's files (its catalog or its generations)
/// can survive, then prove the stale snapshot fails while
/// `recover_if_present` heals to the newest publication. No threads, no
/// sleeps, no timing dependence: this is the regression that must stay in
/// the default gate.
#[test]
fn deeply_superseded_snapshot_heals_through_recovery_retry() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "plomid-generation-deep-supersede-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    let result: Result<()> = (|| {
        use plomid_storage::generation::{
            load_publication_pointer, resolve_publication, GenerationManager,
            PublicationPointerSource,
        };
        use std::sync::Arc;
        let manager = Arc::new(GenerationManager::open(&dir)?);
        // Publish generations 1..=5 with no releases: deep retained history.
        for gen in 1..=5u64 {
            manager.publish(request_for_test(gen, gen, vec![change_for_test(1, gen)]))?;
        }
        let stale = load_publication_pointer(&dir)?;
        assert_eq!(stale.catalog_generation, GenerationId::new(5));
        // Advance three publications, releasing everything at or below the
        // snapshot on the last one, and collect after each. Only the newest
        // catalog is retained, so none of the snapshot's files survive.
        for gen in 6..=8u64 {
            let mut request = request_for_test(gen, gen, vec![change_for_test(1, gen)]);
            if gen == 8 {
                for old in 1..=5u64 {
                    request.releases.push(GenerationId::new(old));
                }
            }
            manager.publish(request)?;
            manager.gc()?;
        }
        assert_eq!(
            load_publication_pointer(&dir)?.catalog_generation,
            GenerationId::new(8)
        );
        // The deeply superseded snapshot must fail (its catalog was
        // reclaimed), while recovery heals to the newest publication.
        let stale_err = resolve_publication(&dir, stale, PublicationPointerSource::Pointer)
            .expect_err("deeply superseded snapshot must fail");
        assert_eq!(
            stale_err.kind(),
            ErrorKind::Corruption,
            "fully-collected snapshot must be the missing-catalog corruption, got {stale_err:?}"
        );
        let recovered =
            GenerationManager::recover_if_present(&dir)?.expect("live state must recover");
        assert_eq!(recovered.source, PublicationPointerSource::Pointer);
        assert_eq!(recovered.pointer.catalog_generation, GenerationId::new(8));
        // Idempotent: a second recovery observes the same state.
        let again = GenerationManager::recover_if_present(&dir)?.expect("recovery must repeat");
        assert_eq!(again.pointer, recovered.pointer);
        Ok(())
    })();
    std::fs::remove_dir_all(&dir).ok();
    result.expect("deep supersession test");
}

/// Concurrent publish+GC while recovering must never surface a torn
/// snapshot: every recovery observes some complete publication.
///
/// This is the live form of the bulk-load kill (per-connection executor
/// setup recovers on a shared root while maintenance publishes and
/// reclaims). Publisher and recoverer run unlocked against each other;
/// the retry converts torn reads into fresh-pointer rereads. The
/// publisher is paced (~20 ms periods) against multi-millisecond walks
/// over a deep retained history, so each recovery has a mid-single-digit
/// tear probability: over hundreds of recoveries a missing retry gets
/// caught with near-certainty, while the retry heals every hit
/// deterministically (nine consecutive tears would be required to fail).
///
/// Soak variant (Tier D): this live race is timing-sensitive under parallel
/// CI contention — when recovery walks outlast publish periods, every retry
/// attempt can tear and the fixed 8-attempt budget exhausts (observed:
/// 50/300 misses on a loaded runner vs 300/300 solo). The tear→heal
/// property itself is covered deterministically in the default gate by
/// `stale_pointer_snapshot_heals_through_recovery_retry` (single
/// supersession) and `deeply_superseded_snapshot_heals_through_recovery_retry`
/// (multi-supersession, no timing dependence). Runs via `make test-scale`
/// (nightly), never in the routine gate.
#[test]
#[ignore = "soak: 300-iteration live publish/reclaim/recover race is timing-sensitive under parallel CI contention; deterministic tear/heal regressions cover the property in the default gate; runs via make test-scale"]
fn concurrent_publish_reclaim_and_recover_never_tears() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    static NEXT: AtomicU64 = AtomicU64::new(10_000);
    let dir = std::env::temp_dir().join(format!(
        "plomid-generation-race-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    let manager = Arc::new(GenerationManager::open(&dir).expect("manager"));
    // Deep retained history: every recovery walk reads dozens of
    // generation files, giving a racing collector a wide window.
    for gen in 1..=60u64 {
        manager
            .publish(request_for_test(gen, gen, vec![change_for_test(1, gen)]))
            .expect("setup publish");
    }
    let stop = Arc::new(AtomicBool::new(false));
    let failures = Arc::new(AtomicU64::new(0));
    let recovered = Arc::new(AtomicU64::new(0));
    // Publisher: advance, release one old generation, and collect, paced
    // so each recovery walk overlaps live reclamation with high
    // probability without saturating the machine.
    let publisher = {
        let manager = Arc::clone(&manager);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut gen = 61u64;
            while !stop.load(Ordering::Relaxed) {
                let mut request = request_for_test(gen, gen, vec![change_for_test(1, gen)]);
                request.releases.push(GenerationId::new(gen - 2));
                if manager.publish(request).is_ok() {
                    let _ = manager.gc();
                }
                gen += 1;
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        })
    };
    // Recoverer: what per-connection executor setup does, in a loop.
    let worker_dir = dir.clone();
    let worker = {
        let stop = Arc::clone(&stop);
        let failures = Arc::clone(&failures);
        let recovered = Arc::clone(&recovered);
        std::thread::spawn(move || {
            for _ in 0..300 {
                match GenerationManager::recover_if_present(&worker_dir) {
                    Ok(Some(_)) => {
                        recovered.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) | Err(_) => {
                        failures.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        })
    };
    worker.join().expect("recoverer");
    stop.store(true, Ordering::Relaxed);
    publisher.join().expect("publisher");
    // Every recovery observed a complete publication; a torn snapshot
    // would have failed instead of healing through retry.
    assert_eq!(recovered.load(Ordering::Relaxed), 300);
    assert_eq!(failures.load(Ordering::Relaxed), 0);
    std::fs::remove_dir_all(&dir).ok();
}
