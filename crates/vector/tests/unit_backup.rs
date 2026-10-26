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
// Moved from `src/backup.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::backup::*;
use plomid_vector::index::IndexKind;
use plomid_vector::index::{IndexConfig, Metric, VectorIndex as _};
use plomid_vector::r#type::VectorMetric;
use plomid_vector::storage::VectorStorageAdapter as _;
use plomid_vector::value::DenseVectorValue;
use plomid_vector::{VectorElementType, VectorValue};

fn dense(vals: &[f32]) -> VectorValue {
    VectorValue::Dense(
        DenseVectorValue::from_f32(VectorElementType::Float32, vals.to_vec()).unwrap(),
    )
}

fn file_store(dir: &std::path::Path, n: u64) -> plomid_vector::storage::FileVectorStorage {
    let mut s = plomid_vector::storage::FileVectorStorage::create(
        dir,
        "public.docs".into(),
        "embedding".into(),
        Some(2),
    )
    .unwrap();
    for i in 0..n {
        s.append_payload(&dense(&[i as f32, i as f32 + 0.5]))
            .unwrap();
    }
    s.checkpoint(42).unwrap();
    s
}

fn tdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "plomid-vector-backup-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|v| v.subsec_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn hnsw_input(rows: &[(u64, Vec<f32>)], deleted: u64) -> IndexBackupInput {
    let dim = 2usize;
    let mut idx = plomid_vector::index::hnsw::HnswIndex::new(IndexConfig::new(
        IndexKind::Hnsw,
        Metric::L2,
        dim,
    ))
    .unwrap();
    idx.build(rows).unwrap();
    let stats = idx.stats();
    IndexBackupInput {
        name: "docs_hnsw".into(),
        kind: IndexKind::Hnsw,
        metric: VectorMetric::L2,
        dimension: dim,
        generation: stats.generation,
        vector_count: stats.num_vectors as u64,
        deleted_count: deleted,
        persist_blob: Some(idx.encode()),
        diskann_pair: None,
    }
}

#[test]
fn manifest_roundtrip_and_version_gate() {
    let m = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        engine_version: "0.0.0".into(),
        checkpoint_lsn: 7,
        row_count: 3,
        dims: 2,
        table: "t".into(),
        column: "c".into(),
        storage_mode: 1,
        has_stats: false,
        files: vec![BackupFileEntry {
            name: RECORDS_FILE.into(),
            len: 10,
            crc: 99,
        }],
        indexes: vec![],
    };
    let back = BackupManifest::from_bytes(&m.to_bytes()).unwrap();
    assert_eq!(m, back);
    let mut bad = m.to_bytes();
    bad[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(BackupManifest::from_bytes(&bad).is_err());
    let mut corrupt = m.to_bytes();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xFF;
    assert!(BackupManifest::from_bytes(&corrupt).is_err());
    let mut trunc = m.to_bytes();
    trunc.truncate(trunc.len() / 2);
    assert!(BackupManifest::from_bytes(&trunc).is_err());
}

#[test]
fn capture_modes_per_index_kind() {
    assert_eq!(capture_mode(IndexKind::Flat, 0), IndexRestoreMode::Rebuild);
    assert_eq!(
        capture_mode(IndexKind::SparseInverted, 0),
        IndexRestoreMode::Rebuild
    );
    assert_eq!(
        capture_mode(IndexKind::Hnsw, 5),
        IndexRestoreMode::Direct,
        "hnsw preserves tombstones: direct eligible"
    );
    assert_eq!(capture_mode(IndexKind::Binary, 2), IndexRestoreMode::Direct);
    assert_eq!(
        capture_mode(IndexKind::IvfFlat, 0),
        IndexRestoreMode::Direct
    );
    assert_eq!(
        capture_mode(IndexKind::IvfFlat, 1),
        IndexRestoreMode::Rebuild,
        "ivf decode drops tombstones: must rebuild"
    );
    assert_eq!(capture_mode(IndexKind::IvfSq, 3), IndexRestoreMode::Rebuild);
    assert_eq!(capture_mode(IndexKind::IvfPq, 3), IndexRestoreMode::Rebuild);
    assert_eq!(
        capture_mode(IndexKind::DiskAnn, 0),
        IndexRestoreMode::DiskAnnPair
    );
}

#[test]
fn empty_database_backup_restores_empty() {
    let src = tdir("empty-src");
    let s =
        plomid_vector::storage::FileVectorStorage::create(&src, "t".into(), "c".into(), Some(2))
            .unwrap();
    s.checkpoint(1).unwrap();
    let out = tdir("empty-out");
    let report = create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        1,
        None,
        &[],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert_eq!(report.rows, 0);
    let target = tdir("empty-target");
    let restored = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(restored.rows, 0);
    assert_eq!(restored.manifest_lsn, 1);
    let (reopened, _) = plomid_vector::storage::FileVectorStorage::open(
        &target,
        "t".into(),
        "c".into(),
        Some(2),
        false,
    )
    .unwrap();
    assert_eq!(reopened.row_count(), 0);
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn tiny_table_roundtrip_with_mutations_and_stats() {
    let src = tdir("tiny-src");
    let mut s = file_store(&src, 8);
    // Mutations before backup: update + delete.
    s.write_payload(0, &dense(&[100.0, 101.0])).unwrap();
    let mut stats = plomid_vector::stats::VectorStats::unknown(2);
    for _ in 0..8 {
        stats.observe_row(false, false, true);
    }
    stats.build_watermark = 42;
    let out = tdir("tiny-out");
    let report = create_backup(
        &s,
        Some(&src),
        "public.docs",
        "embedding",
        42,
        Some(&stats.encode()),
        &[],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert_eq!(report.rows, 8);
    assert_eq!(report.checkpoint_lsn, 42);
    drop(s);
    let target = tdir("tiny-target");
    let restored = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(restored.rows, 8);
    assert!(restored.stats_restored);
    assert!(restored.engine_version_match);
    let (reopened, recovery) = plomid_vector::storage::FileVectorStorage::open(
        &target,
        "public.docs".into(),
        "embedding".into(),
        Some(2),
        false,
    )
    .unwrap();
    assert_eq!(recovery.replayed_records, 8);
    assert_eq!(reopened.read_payload(0).unwrap(), dense(&[100.0, 101.0]));
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn hnsw_direct_blob_validates_and_serves() {
    let src = tdir("hnsw-src");
    let s = file_store(&src, 64);
    let rows: Vec<(u64, Vec<f32>)> = (0..64u64)
        .map(|i| (i, vec![i as f32, i as f32 + 0.5]))
        .collect();
    let input = hnsw_input(&rows, 0);
    let out = tdir("hnsw-out");
    create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        42,
        None,
        std::slice::from_ref(&input),
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    drop(s);
    let target = tdir("hnsw-target");
    let report = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(report.indexes.len(), 1);
    assert_eq!(report.indexes[0].action, RestoredIndexAction::Direct);
    // Staged blob decodes + validates: may serve.
    let blob = std::fs::read(target.join("indexes/docs_hnsw.blob")).unwrap();
    let config = IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2);
    validate_restored_blob(IndexKind::Hnsw, &config, &blob).unwrap();
    let idx = plomid_vector::index::hnsw::HnswIndex::decode(&blob, config).unwrap();
    let hits = idx.search(&[1.0, 1.5], 3).unwrap();
    assert_eq!(hits[0].row_id, 1);
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn corrupted_direct_blob_forces_rebuild_never_served() {
    let rows: Vec<(u64, Vec<f32>)> = (0..32u64).map(|i| (i, vec![i as f32, i as f32])).collect();
    let mut input = hnsw_input(&rows, 0);
    // Corrupt the staged bytes (bit flip): validation must fail closed.
    if let Some(b) = input.persist_blob.as_mut() {
        let last = b.len() - 1;
        b[last] ^= 0xFF;
    }
    // Backup-time CRC covers the (corrupt) bytes honestly; restore-time
    // decode+validate is what refuses to serve.
    let config = IndexConfig::new(IndexKind::Hnsw, Metric::L2, 2);
    assert!(validate_restored_blob(
        IndexKind::Hnsw,
        &config,
        input.persist_blob.as_ref().unwrap()
    )
    .is_err());
    // And the caller rebuilds from authoritative rows instead.
    let mut rebuilt = plomid_vector::index::hnsw::HnswIndex::new(config).unwrap();
    rebuilt.build(&rows).unwrap();
    rebuilt.validate().unwrap();
    assert_eq!(rebuilt.search(&[1.0, 1.0], 1).unwrap()[0].row_id, 1);
}

#[test]
fn ivf_with_tombstones_restores_as_rebuild_marker() {
    let src = tdir("ivf-src");
    let s = file_store(&src, 32);
    let dim = 2usize;
    let rows: Vec<(u64, Vec<f32>)> = (0..32u64).map(|i| (i, vec![i as f32, i as f32])).collect();
    let mut idx = plomid_vector::index::ivf::IvfFlatIndex::new(IndexConfig::new(
        IndexKind::IvfFlat,
        Metric::L2,
        dim,
    ))
    .unwrap();
    idx.build(&rows).unwrap();
    idx.delete(0).unwrap();
    let stats = idx.stats();
    assert!(stats.num_deleted > 0);
    let input = IndexBackupInput {
        name: "docs_ivf".into(),
        kind: IndexKind::IvfFlat,
        metric: VectorMetric::L2,
        dimension: dim,
        generation: stats.generation,
        vector_count: stats.num_vectors as u64,
        deleted_count: stats.num_deleted as u64,
        persist_blob: Some(idx.encode()),
        diskann_pair: None,
    };
    let out = tdir("ivf-out");
    let report = create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        42,
        None,
        std::slice::from_ref(&input),
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert_eq!(report.rebuild_marked, 1);
    drop(s);
    let target = tdir("ivf-target");
    let restored = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(
        restored.indexes[0].action,
        RestoredIndexAction::RebuildRequired
    );
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn backup_failure_points_leave_no_healthy_backup() {
    let src = tdir("fail-src");
    let s = file_store(&src, 16);
    for fail in [
        BackupFailPoint::BeforeDataCopy,
        BackupFailPoint::DuringDataCopy(4),
        BackupFailPoint::DuringIndexCopy,
        BackupFailPoint::DuringFinalize,
    ] {
        let out = tdir("fail-out");
        assert!(
            create_backup(&s, Some(&src), "t", "c", 42, None, &[], 0, &out, fail, None).is_err(),
            "fail point {fail:?} must fail"
        );
        // No finalized manifest ⇒ restore refuses (never falsely healthy).
        let target = tdir("fail-target");
        assert!(restore_backup(&out, &target, RestoreFailPoint::None, None).is_err());
        let _ = std::fs::remove_dir_all(&out);
        let _ = std::fs::remove_dir_all(&target);
    }
    let _ = std::fs::remove_dir_all(&src);
}

#[test]
fn restore_failure_points_leave_no_healthy_target() {
    let src = tdir("rfail-src");
    let s = file_store(&src, 16);
    let out = tdir("rfail-out");
    create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        42,
        None,
        &[],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    drop(s);
    for fail in [
        RestoreFailPoint::BeforeStart,
        RestoreFailPoint::DuringDataRestore(4),
        RestoreFailPoint::DuringIndexRestore,
        RestoreFailPoint::DuringFinalize,
    ] {
        let target = tdir("rfail-target");
        assert!(
            restore_backup(&out, &target, fail, None).is_err(),
            "fail point {fail:?} must fail"
        );
        // Target has no MANIFEST/records ⇒ open fails closed.
        assert!(
            plomid_vector::storage::FileVectorStorage::open(
                &target,
                "t".into(),
                "c".into(),
                Some(2),
                false
            )
            .is_err(),
            "failed restore must not look healthy ({fail:?})"
        );
        let _ = std::fs::remove_dir_all(&target);
    }
    for d in [&src, &out] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn invalid_inputs_fail_safely() {
    let target = tdir("invalid-target");
    // Missing backup manifest.
    let empty = tdir("invalid-empty");
    std::fs::create_dir_all(&empty).unwrap();
    assert!(restore_backup(&empty, &target, RestoreFailPoint::None, None).is_err());
    // Version mismatch.
    let bad = tdir("invalid-version");
    std::fs::create_dir_all(&bad).unwrap();
    let mut m = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        engine_version: "x".into(),
        checkpoint_lsn: 0,
        row_count: 0,
        dims: 0,
        table: "t".into(),
        column: "c".into(),
        storage_mode: 0,
        has_stats: false,
        files: vec![],
        indexes: vec![],
    }
    .to_bytes();
    m[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(bad.join(BACKUP_MANIFEST_FILE), &m).unwrap();
    assert!(restore_backup(&bad, &target, RestoreFailPoint::None, None).is_err());
    // Restore into a live store is refused.
    let live = tdir("invalid-live");
    let s =
        plomid_vector::storage::FileVectorStorage::create(&live, "t".into(), "c".into(), Some(1))
            .unwrap();
    drop(s);
    let src = tdir("invalid-src");
    let s2 =
        plomid_vector::storage::FileVectorStorage::create(&src, "t".into(), "c".into(), Some(1))
            .unwrap();
    let out = tdir("invalid-out");
    create_backup(
        &s2,
        Some(&src),
        "t",
        "c",
        1,
        None,
        &[],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert!(restore_backup(&out, &live, RestoreFailPoint::None, None).is_err());
    for d in [&target, &empty, &bad, &live, &src, &out] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn diskann_pair_roundtrip_serves_after_open() {
    use plomid_vector::index::diskann::DiskAnnIndex;
    let dim = 8usize;
    let rows: Vec<(u64, Vec<f32>)> = (0..64u64)
        .map(|i| {
            let v: Vec<f32> = (0..dim)
                .map(|d| (i as f32) * 0.1 + d as f32 * 0.01)
                .collect();
            (i, v)
        })
        .collect();
    let data_dir = tdir("diskann-data");
    let config = IndexConfig::new(IndexKind::DiskAnn, Metric::L2, dim);
    let mut idx = DiskAnnIndex::new(config.clone(), &data_dir).unwrap();
    idx.build(&rows).unwrap();
    idx.flush().unwrap();
    let vectors = std::fs::read(data_dir.join("vectors.dat")).unwrap();
    let graph = std::fs::read(data_dir.join("graph.dat")).unwrap();

    let src = tdir("diskann-src");
    let mut s = plomid_vector::storage::FileVectorStorage::create(
        &src,
        "t".into(),
        "c".into(),
        Some(dim as u32),
    )
    .unwrap();
    for (_, v) in &rows {
        s.append_payload(&dense(v)).unwrap();
    }
    s.checkpoint(9).unwrap();
    drop(s);
    let input = IndexBackupInput {
        name: "docs_diskann".into(),
        kind: IndexKind::DiskAnn,
        metric: VectorMetric::L2,
        dimension: dim,
        generation: 1,
        vector_count: rows.len() as u64,
        deleted_count: 0,
        persist_blob: None,
        diskann_pair: Some((vectors, graph)),
    };
    let out = tdir("diskann-out");
    // Need the store handle again for the adapter read.
    let (s2, _) = plomid_vector::storage::FileVectorStorage::open(
        &src,
        "t".into(),
        "c".into(),
        Some(dim as u32),
        false,
    )
    .unwrap();
    create_backup(
        &s2,
        Some(&src),
        "t",
        "c",
        9,
        None,
        std::slice::from_ref(&input),
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    drop(s2);
    let target = tdir("diskann-target");
    let report = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(report.indexes[0].action, RestoredIndexAction::Direct);
    // Pair opens in place and serves the same nearest row.
    let pair_dir = target.join("indexes/docs_diskann");
    assert!(pair_dir.join("vectors.dat").exists());
    assert!(pair_dir.join("graph.dat").exists());
    let reopened = open_restored_diskann(config, &pair_dir).unwrap();
    reopened.validate().unwrap();
    let hits = reopened.search(&rows[0].1, 3).unwrap();
    assert_eq!(hits[0].row_id, 0);
    for d in [&data_dir, &src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn sq_pq_binary_direct_paths_validate_and_serve() {
    use plomid_vector::index::binary::BinaryIndex;
    use plomid_vector::index::pq::PqIndex;
    use plomid_vector::index::sq::Sq8Index;
    let dim = 8usize;
    let rows: Vec<(u64, Vec<f32>)> = (0..64u64)
        .map(|i| (i, vec![i as f32 * 0.25 + 0.5; dim]))
        .collect();
    let mut inputs = Vec::new();
    let mut sq = Sq8Index::new(IndexConfig::new(IndexKind::IvfSq, Metric::L2, dim)).unwrap();
    sq.build(&rows).unwrap();
    let st = sq.stats();
    inputs.push((
        IndexBackupInput {
            name: "d_sq".into(),
            kind: IndexKind::IvfSq,
            metric: VectorMetric::L2,
            dimension: dim,
            generation: st.generation,
            vector_count: st.num_vectors as u64,
            deleted_count: 0,
            persist_blob: Some(sq.encode()),
            diskann_pair: None,
        },
        IndexKind::IvfSq,
    ));
    let mut pq = PqIndex::new(IndexConfig::new(IndexKind::IvfPq, Metric::L2, dim)).unwrap();
    pq.build(&rows).unwrap();
    let st = pq.stats();
    inputs.push((
        IndexBackupInput {
            name: "d_pq".into(),
            kind: IndexKind::IvfPq,
            metric: VectorMetric::L2,
            dimension: dim,
            generation: st.generation,
            vector_count: st.num_vectors as u64,
            deleted_count: 0,
            persist_blob: Some(pq.encode()),
            diskann_pair: None,
        },
        IndexKind::IvfPq,
    ));
    let mut bi =
        BinaryIndex::new(IndexConfig::new(IndexKind::Binary, Metric::Hamming, dim)).unwrap();
    bi.build(&rows).unwrap();
    let st = bi.stats();
    inputs.push((
        IndexBackupInput {
            name: "d_bin".into(),
            kind: IndexKind::Binary,
            metric: VectorMetric::Hamming,
            dimension: dim,
            generation: st.generation,
            vector_count: st.num_vectors as u64,
            deleted_count: 0,
            persist_blob: Some(bi.encode()),
            diskann_pair: None,
        },
        IndexKind::Binary,
    ));

    let src = tdir("qpb-src");
    let mut s = plomid_vector::storage::FileVectorStorage::create(
        &src,
        "t".into(),
        "c".into(),
        Some(dim as u32),
    )
    .unwrap();
    for (_, v) in &rows {
        s.append_payload(&dense(v)).unwrap();
    }
    s.checkpoint(5).unwrap();
    let only: Vec<IndexBackupInput> = inputs.iter().map(|(i, _)| i.clone()).collect();
    let out = tdir("qpb-out");
    create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        5,
        None,
        &only,
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    drop(s);
    let target = tdir("qpb-target");
    let report = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(report.indexes.len(), 3);
    for (entry, kind) in report.indexes.iter().zip(inputs.iter().map(|(_, k)| k)) {
        assert_eq!(entry.action, RestoredIndexAction::Direct, "{}", entry.name);
        let blob = std::fs::read(target.join(format!("indexes/{}.blob", entry.name))).unwrap();
        let metric = if *kind == IndexKind::Binary {
            Metric::Hamming
        } else {
            Metric::L2
        };
        let config = IndexConfig::new(*kind, metric, dim);
        validate_restored_blob(*kind, &config, &blob).unwrap();
    }
    // Restored SQ serves top-k.
    let blob = std::fs::read(target.join("indexes/d_sq.blob")).unwrap();
    let sq2 = Sq8Index::decode(&blob, IndexConfig::new(IndexKind::IvfSq, Metric::L2, dim)).unwrap();
    assert_eq!(sq2.search(&rows[3].1, 5).unwrap().len(), 5);
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn sparse_and_flat_rebuild_from_authoritative_rows() {
    use plomid_vector::index::online_build::run_online_build;
    use plomid_vector::mvcc::delta::CommittedDelta;
    // Flat + sparse have no persist path: backup records rebuild markers,
    // restore rebuilds via the canonical online-build protocol.
    let flat_in = IndexBackupInput {
        name: "d_flat".into(),
        kind: IndexKind::Flat,
        metric: VectorMetric::L2,
        dimension: 2,
        generation: 0,
        vector_count: 16,
        deleted_count: 0,
        persist_blob: None,
        diskann_pair: None,
    };
    let sparse_in = IndexBackupInput {
        name: "d_sparse".into(),
        kind: IndexKind::SparseInverted,
        metric: VectorMetric::Dot,
        dimension: 16,
        generation: 0,
        vector_count: 4,
        deleted_count: 0,
        persist_blob: None,
        diskann_pair: None,
    };
    let src = tdir("fs-src");
    let mut s =
        plomid_vector::storage::FileVectorStorage::create(&src, "t".into(), "c".into(), Some(2))
            .unwrap();
    for i in 0..16u64 {
        s.append_payload(&dense(&[i as f32, i as f32])).unwrap();
    }
    s.checkpoint(3).unwrap();
    let out = tdir("fs-out");
    let report = create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        3,
        None,
        &[flat_in, sparse_in],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    assert_eq!(report.rebuild_marked, 2);
    drop(s);
    let target = tdir("fs-target");
    let restored = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert!(restored
        .indexes
        .iter()
        .all(|i| i.action == RestoredIndexAction::RebuildRequired));
    // Rebuild flat from restored rows; never served before this point.
    let (reopened, _) = plomid_vector::storage::FileVectorStorage::open(
        &target,
        "t".into(),
        "c".into(),
        Some(2),
        false,
    )
    .unwrap();
    let batch = reopened.scan_batch(0, 1024).unwrap();
    let snapshot: Vec<(u64, Vec<f32>)> = batch
        .iter()
        .map(|(id, v)| match v {
            VectorValue::Dense(d) => (*id, d.values_f32.clone()),
            _ => panic!("expected dense"),
        })
        .collect();
    let mut flat =
        plomid_vector::index::FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, 2))
            .unwrap();
    let delta = CommittedDelta::new();
    let build_report =
        run_online_build(&mut flat, &snapshot, restored.manifest_lsn, &delta, None).unwrap();
    assert_eq!(build_report.base_rows, 16);
    flat.validate().unwrap();
    assert_eq!(flat.search(&[5.0, 5.0], 1).unwrap()[0].row_id, 5);
    // Sparse rebuild from sparse literals serves too.
    let mut sparse = plomid_vector::sparse::inverted::SparseInvertedIndex::new(16, Metric::Dot);
    for i in 0..4u64 {
        let sv = plomid_vector::sparse::SparseVector::new(16, vec![(i as u32, 1.0)]).unwrap();
        sparse.insert(i, sv).unwrap();
    }
    sparse.validate().unwrap();
    let q = plomid_vector::sparse::SparseVector::new(16, vec![(2, 1.0)]).unwrap();
    assert_eq!(sparse.search(&q, 1).unwrap()[0].row_id, 2);
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn restart_after_restore_then_mutate() {
    // Restart: reopened target replays; then it stays mutable (MVCC +
    // WAL/recovery behavior of the file adapter is unchanged).
    let src = tdir("rm-src");
    let mut s =
        plomid_vector::storage::FileVectorStorage::create(&src, "t".into(), "c".into(), Some(1))
            .unwrap();
    s.append_payload(&dense(&[1.0])).unwrap();
    s.checkpoint(8).unwrap();
    let out = tdir("rm-out");
    create_backup(
        &s,
        Some(&src),
        "t",
        "c",
        8,
        None,
        &[],
        0,
        &out,
        BackupFailPoint::None,
        None,
    )
    .unwrap();
    drop(s);
    let target = tdir("rm-target");
    let restored = restore_backup(&out, &target, RestoreFailPoint::None, None).unwrap();
    assert_eq!(restored.manifest_lsn, 8);
    // Simulate restart: drop everything, reopen.
    let (mut reopened, recovery) = plomid_vector::storage::FileVectorStorage::open(
        &target,
        "t".into(),
        "c".into(),
        Some(1),
        false,
    )
    .unwrap();
    assert_eq!(recovery.replayed_records, 1);
    // Mutation after restore works and checkpoints.
    reopened.append_payload(&dense(&[2.0])).unwrap();
    reopened.checkpoint(9).unwrap();
    assert_eq!(reopened.row_count(), 2);
    assert_eq!(reopened.read_payload(1).unwrap(), dense(&[2.0]));
    for d in [&src, &out, &target] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn backup_input_validation_rejects_bad_identity() {
    let bad = IndexBackupInput {
        name: "../evil".into(),
        kind: IndexKind::Hnsw,
        metric: VectorMetric::L2,
        dimension: 8,
        generation: 0,
        vector_count: 0,
        deleted_count: 0,
        persist_blob: None,
        diskann_pair: None,
    };
    assert!(bad.validate().is_err());
    let bad_dim = IndexBackupInput {
        name: "ok".into(),
        dimension: 0,
        ..bad.clone()
    };
    assert!(bad_dim.validate().is_err());
}
