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
// Moved from `src/checkpoint.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::checkpoint::*;

#[test]
fn encoding_is_deterministic_and_round_trips() {
    let meta = CheckpointMetadata {
        checkpoint_generation: GenerationId::new(7),
        checkpoint_lsn: Lsn::new(9),
        storage_generation: GenerationId::new(3),
        catalog_generation: CatalogVersion::new(4),
        metadata: vec![9, 8, 7],
    };
    assert_eq!(
        meta.encode().expect("encode"),
        meta.encode().expect("encode")
    );
    assert_eq!(
        CheckpointMetadata::decode(&meta.encode().expect("encode")).expect("decode"),
        meta
    );
}

#[test]
fn rejects_corruption_matrix() {
    let valid = CheckpointMetadata {
        checkpoint_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(1),
        storage_generation: GenerationId::new(1),
        catalog_generation: CatalogVersion::new(0),
        metadata: vec![5, 6],
    }
    .encode()
    .expect("encode");
    let mut bad = valid.clone();
    bad[0] = b'X';
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad[OFF_VERSION] = 0xFF;
    assert!(CheckpointMetadata::decode(&bad).is_err());
    assert!(CheckpointMetadata::decode(&valid[..10]).is_err());
    assert!(CheckpointMetadata::decode(&valid[..CHECKPOINT_HEADER_SIZE + 1]).is_err());
    let mut bad = valid.clone();
    bad[OFF_META_LEN] = 0xFF;
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xFF;
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad[OFF_CHECKSUM] ^= 0xFF;
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad[OFF_RESERVED] = 1;
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad.push(0);
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad[OFF_CKPT_GEN..OFF_CKPT_GEN + 8].copy_from_slice(&0u64.to_le_bytes());
    assert!(CheckpointMetadata::decode(&bad).is_err());
    let mut bad = valid.clone();
    bad[OFF_CKPT_LSN..OFF_CKPT_LSN + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(CheckpointMetadata::decode(&bad).is_err());
}

use plomid_core::{CatalogVersion, GenerationId, Lsn, PlomidError};
use std::sync::atomic::{AtomicU64, Ordering};

fn scratch(label: &str) -> std::path::PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-ckpt-{label}-{}-{id}", std::process::id()))
}

fn request(lsn: u64, storage: u64, catalog: u64) -> CheckpointRequest {
    CheckpointRequest {
        checkpoint_lsn: Lsn::new(lsn),
        storage_generation: GenerationId::new(storage),
        catalog_generation: CatalogVersion::new(catalog),
        metadata: vec![1, 2, 3],
    }
}

#[test]
fn publication_is_atomic_and_falls_back() {
    let root = scratch("publish");
    let result = (|| {
        let first = create_checkpoint(&root, request(1, 1, 0))?;
        assert_eq!(first.checkpoint_generation.get(), 1);
        let dir = root.join(CHECKPOINT_DIR_NAME);
        std::fs::write(
            dir.join("checkpoint-00000000000000000002.ckpt.tmp"),
            b"partial",
        )?;
        std::fs::write(dir.join("garbage.txt"), b"nope")?;
        let selected = latest_valid(&root)?.expect("checkpoint");
        assert_eq!(selected.1.checkpoint_generation.get(), 1);
        let second = create_checkpoint(&root, request(2, 2, 1))?;
        assert_eq!(second.checkpoint_generation.get(), 2);
        let path = dir.join("checkpoint-00000000000000000002.ckpt");
        let mut bytes = std::fs::read(&path)?;
        bytes[10] ^= 0xFF;
        std::fs::write(&path, bytes)?;
        let fallback = latest_valid(&root)?.expect("fallback");
        assert_eq!(fallback.1.checkpoint_generation.get(), 1);
        Ok::<(), PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(&root);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn stale_state_is_rejected() {
    let root = scratch("stale");
    let result = (|| {
        create_checkpoint(&root, request(5, 3, 2))?;
        assert!(create_checkpoint(&root, request(4, 3, 2)).is_err());
        assert!(create_checkpoint(&root, request(5, 2, 2)).is_err());
        Ok::<(), PlomidError>(())
    })();
    let _ = std::fs::remove_dir_all(&root);
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn failure_at_every_boundary_keeps_previous() {
    for point in [
        FailPoint::BeforeBuild,
        FailPoint::AfterBuild,
        FailPoint::AfterFlush,
        FailPoint::DuringVerify,
        FailPoint::BeforeSync,
        FailPoint::AfterSync,
        FailPoint::DuringPublish,
    ] {
        let root = scratch("fail");
        let result = (|| {
            create_checkpoint(&root, request(1, 1, 0))?;
            assert!(create_checkpoint_with_fail_point(&root, request(2, 2, 1), point).is_err());
            let selected = latest_valid(&root)?.expect("previous survives");
            assert_eq!(selected.1.checkpoint_generation.get(), 1);
            Ok::<(), PlomidError>(())
        })();
        let _ = std::fs::remove_dir_all(&root);
        assert!(result.is_ok(), "point {point:?}: {result:?}");
    }
}

#[test]
fn undurable_lsn_claim_is_rejected() {
    let checkpoint = CheckpointMetadata {
        checkpoint_generation: GenerationId::new(1),
        checkpoint_lsn: Lsn::new(10),
        storage_generation: GenerationId::new(1),
        catalog_generation: CatalogVersion::new(0),
        metadata: Vec::new(),
    };
    assert!(checkpoint.check_durable(Lsn::new(9)).is_err());
    assert!(checkpoint.check_durable(Lsn::new(10)).is_ok());
}
