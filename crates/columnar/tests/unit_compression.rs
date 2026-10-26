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
// Moved from src/compression.rs inline #[cfg(test)].
mod tier_tests {
    use plomid_columnar::chunk::ChunkCompression;
    use plomid_columnar::compression::*;

    use plomid_hw::{HardwareProfile, StorageKind};

    fn profile(
        logical_cpus: usize,
        ram_bytes: Option<u64>,
        storage: StorageKind,
    ) -> HardwareProfile {
        HardwareProfile {
            arch: plomid_hw::Architecture::X86_64,
            cpu: plomid_hw::CpuFeatures {
                scalar: true,
                sse42: false,
                avx2: false,
                avx512: false,
                neon: false,
            },
            logical_cpus,
            physical_cpus: None,
            cache_bytes: [None, None, None],
            ram_total_bytes: ram_bytes,
            ram_available_bytes: None,
            storage,
            storage_bandwidth_bps: None,
            storage_iops_estimate: None,
            numa_nodes: None,
            notes: Vec::new(),
        }
    }

    fn edge_default() -> ChunkCompression {
        if is_compression_available(ChunkCompression::Zstd) {
            ChunkCompression::Zstd
        } else {
            // A build without the zstd codec can never select it, even on
            // edge hardware; the flush path would reject it at validate().
            ChunkCompression::None
        }
    }

    #[test]
    fn edge_tier_defaults_to_edge_compression() {
        assert_eq!(
            default_compression_for_tier(plomid_hw::RuntimeTier::Edge, StorageKind::Ssd),
            edge_default()
        );
        // Tier-level entry agrees with the profile-level entry.
        let small = profile(2, Some(2 * 1024 * 1024 * 1024), StorageKind::Ssd);
        assert_eq!(
            plomid_hw::RuntimeTier::classify(&small),
            plomid_hw::RuntimeTier::Edge
        );
        assert_eq!(default_compression_for_profile(&small), edge_default());
    }

    #[test]
    fn hdd_defaults_to_edge_compression_on_any_tier() {
        use plomid_hw::RuntimeTier::{Edge, Laptop, Server};
        for tier in [Edge, Laptop, Server] {
            assert_eq!(
                default_compression_for_tier(tier, StorageKind::Hdd),
                edge_default(),
                "tier={tier:?}"
            );
        }
        // A server-class host on HDD classifies away from Edge yet still
        // compresses by default because the storage is slow.
        let hdd_server = profile(8, Some(32 * 1024 * 1024 * 1024), StorageKind::Hdd);
        assert_ne!(
            plomid_hw::RuntimeTier::classify(&hdd_server),
            plomid_hw::RuntimeTier::Edge
        );
        assert_eq!(default_compression_for_profile(&hdd_server), edge_default());
    }

    #[test]
    fn laptop_and_server_tiers_stay_uncompressed_on_fast_storage() {
        use plomid_hw::RuntimeTier::{Laptop, Server};
        for tier in [Laptop, Server] {
            for storage in [StorageKind::Nvme, StorageKind::Ssd, StorageKind::Unknown] {
                assert_eq!(
                    default_compression_for_tier(tier, storage),
                    ChunkCompression::None,
                    "tier={tier:?} storage={storage:?}"
                );
            }
        }
    }

    #[test]
    fn unknown_ram_never_forces_the_edge_profile() {
        // `is_small_device` requires known-small RAM; unknown RAM stays on
        // the server default even with few CPUs.
        let unknown_ram = profile(2, None, StorageKind::Ssd);
        assert!(!unknown_ram.is_small_device());
        assert_eq!(
            default_compression_for_profile(&unknown_ram),
            ChunkCompression::None
        );
    }
}
