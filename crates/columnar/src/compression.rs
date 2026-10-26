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
//! Compression support for columnar chunks.
//!
//! This module is the byte-oriented codec layer only. Value encodings,
//! selection, and the self-describing chunk frame live in [`crate::encoding`],
//! which drives this facade. Both layers share the same codec tags, so a chunk
//! header, a frame, and this module can never disagree about what a payload is.

use crate::chunk::ChunkCompression;
#[cfg(feature = "zstd")]
use crate::layout::corruption;
use plomid_core::Result;
#[cfg(not(feature = "zstd"))]
use plomid_core::{ErrorKind, PlomidError};

/// Default compression to use for columnar data.
///
/// Tier-aware: edge/small-device hosts and slow (HDD-class) storage default
/// to Zstd (smaller segments spare constrained disks and slow I/O), while
/// all other hosts default to `None` (no codec CPU cost where I/O is not
/// the bottleneck). The tier comes from [`plomid_hw::RuntimeProfile`], so a
/// `PLOMID_PROFILE=edge` override also flips this default.
///
/// This default is deliberately conservative: it is *not* sibling A's
/// [`plomid_hw::RuntimeProfile::prefer_compression`] hint (which also
/// compresses on server-class hosts for capacity at scale and is consumed
/// by adaptive per-chunk selection). The default below governs only what an
/// explicit `FlushConfig::default()` stores; per-chunk selection may still
/// compress more aggressively elsewhere.
///
/// Multi-modal safety: the choice only affects newly flushed chunks. Every
/// chunk header carries its own compression tag
/// ([`ChunkCompression::tag`]), and every reader decodes through
/// [`ChunkCompression::from_tag`] + [`decompress`], which handle both tags —
/// so segments written under either default stay decodable by all readers
/// and decode to byte-identical rows. A build without the `zstd` feature
/// can never select Zstd here: [`is_compression_available`] gates it, and
/// the flush path re-validates via [`FlushConfig::validate`](crate::flush::FlushConfig).
///
/// The host profile is detected once per process (cached); per-tier behavior
/// without host dependence is available through
/// [`default_compression_for_profile`].
pub fn default_compression() -> ChunkCompression {
    use std::sync::OnceLock;
    static CACHED: OnceLock<ChunkCompression> = OnceLock::new();
    *CACHED.get_or_init(|| {
        let hardware = plomid_hw::HardwareProfile::detect();
        // `resolve(None)` honors `PLOMID_PROFILE` without a CLI value; a
        // malformed env value falls back to host classification here (the
        // server binary itself fails closed on it at startup).
        let tier = plomid_hw::RuntimeProfile::resolve(None)
            .map(|resolved| resolved.tier())
            .unwrap_or_else(|_| plomid_hw::RuntimeTier::classify(&hardware));
        default_compression_for_tier(tier, hardware.storage)
    })
}

/// Tier default for an explicit hardware profile.
///
/// Zstd when the profile classifies as
/// [`plomid_hw::RuntimeTier::Edge`] (via
/// [`plomid_hw::RuntimeTier::classify`], i.e. small-device) or sits on slow
/// HDD-class storage ([`plomid_hw::StorageKind::Hdd`]); `None` otherwise
/// (laptop/server tiers on fast or unknown storage, or a build where Zstd
/// is unavailable — see [`default_compression`]).
///
/// Pure over its input so tests can mock each tier without touching the host.
#[must_use]
pub fn default_compression_for_profile(profile: &plomid_hw::HardwareProfile) -> ChunkCompression {
    default_compression_for_tier(plomid_hw::RuntimeTier::classify(profile), profile.storage)
}

/// Tier default for an explicit tier plus storage kind.
///
/// Same rule as [`default_compression_for_profile`] with the classification
/// already done: edge tier or HDD storage selects Zstd (when the codec is
/// built in), everything else stays uncompressed.
#[must_use]
pub fn default_compression_for_tier(
    tier: plomid_hw::RuntimeTier,
    storage: plomid_hw::StorageKind,
) -> ChunkCompression {
    let edge = matches!(tier, plomid_hw::RuntimeTier::Edge)
        || matches!(storage, plomid_hw::StorageKind::Hdd);
    if edge && is_compression_available(ChunkCompression::Zstd) {
        ChunkCompression::Zstd
    } else {
        ChunkCompression::None
    }
}

/// Compresses data using the specified compression method.
pub fn compress(data: &[u8], compression: ChunkCompression) -> Result<Vec<u8>> {
    match compression {
        ChunkCompression::None => Ok(data.to_vec()),
        ChunkCompression::Zstd => {
            #[cfg(feature = "zstd")]
            {
                Ok(zstd::encode_all(data, 3)?)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err(PlomidError::new(
                    ErrorKind::Unsupported,
                    "zstd not available",
                ))
            }
        }
    }
}

/// Decompresses data using the specified compression method.
///
/// `expected_size` is the length the caller's framing declares. It is passed to
/// the codec as a hard output bound, so a payload that claims to expand beyond
/// it is rejected by the codec instead of being materialized. The declared
/// length must therefore be validated by the caller before it reaches here; the
/// columnar read path checks it against the format's per-chunk limit.
pub fn decompress(
    data: &[u8],
    expected_size: u64,
    compression: ChunkCompression,
) -> Result<Vec<u8>> {
    match compression {
        ChunkCompression::None => Ok(data.to_vec()),
        ChunkCompression::Zstd => {
            #[cfg(feature = "zstd")]
            {
                let capacity = usize::try_from(expected_size)
                    .map_err(|_| corruption("declared chunk size does not fit in memory"))?;
                // `bulk::decompress` writes at most `capacity` bytes and fails
                // rather than growing, so the declared length bounds the
                // allocation even for a hostile frame.
                Ok(zstd::bulk::decompress(data, capacity)?)
            }
            #[cfg(not(feature = "zstd"))]
            {
                let _ = expected_size;
                Err(PlomidError::new(
                    ErrorKind::Unsupported,
                    "zstd not available",
                ))
            }
        }
    }
}

/// Returns true if compression is available for the given method.
pub fn is_compression_available(compression: ChunkCompression) -> bool {
    match compression {
        ChunkCompression::None => true,
        #[cfg(feature = "zstd")]
        ChunkCompression::Zstd => true,
        #[cfg(not(feature = "zstd"))]
        ChunkCompression::Zstd => false,
    }
}

/// Estimates compressed size for planning chunk layout.
pub fn estimate_compressed_size(uncompressed_size: usize) -> usize {
    (uncompressed_size as f64 * 0.7) as usize
}
