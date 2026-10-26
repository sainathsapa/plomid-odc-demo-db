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
#![forbid(unsafe_code)]
//! `VectorComputeBackend`: one trait over CPU and (optional) GPU
//! execution for distance batches, exact top-k, ANN search, index
//! builds, synchronization, and cancellation.
//!
//! The CPU backend always works. GPU access goes through [`GpuStub`]
//! (feature `gpu`), which reports [`ComputeDevice`] entries but
//! returns [`VectorError::IndexUnsupported`] for compute until a real GPU
//! kernel is linked. Callers use [`execute_with_cpu_fallback`] for
//! graceful degradation.
//!
//! NOTE: this file intentionally lives outside `math/` (owned by the
//! SIMD-dispatch task) per the "do not overwrite" rule; it only
//! depends on `crate::{Result, VectorError}`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::{Result, VectorError};

/// Cancellation error: cooperative cancellation was requested.
fn cancelled_err() -> VectorError {
    VectorError::Cancelled("operation cancelled".to_string())
}

/// Distance metric for brute-force kernels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistanceKind {
    /// Squared Euclidean distance.
    L2Squared,
    /// `1 - cosine similarity` (`1.0` on zero vectors).
    Cosine,
    /// Raw dot product (larger = closer).
    InnerProduct,
}

/// Device class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeviceKind {
    /// Synchronous host CPU device.
    Cpu,
    /// Discrete/accelerator GPU device.
    Gpu,
}

/// An addressable compute device.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComputeDevice {
    /// Backend-local device id.
    pub id: u32,
    /// Device class.
    pub kind: DeviceKind,
    /// Stable display name (e.g. `cpu:0`).
    pub name: String,
}

/// Per-device capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCapabilities {
    /// Maximum supported dimensionality.
    pub max_dims: usize,
    /// Maximum vectors per batch call.
    pub max_batch_elements: usize,
    /// Exact distance/top-k supported.
    pub supports_exact: bool,
    /// ANN search supported (CPU: exact fallback).
    pub supports_ann: bool,
    /// Index build supported.
    pub supports_index_build: bool,
    /// SIMD lane width hint (`0` = unknown/none).
    pub simd_width: usize,
}

/// Aggregate capability discovery result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityReport {
    /// Every enumerated device with its capabilities.
    pub devices: Vec<(ComputeDevice, DeviceCapabilities)>,
}

/// Backend-level capability summary (all devices, GPU presence).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendCapabilities {
    /// [`VectorComputeBackend::name`] of the reporting backend.
    pub backend_name: String,
    /// Number of enumerated devices.
    pub device_count: usize,
    /// `true` when at least one GPU device is present.
    pub has_gpu: bool,
    /// Maximum `max_dims` across devices (`0` when empty).
    pub max_dims: usize,
}

impl BackendCapabilities {
    /// Summarize a [`CapabilityReport`] for `backend_name`.
    #[must_use]
    pub fn from_report(backend_name: &str, report: &CapabilityReport) -> Self {
        Self {
            backend_name: backend_name.to_string(),
            device_count: report.devices.len(),
            has_gpu: report
                .devices
                .iter()
                .any(|(d, _)| d.kind == DeviceKind::Gpu),
            max_dims: report
                .devices
                .iter()
                .map(|(_, c)| c.max_dims)
                .max()
                .unwrap_or(0),
        }
    }
}

/// Opaque handle returned by `build_index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexHandle {
    /// Backend-allocated index id.
    pub id: u64,
    /// Rows covered by the handle.
    pub row_count: u64,
}

/// Compute backend. All methods take `&self` so backends are
/// shareable; cancellation uses interior mutability.
pub trait VectorComputeBackend: Send + Sync {
    /// Stable backend name (`"cpu"`, `"gpu-stub"`, ...).
    fn name(&self) -> &'static str;

    /// Devices this backend can address (never empty for CPU).
    fn enumerate_devices(&self) -> Vec<ComputeDevice>;

    /// Capabilities for one device id from [`Self::enumerate_devices`].
    fn capabilities(&self, device_id: u32) -> DeviceCapabilities;

    /// Pairwise distances from `query` to each row of `base`
    /// (both packed `dims`-wide, `base.len() % dims == 0`).
    fn distance_batch(
        &self,
        device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        metric: DistanceKind,
    ) -> Result<Vec<f32>>;

    /// Exact top-`k` smallest-distance rows (`(row_offset, distance)`).
    fn topk_exact(
        &self,
        device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        k: usize,
        metric: DistanceKind,
    ) -> Result<Vec<(u64, f32)>>;

    /// Approximate search; CPU impl falls back to exact.
    ///
    /// The seven parameters are the trait contract (device, query, base,
    /// shape, depth, metric, beam); grouping them would churn every
    /// backend for no behavioral gain.
    #[allow(clippy::too_many_arguments)]
    fn ann_search(
        &self,
        device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        k: usize,
        metric: DistanceKind,
        ef: usize,
    ) -> Result<Vec<(u64, f32)>>;

    /// Build (or rebuild) an index over `base`; CPU impl validates
    /// and returns a handle without persisting accelerator state.
    fn build_index(
        &self,
        device_id: u32,
        base: &[f32],
        dims: usize,
        metric: DistanceKind,
    ) -> Result<IndexHandle>;

    /// Block until previously issued work on the device completes.
    /// CPU impl is synchronous and returns `Ok`.
    fn synchronize(&self, device_id: u32) -> Result<()>;

    /// Request cooperative cancellation of in-flight work.
    fn request_cancel(&self);

    /// `true` after [`Self::request_cancel`] until [`Self::clear_cancel`].
    fn is_cancelled(&self) -> bool;

    /// Clear a pending cancellation request.
    fn clear_cancel(&self);
}

fn check_shapes(query: &[f32], base: &[f32], dims: usize) -> Result<usize> {
    if dims == 0 {
        return Err(VectorError::IndexParameterInvalid(
            "dims must be > 0".into(),
        ));
    }
    if query.len() != dims {
        return Err(VectorError::DimensionMismatch {
            expected: dims as u32,
            got: query.len() as u32,
        });
    }
    if !base.len().is_multiple_of(dims) {
        return Err(VectorError::IndexParameterInvalid(
            "base len must be a multiple of dims".into(),
        ));
    }
    Ok(base.len() / dims)
}

fn distance_one(query: &[f32], row: &[f32], metric: DistanceKind) -> f32 {
    match metric {
        DistanceKind::L2Squared => query
            .iter()
            .zip(row.iter())
            .map(|(a, b)| {
                let d = a - b;
                d * d
            })
            .sum(),
        DistanceKind::InnerProduct => query.iter().zip(row.iter()).map(|(a, b)| a * b).sum(),
        DistanceKind::Cosine => {
            let dot: f32 = query.iter().zip(row.iter()).map(|(a, b)| a * b).sum();
            let nq: f32 = query.iter().map(|a| a * a).sum::<f32>().sqrt();
            let nb: f32 = row.iter().map(|b| b * b).sum::<f32>().sqrt();
            if nq == 0.0 || nb == 0.0 {
                1.0
            } else {
                1.0 - dot / (nq * nb)
            }
        }
    }
}

/// Always-available CPU backend (brute force, synchronous).
#[derive(Debug, Clone)]
pub struct CpuBackend {
    cancelled: Arc<AtomicBool>,
    next_index: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for CpuBackend {
    fn default() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            next_index: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }
}

impl CpuBackend {
    /// Fresh CPU backend with cleared cancellation state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn check_cancel(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(cancelled_err());
        }
        Ok(())
    }
}

impl VectorComputeBackend for CpuBackend {
    fn name(&self) -> &'static str {
        "cpu"
    }

    fn enumerate_devices(&self) -> Vec<ComputeDevice> {
        vec![ComputeDevice {
            id: 0,
            kind: DeviceKind::Cpu,
            name: "cpu:0".to_string(),
        }]
    }

    fn capabilities(&self, _device_id: u32) -> DeviceCapabilities {
        DeviceCapabilities {
            max_dims: 65_536,
            max_batch_elements: usize::MAX,
            supports_exact: true,
            supports_ann: true,
            supports_index_build: true,
            simd_width: 8,
        }
    }

    fn distance_batch(
        &self,
        _device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        metric: DistanceKind,
    ) -> Result<Vec<f32>> {
        let n = check_shapes(query, base, dims)?;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            self.check_cancel()?;
            out.push(distance_one(query, &base[i * dims..(i + 1) * dims], metric));
        }
        Ok(out)
    }

    fn topk_exact(
        &self,
        device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        k: usize,
        metric: DistanceKind,
    ) -> Result<Vec<(u64, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let dists = self.distance_batch(device_id, query, base, dims, metric)?;
        let mut pairs: Vec<(u64, f32)> = dists
            .into_iter()
            .enumerate()
            .map(|(i, d)| (i as u64, d))
            .collect();
        pairs.sort_by(|a, b| a.1.total_cmp(&b.1));
        pairs.truncate(k.min(pairs.len()));
        Ok(pairs)
    }

    fn ann_search(
        &self,
        device_id: u32,
        query: &[f32],
        base: &[f32],
        dims: usize,
        k: usize,
        metric: DistanceKind,
        _ef: usize,
    ) -> Result<Vec<(u64, f32)>> {
        // No approximation on CPU: exact is the correct fallback.
        self.topk_exact(device_id, query, base, dims, k, metric)
    }

    fn build_index(
        &self,
        _device_id: u32,
        base: &[f32],
        dims: usize,
        _metric: DistanceKind,
    ) -> Result<IndexHandle> {
        if dims == 0 {
            return Err(VectorError::IndexParameterInvalid(
                "dims must be > 0".into(),
            ));
        }
        if !base.len().is_multiple_of(dims) {
            return Err(VectorError::IndexParameterInvalid(
                "base len must be a multiple of dims".into(),
            ));
        }
        self.check_cancel()?;
        let id = self.next_index.fetch_add(1, Ordering::Relaxed);
        Ok(IndexHandle {
            id,
            row_count: (base.len() / dims) as u64,
        })
    }

    fn synchronize(&self, _device_id: u32) -> Result<()> {
        // Synchronous backend: nothing outstanding.
        self.check_cancel()
    }

    fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    fn clear_cancel(&self) {
        self.cancelled.store(false, Ordering::Relaxed);
    }
}

/// GPU stub. Reports one device for discovery, but every compute
/// entry returns [`VectorError::IndexUnsupported`] until a real GPU
/// kernel backend is linked.
#[cfg(feature = "gpu")]
#[derive(Debug, Clone, Default)]
pub struct GpuStub {
    cancelled: Arc<AtomicBool>,
}

#[cfg(feature = "gpu")]
impl GpuStub {
    /// Construct the GPU stub (unlinked kernels; execution degrades to CPU).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "gpu")]
impl VectorComputeBackend for GpuStub {
    fn name(&self) -> &'static str {
        "gpu-stub"
    }

    fn enumerate_devices(&self) -> Vec<ComputeDevice> {
        vec![ComputeDevice {
            id: 0,
            kind: DeviceKind::Gpu,
            name: "gpu-stub:0".to_string(),
        }]
    }

    fn capabilities(&self, _device_id: u32) -> DeviceCapabilities {
        DeviceCapabilities {
            max_dims: 0,
            max_batch_elements: 0,
            supports_exact: false,
            supports_ann: false,
            supports_index_build: false,
            simd_width: 0,
        }
    }

    fn distance_batch(
        &self,
        _device_id: u32,
        _query: &[f32],
        _base: &[f32],
        _dims: usize,
        _metric: DistanceKind,
    ) -> Result<Vec<f32>> {
        Err(VectorError::IndexUnsupported(
            "gpu backend not linked; enable a real gpu kernel".into(),
        ))
    }

    fn topk_exact(
        &self,
        _device_id: u32,
        _query: &[f32],
        _base: &[f32],
        _dims: usize,
        _k: usize,
        _metric: DistanceKind,
    ) -> Result<Vec<(u64, f32)>> {
        Err(VectorError::IndexUnsupported(
            "gpu backend not linked".into(),
        ))
    }

    fn ann_search(
        &self,
        _device_id: u32,
        _query: &[f32],
        _base: &[f32],
        _dims: usize,
        _k: usize,
        _metric: DistanceKind,
        _ef: usize,
    ) -> Result<Vec<(u64, f32)>> {
        Err(VectorError::IndexUnsupported(
            "gpu backend not linked".into(),
        ))
    }

    fn build_index(
        &self,
        _device_id: u32,
        _base: &[f32],
        _dims: usize,
        _metric: DistanceKind,
    ) -> Result<IndexHandle> {
        Err(VectorError::IndexUnsupported(
            "gpu backend not linked".into(),
        ))
    }

    fn synchronize(&self, _device_id: u32) -> Result<()> {
        Err(VectorError::IndexUnsupported(
            "gpu backend not linked".into(),
        ))
    }

    fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    fn clear_cancel(&self) {
        self.cancelled.store(false, Ordering::Relaxed);
    }
}

/// Discover every device plus its capabilities for a backend.
#[must_use]
pub fn discover_capabilities(backend: &dyn VectorComputeBackend) -> CapabilityReport {
    let devices = backend
        .enumerate_devices()
        .into_iter()
        .map(|d| {
            let caps = backend.capabilities(d.id);
            (d, caps)
        })
        .collect();
    CapabilityReport { devices }
}

/// Pick the first device of the preferred class, falling back to
/// whatever the backend enumerates (`None` only if no devices).
#[must_use]
pub fn select_device(
    backend: &dyn VectorComputeBackend,
    prefer_gpu: bool,
) -> Option<ComputeDevice> {
    let devices = backend.enumerate_devices();
    if prefer_gpu {
        if let Some(d) = devices.iter().find(|d| d.kind == DeviceKind::Gpu) {
            return Some(d.clone());
        }
    }
    devices.into_iter().next()
}

/// Backend choice returned by graceful-degradation helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyBackendChoice {
    /// Cpu variant.
    Cpu,
    /// GPU stub variant (unlinked kernels; always degrades to CPU).
    #[cfg(feature = "gpu")]
    GpuStub,
}

/// Construct the default CPU backend (always available).
#[must_use]
pub fn default_cpu_backend() -> CpuBackend {
    CpuBackend::new()
}

/// Run `primary`; when it fails with [`VectorError::IndexUnsupported`],
/// run `cpu_op` on a fresh [`CpuBackend`] instead. Other errors
/// propagate unchanged.
pub fn execute_with_cpu_fallback<T>(
    primary: impl FnOnce() -> Result<T>,
    cpu_op: impl FnOnce(&CpuBackend) -> Result<T>,
) -> Result<T> {
    match primary() {
        Ok(v) => Ok(v),
        Err(VectorError::IndexUnsupported(_)) => {
            let cpu = CpuBackend::new();
            cpu_op(&cpu)
        }
        Err(e) => Err(e),
    }
}
