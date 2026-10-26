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

//! Unified workload-aware buffer/cache manager: OLAP scans must not evict
//! the OLTP working set. Every byte carries a [`CacheClass`] + [`Temperature`].
//! OLTP/WAL enter Hot behind an `oltp_min_bytes` fence; OLAP scans default to
//! Streaming and bypass the cache until reuse promotes them (Streaming ->
//! Warm -> Hot). Eviction runs cold-first, HOT OLTP last (Critical only).
//! Only `std` is used; counters are atomics, entries behind one mutex.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use crate::config::PressureState;

/// Hook for cache participation in memory-pressure response.
///
/// Any cache (page cache, plan cache, result cache) plugs into HTAP
/// governance by implementing this trait and having its occupancy counted in
/// the governor's global stats (see
/// [`ResourceGovernor::account_cache_bytes`](crate::governor::ResourceGovernor::account_cache_bytes)).
/// [`UnifiedBufferManager`] is the canonical implementation; its
/// [`evict_under_pressure`](UnifiedBufferManager::evict_under_pressure)
/// sheds exactly [`CachePolicy::target_eviction_bytes`] worth of entries, so
/// advisory sizing and actual eviction can never disagree.
pub trait CachePolicy {
    /// Stable policy name for diagnostics.
    fn name(&self) -> &str;
    /// Bytes this cache *should* evict under `pressure`. Advisory: the
    /// governor uses it to size `evict_under_pressure` calls.
    fn target_eviction_bytes(&self, pressure: PressureState) -> u64;
}

/// Eviction fraction (% of occupancy) per pressure state. Single sizing
/// source behind [`CachePolicy`].
pub fn eviction_pct(pressure: PressureState) -> u64 {
    match pressure {
        PressureState::Normal => 0,
        PressureState::Warning => 10,
        PressureState::Pressure => 25,
        PressureState::Critical => 50,
    }
}

/// Which workload owns a cached byte (HOT/WARM/COLD/STREAMING tiers mapped
/// onto concrete engine content).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheClass {
    OltpHot,   // OLTP point-lookup rows. Fenced by reservation.
    OlapChunk, // OLAP scan chunks. Streaming by default.
    Metadata,  // Catalog / schema pages.
    Index,     // B-tree / vector index pages.
    Wal,       // WAL / redo staging buffers.
    Temp,      // Sort / hash / spill intermediates. Bypass-first.
}

impl CacheClass {
    /// All classes in stable order (index into per-class counters).
    pub const ALL: [CacheClass; 6] = [
        CacheClass::OltpHot,
        CacheClass::OlapChunk,
        CacheClass::Metadata,
        CacheClass::Index,
        CacheClass::Wal,
        CacheClass::Temp,
    ];

    fn index(self) -> usize {
        match self {
            CacheClass::OltpHot => 0,
            CacheClass::OlapChunk => 1,
            CacheClass::Metadata => 2,
            CacheClass::Index => 3,
            CacheClass::Wal => 4,
            CacheClass::Temp => 5,
        }
    }

    /// Stable lowercase name for diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            CacheClass::OltpHot => "oltp_hot",
            CacheClass::OlapChunk => "olap_chunk",
            CacheClass::Metadata => "metadata",
            CacheClass::Index => "index",
            CacheClass::Wal => "wal",
            CacheClass::Temp => "temp",
        }
    }

    /// One-shot scans of these classes bypass rather than occupy.
    fn is_bypassable(self) -> bool {
        matches!(self, CacheClass::OlapChunk | CacheClass::Temp)
    }
}

/// Cache temperature: eviction priority from cold (first) to hot (last).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Temperature {
    Hot,
    Warm,
    Cold,
    Streaming,
}

/// One cached entry.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub key: String,
    pub class: CacheClass,
    pub temp: Temperature,
    pub bytes: u64,
    pub hits: u64,
    pub last_tick: u64,
}

/// Admission decision for a cache insert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitDecision {
    /// Inserted (or already resident) at this temperature.
    Admit { temp: Temperature },
    /// One-shot scan: bypass the cache, occupy nothing, evict nothing.
    AdmitStreaming,
    /// No room even after shedding evictable bytes.
    Reject { reason: String },
}

impl AdmitDecision {
    pub fn is_admitted(&self) -> bool {
        matches!(self, AdmitDecision::Admit { .. })
    }
    pub fn is_streaming(&self) -> bool {
        matches!(self, AdmitDecision::AdmitStreaming)
    }
    pub fn is_rejected(&self) -> bool {
        matches!(self, AdmitDecision::Reject { .. })
    }
}

/// Point-in-time snapshot of manager counters.
#[derive(Debug, Clone)]
pub struct BufferStats {
    pub occupancy_bytes: u64,
    pub capacity_bytes: u64,
    pub entry_count: usize,
    pub hits_total: u64,
    pub misses_total: u64,
    pub evicted_total_bytes: u64,
    pub streaming_bypass_count: u64,
    pub hits_by_class: HashMap<CacheClass, u64>,
    pub misses_by_class: HashMap<CacheClass, u64>,
    pub evicted_bytes_by_class: HashMap<CacheClass, u64>,
}

/// Unified workload-aware buffer manager.
#[derive(Debug)]
pub struct UnifiedBufferManager {
    capacity_bytes: u64,
    oltp_min_bytes: u64,
    occupancy: AtomicU64,
    tick: AtomicU64,
    entries: Mutex<HashMap<String, CacheEntry>>,
    hits: [AtomicU64; 6],
    misses: [AtomicU64; 6],
    evicted_bytes: [AtomicU64; 6],
    bypass_count: AtomicU64,
}

impl UnifiedBufferManager {
    /// Create a manager holding at most `capacity_bytes` with
    /// `oltp_reserve_bytes` fenced for HOT OLTP (clamped to capacity).
    pub fn new(capacity_bytes: u64, oltp_reserve_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            oltp_min_bytes: oltp_reserve_bytes.min(capacity_bytes),
            occupancy: AtomicU64::new(0),
            tick: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
            hits: [(); 6].map(|()| AtomicU64::new(0)),
            misses: [(); 6].map(|()| AtomicU64::new(0)),
            evicted_bytes: [(); 6].map(|()| AtomicU64::new(0)),
            bypass_count: AtomicU64::new(0),
        }
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }
    pub fn oltp_reserve_bytes(&self) -> u64 {
        self.oltp_min_bytes
    }
    /// Current occupancy in bytes.
    pub fn occupancy(&self) -> u64 {
        self.occupancy.load(Ordering::Relaxed)
    }
    /// Number of resident entries.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("buffer lock").len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes held by one class (sums resident entries).
    pub fn class_bytes(&self, class: CacheClass) -> u64 {
        self.entries
            .lock()
            .expect("buffer lock")
            .values()
            .filter(|e| e.class == class)
            .map(|e| e.bytes)
            .sum()
    }

    /// True when `key` is resident.
    pub fn contains(&self, key: &str) -> bool {
        self.entries.lock().expect("buffer lock").contains_key(key)
    }

    /// Temperature of a resident entry, if any.
    pub fn temperature_of(&self, key: &str) -> Option<Temperature> {
        self.entries
            .lock()
            .expect("buffer lock")
            .get(key)
            .map(|e| e.temp)
    }

    /// Advisory scan-bypass check for operators that know their scan shape.
    /// `Temp` always bypasses; `OlapChunk` bypasses on low reuse probability
    /// (large one-shot scans bypass even at moderate reuse); point-lookup
    /// classes (`OltpHot`, `Metadata`, `Index`, `Wal`) never bypass.
    pub fn should_bypass_cache(
        class: CacheClass,
        estimated_rows: u64,
        reuse_probability: f64,
    ) -> bool {
        match class {
            CacheClass::Temp => true,
            CacheClass::OlapChunk => {
                if !reuse_probability.is_finite() {
                    return true;
                }
                reuse_probability < 0.5 || (estimated_rows >= 100_000 && reuse_probability < 0.8)
            }
            _ => false,
        }
    }

    /// Initial temperature for a fresh insert. OLAP without a reuse hint and
    /// Temp start Streaming; an explicit "won't reuse" hint parks
    /// Metadata/Index in Cold.
    fn initial_temp(class: CacheClass, reuse_hint: Option<bool>) -> Temperature {
        match (class, reuse_hint) {
            (CacheClass::OltpHot, _) | (CacheClass::Wal, _) => Temperature::Hot,
            (CacheClass::Temp, _) => Temperature::Streaming,
            (CacheClass::OlapChunk, Some(true)) => Temperature::Warm,
            (CacheClass::OlapChunk, _) => Temperature::Streaming,
            (CacheClass::Metadata, Some(false)) | (CacheClass::Index, Some(false)) => {
                Temperature::Cold
            }
            (CacheClass::Metadata, _) | (CacheClass::Index, _) => Temperature::Warm,
        }
    }

    /// Admit `bytes` under `key`. One-shot scans (`OlapChunk`/`Temp` without
    /// a reuse hint) bypass via [`AdmitDecision::AdmitStreaming`]. Otherwise
    /// the entry is inserted, evicting victims first when full; `Reject`
    /// only when even full shedding cannot make room.
    pub fn admit(
        &self,
        key: &str,
        class: CacheClass,
        bytes: u64,
        reuse_hint: Option<bool>,
    ) -> AdmitDecision {
        let temp = Self::initial_temp(class, reuse_hint);
        if temp == Temperature::Streaming && class.is_bypassable() && reuse_hint != Some(true) {
            self.misses[class.index()].fetch_add(1, Ordering::Relaxed);
            self.bypass_count.fetch_add(1, Ordering::Relaxed);
            return AdmitDecision::AdmitStreaming;
        }
        self.admit_with_temp(key, class, bytes, temp)
    }

    /// Admit with an explicit temperature (Cold inserts, tests, tooling).
    /// Bypassable classes fall back to streaming bypass when room cannot be
    /// made; other classes get `Reject`.
    pub fn admit_with_temp(
        &self,
        key: &str,
        class: CacheClass,
        bytes: u64,
        temp: Temperature,
    ) -> AdmitDecision {
        if bytes > self.capacity_bytes {
            if class.is_bypassable() {
                self.bypass_count.fetch_add(1, Ordering::Relaxed);
                return AdmitDecision::AdmitStreaming;
            }
            return AdmitDecision::Reject {
                reason: format!(
                    "entry {bytes}B exceeds cache capacity {}B",
                    self.capacity_bytes
                ),
            };
        }
        let mut entries = self.entries.lock().expect("buffer lock");
        if let Some(existing) = entries.get_mut(key) {
            existing.hits += 1;
            existing.last_tick = self.tick.fetch_add(1, Ordering::Relaxed);
            self.hits[existing.class.index()].fetch_add(1, Ordering::Relaxed);
            return AdmitDecision::Admit {
                temp: existing.temp,
            };
        }
        self.misses[class.index()].fetch_add(1, Ordering::Relaxed);
        let free = self.capacity_bytes - self.occupancy.load(Ordering::Relaxed);
        if bytes > free {
            let need = bytes - free;
            let freed = Self::evict_locked(
                &mut entries,
                &self.occupancy,
                &self.evicted_bytes,
                need,
                false,
                self.oltp_min_bytes,
            );
            if freed < need {
                if class.is_bypassable() {
                    self.bypass_count.fetch_add(1, Ordering::Relaxed);
                    return AdmitDecision::AdmitStreaming;
                }
                return AdmitDecision::Reject {
                    reason: format!(
                        "cache full: OLTP reserve {}B protected",
                        self.oltp_min_bytes
                    ),
                };
            }
        }
        let tick = self.tick.fetch_add(1, Ordering::Relaxed);
        entries.insert(
            key.to_string(),
            CacheEntry {
                key: key.to_string(),
                class,
                temp,
                bytes,
                hits: 0,
                last_tick: tick,
            },
        );
        self.occupancy.fetch_add(bytes, Ordering::Relaxed);
        AdmitDecision::Admit { temp }
    }

    /// Record a hit: bumps counters, promotes on reuse (2nd hit Streaming ->
    /// Warm; 4th+ hit Warm -> Hot except Temp; 3rd+ hit Cold -> Warm).
    /// Returns false on miss (misses are counted at [`admit`](Self::admit),
    /// where the class is known).
    pub fn record_hit(&self, key: &str) -> bool {
        let mut entries = self.entries.lock().expect("buffer lock");
        let Some(entry) = entries.get_mut(key) else {
            return false;
        };
        entry.hits += 1;
        entry.last_tick = self.tick.fetch_add(1, Ordering::Relaxed);
        self.hits[entry.class.index()].fetch_add(1, Ordering::Relaxed);
        entry.temp = match (entry.temp, entry.hits, entry.class) {
            (Temperature::Streaming, 2.., _) => Temperature::Warm,
            (Temperature::Warm, 4.., CacheClass::Temp) => Temperature::Warm,
            (Temperature::Warm, 4.., _) => Temperature::Hot,
            (Temperature::Cold, 3.., _) => Temperature::Warm,
            _ => entry.temp,
        };
        true
    }

    /// Explicit reuse signal (e.g. second scan over the same chunk): promotes
    /// exactly one step Streaming/Cold -> Warm, Warm -> Hot (Temp caps at
    /// Warm). Returns false when absent.
    pub fn note_reuse(&self, key: &str) -> bool {
        let mut entries = self.entries.lock().expect("buffer lock");
        let Some(entry) = entries.get_mut(key) else {
            return false;
        };
        entry.hits += 1;
        entry.last_tick = self.tick.fetch_add(1, Ordering::Relaxed);
        self.hits[entry.class.index()].fetch_add(1, Ordering::Relaxed);
        entry.temp = match (entry.temp, entry.class) {
            (Temperature::Streaming, _) | (Temperature::Cold, _) => Temperature::Warm,
            (Temperature::Warm, CacheClass::Temp) => Temperature::Warm,
            (Temperature::Warm, _) => Temperature::Hot,
            (Temperature::Hot, _) => Temperature::Hot,
        };
        true
    }

    /// Evict the pressure-appropriate fraction of occupancy, cold-first
    /// (Cold/Streaming, Warm OLAP, Warm others, Hot non-OLTP, Hot OLTP last).
    /// HOT OLTP never drops below the reservation unless Critical. Returns
    /// bytes actually evicted (entry-granular: may overshoot the target).
    /// The target is [`CachePolicy::target_eviction_bytes`], so advisory
    /// sizing and actual eviction share one formula.
    pub fn evict_under_pressure(&self, pressure: PressureState) -> u64 {
        if pressure == PressureState::Normal {
            return 0;
        }
        let target = self.target_eviction_bytes(pressure);
        if target == 0 {
            return 0;
        }
        let mut entries = self.entries.lock().expect("buffer lock");
        Self::evict_locked(
            &mut entries,
            &self.occupancy,
            &self.evicted_bytes,
            target,
            pressure == PressureState::Critical,
            self.oltp_min_bytes,
        )
    }

    /// Victim rank: lower evicted first.
    fn rank(class: CacheClass, temp: Temperature) -> u8 {
        match temp {
            Temperature::Cold | Temperature::Streaming => 0,
            Temperature::Warm if class == CacheClass::OlapChunk => 1,
            Temperature::Warm => 2,
            Temperature::Hot if class == CacheClass::OltpHot => 4,
            Temperature::Hot => 3,
        }
    }

    /// Remove victims in priority order until `target` bytes are freed or no
    /// evictable victim remains. HOT OLTP victims that would drop the HOT
    /// OLTP total below `oltp_reserve` are skipped unless `allow_oltp_evict`.
    fn evict_locked(
        entries: &mut HashMap<String, CacheEntry>,
        occupancy: &AtomicU64,
        evicted_bytes: &[AtomicU64; 6],
        target: u64,
        allow_oltp_evict: bool,
        oltp_reserve: u64,
    ) -> u64 {
        let mut order: Vec<(u8, u64, String)> = entries
            .values()
            .map(|e| (Self::rank(e.class, e.temp), e.last_tick, e.key.clone()))
            .collect();
        order.sort();
        let mut oltp_hot: u64 = entries
            .values()
            .filter(|e| e.class == CacheClass::OltpHot && e.temp == Temperature::Hot)
            .map(|e| e.bytes)
            .sum();
        let mut freed = 0u64;
        for (_, _, key) in order {
            if freed >= target {
                break;
            }
            let Some(entry) = entries.get(&key) else {
                continue;
            };
            let protected = entry.class == CacheClass::OltpHot && entry.temp == Temperature::Hot;
            if protected && !allow_oltp_evict && oltp_hot.saturating_sub(entry.bytes) < oltp_reserve
            {
                continue;
            }
            let entry = entries.remove(&key).expect("victim present");
            if protected {
                oltp_hot = oltp_hot.saturating_sub(entry.bytes);
            }
            let used = occupancy.load(Ordering::Relaxed);
            occupancy.store(used.saturating_sub(entry.bytes), Ordering::Relaxed);
            evicted_bytes[entry.class.index()].fetch_add(entry.bytes, Ordering::Relaxed);
            freed += entry.bytes;
        }
        freed
    }

    /// Counter snapshot.
    pub fn stats(&self) -> BufferStats {
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let (mut h_total, mut m_total, mut e_total) = (0u64, 0u64, 0u64);
        let (mut h_map, mut m_map, mut e_map) = (HashMap::new(), HashMap::new(), HashMap::new());
        for class in CacheClass::ALL {
            let (h, m, e) = (
                load(&self.hits[class.index()]),
                load(&self.misses[class.index()]),
                load(&self.evicted_bytes[class.index()]),
            );
            h_map.insert(class, h);
            m_map.insert(class, m);
            e_map.insert(class, e);
            h_total += h;
            m_total += m;
            e_total += e;
        }
        BufferStats {
            occupancy_bytes: self.occupancy(),
            capacity_bytes: self.capacity_bytes,
            entry_count: self.len(),
            hits_total: h_total,
            misses_total: m_total,
            evicted_total_bytes: e_total,
            streaming_bypass_count: load(&self.bypass_count),
            hits_by_class: h_map,
            misses_by_class: m_map,
            evicted_bytes_by_class: e_map,
        }
    }
}

impl CachePolicy for UnifiedBufferManager {
    fn name(&self) -> &str {
        "unified-workload-aware"
    }
    fn target_eviction_bytes(&self, pressure: PressureState) -> u64 {
        self.occupancy() * eviction_pct(pressure) / 100
    }
}

/// Capacity of the process-wide OLAP scan cache (see
/// [`global_buffer_manager`]).
///
/// Standard-tier pin (see `global_buffer_sizing`): the live default reads
/// the tiered sizing, which returns exactly these values on Standard/Large
/// hosts and a smaller cache on Edge hosts. `PLOMID_BUFFER_CAPACITY_BYTES`
/// env overrides the capacity when set (reserve via
/// `PLOMID_BUFFER_OLTP_RESERVE_BYTES`); explicit configuration always wins
/// over the derived default.
pub const GLOBAL_BUFFER_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;

/// OLTP-fenced reserve inside the process-wide cache.
///
/// Standard-tier pin (25% of the Standard capacity); the tiered default is
/// 16 MiB on Edge. Env override: `PLOMID_BUFFER_OLTP_RESERVE_BYTES`.
pub const GLOBAL_BUFFER_OLTP_RESERVE_BYTES: u64 = 64 * 1024 * 1024;

static GLOBAL_BUFFER_MANAGER: OnceLock<UnifiedBufferManager> = OnceLock::new();

/// `(capacity, OLTP reserve)` for a resolved
/// [`RuntimeProfile`](plomid_hw::RuntimeProfile). Pure over its input so
/// tests mock tiers without touching the host.
///
/// The profile's `buffer_capacity_bytes()` passes through capped at the
/// historical [`GLOBAL_BUFFER_CAPACITY_BYTES`] (256 MiB), so Edge takes its
/// 64 MiB cap, Laptop its 256 MiB, and Server cannot exceed the memory
/// crate's 512 MiB OLAP budget the current default was sized against. The
/// reserve is 1/4 of the capacity (the historical 25% OLTP-fence ratio);
/// `UnifiedBufferManager::new` additionally clamps the reserve to the
/// capacity, so env-provided pairs stay safe.
#[must_use]
pub fn buffer_sizing_for_profile(profile: &plomid_hw::RuntimeProfile) -> (u64, u64) {
    let capacity = profile
        .buffer_capacity_bytes()
        .min(GLOBAL_BUFFER_CAPACITY_BYTES);
    (capacity, capacity / 4)
}

/// Resolved profile for the global sizing, detected once per process.
/// An unparsable `PLOMID_PROFILE` fails safe to Laptop with one stderr
/// warning, mirroring the executor's detection.
fn cached_runtime_profile() -> &'static plomid_hw::RuntimeProfile {
    static CACHED: std::sync::OnceLock<plomid_hw::RuntimeProfile> = std::sync::OnceLock::new();
    CACHED.get_or_init(|| {
        plomid_hw::RuntimeProfile::resolve(None).unwrap_or_else(|reason| {
            eprintln!("warning: {reason}; falling back to the laptop tier");
            plomid_hw::RuntimeProfile::with_tier(
                plomid_hw::RuntimeTier::Laptop,
                plomid_hw::HardwareProfile::detect(),
                plomid_hw::ProfileSource::Explicit,
            )
        })
    })
}

/// Tiered `(capacity, OLTP reserve)` for the process-wide cache:
/// `PLOMID_BUFFER_CAPACITY_BYTES` / `PLOMID_BUFFER_OLTP_RESERVE_BYTES` env
/// (each parsed independently; unparsable-or-zero values are ignored, so
/// explicit configuration always wins over the derived default) over the
/// profile default ([`buffer_sizing_for_profile`]).
#[must_use]
pub fn global_buffer_sizing() -> (u64, u64) {
    fn env_bytes(name: &str) -> Option<u64> {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|&n| n >= 1)
    }
    let (tier_capacity, _) = buffer_sizing_for_profile(cached_runtime_profile());
    let capacity = env_bytes("PLOMID_BUFFER_CAPACITY_BYTES").unwrap_or(tier_capacity);
    // The default reserve is 1/4 of the effective capacity (which is exactly
    // the tier reserve when capacity is not overridden); an explicit reserve
    // clamps to the capacity instead of inverting the fence.
    let reserve = env_bytes("PLOMID_BUFFER_OLTP_RESERVE_BYTES").unwrap_or(capacity / 4);
    (capacity, reserve.min(capacity))
}

/// Returns the process-wide buffer manager consulted by OLAP segment reads.
///
/// Sizing is consistent with the existing budgets: the 256 MiB capacity fits
/// inside [`crate::HtapConfig`]'s OLAP pool order of magnitude and inside the
/// memory crate's default 512 MiB OLAP budget, while the 64 MiB (25%) OLTP
/// fence mirrors [`crate::HtapConfig`]'s ~20% OLTP reservation ratio. The
/// manager is accounting-only (entries track bytes, not payloads), so the cap
/// bounds ledger entries, not resident data: admission past the cap degrades
/// to streaming bypass instead of failing.
///
/// The columnar OLAP scan path (`read_current_generation_rows`) admits each
/// segment it reads as [`CacheClass::OlapChunk`] at
/// [`Temperature::Streaming`] and calls [`UnifiedBufferManager::record_hit`]
/// on reuse, so genuinely re-scanned segments promote Streaming -> Warm ->
/// Hot while one-shot scans stay first-to-evict.
pub fn global_buffer_manager() -> &'static UnifiedBufferManager {
    GLOBAL_BUFFER_MANAGER.get_or_init(|| {
        let (capacity, reserve) = global_buffer_sizing();
        UnifiedBufferManager::new(capacity, reserve)
    })
}
