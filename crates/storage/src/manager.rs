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
//! Segment-oriented storage management built on the existing durable B+Tree.
//!
//! Each segment is an independent page file (an on-disk B+Tree). The manager
//! routes reads to all segments and directs new keys to the active segment,
//! rotating it once its configured on-disk size is reached.
//!
//! # Physical layout
//!
//! Segments live inside the physical device tree, never under the logical
//! object tree. A segment belongs to exactly one device, and devices are
//! discovered from the layout. No manifest or volume registry exists: the
//! filesystem listing of each device's `packs/` directory is the durable
//! segment inventory, combined with the device records the registry validates.
//!
//! ```text
//! PLOMID_DATA/
//!   devices/
//!     D-0000000000000001/
//!       packs/
//!         SEG-00000000000000000001.dat   <- segment 1 page file
//!     D-0000000000000002/
//!       packs/
//!         SEG-00000000000000000003.dat   <- segment 3 page file
//! ```
//!
//! Logical table/generation metadata references segments by segment ID. The
//! manager never reads logical metadata: it only owns the physical segment
//! page files and their placement on devices.
//!
//! # Device placement
//!
//! The manager never assumes a device. Every segment is placed by the device
//! allocator, which selects among the devices the registry reports. A segment
//! commits its maximum size on the selected device when it is created — the
//! device reserves that capacity as one contiguous extent — so a device can
//! never be oversubscribed and a full device simply makes another eligible
//! device the choice. The capacity of the database is therefore the sum of its
//! devices rather than the capacity of any single one, and one logical table
//! can span several devices through its segments.

use crate::device::{
    default_device_capacity, register_device, round_up_to_extent, DeviceAllocator, DeviceRegistry,
};
use crate::durable;
use crate::layout::{reject_legacy_layout, DatabaseLayout, STAGING_SUFFIX};
use placement::segment_from_file_name;
use plomid_core::{CatalogVersion, ErrorKind, GenerationId, PlomidError, Result, SegmentId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

pub(crate) mod placement;
mod segment;

pub use placement::storage_generation;
pub(crate) use placement::{durable_segment_ids, segment_id_exhausted};
pub use segment::{Segment, SegmentState};

/// Segment-oriented physical storage manager.
pub struct StorageManager {
    layout: DatabaseLayout,
    pool_capacity: usize,
    segment_size_bytes: u64,
    segments: Vec<Segment>,
    next_segment_id: u64,
    allocator: DeviceAllocator,
    /// In-memory ownership map for keys already seen by this manager.  The
    /// durable B+Trees remain authoritative; this is only a routing cache so
    /// mutations do not probe every sealed generation before touching a key.
    key_segments: Option<HashMap<Vec<u8>, usize>>,
    /// Sealed-segment probes attempted (exact B-tree descents).
    filter_probes: u64,
    /// Sealed-segment probes skipped by the negative-membership filter.
    filter_skips: u64,
}

impl std::fmt::Debug for StorageManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageManager")
            .field("segment_count", &self.segments.len())
            .field("next_segment_id", &self.next_segment_id)
            .finish()
    }
}

/// Result of one drained-segment reclamation pass.
///
/// Only sealed segments proven empty by an exact key-space descent are ever
/// removed; non-empty segments, referenced segments, and the active segment
/// are never touched. This mirrors [`crate::GcOutcome`] at segment
/// granularity: deletion is never the authority (the filesystem listing plus
/// the published catalog state are), so an interrupted pass only leaves
/// orphan files the next pass reclaims.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SegmentReclaimOutcome {
    /// Sealed segment IDs reclaimed by this pass, ascending.
    pub reclaimed: Vec<u64>,
    /// Durable bytes removed with the reclaimed segment files.
    pub reclaimed_bytes: u64,
    /// Proven-empty sealed segments retained because they are still referenced
    /// by a live generation, lease pin, or checkpoint, ascending.
    pub retained_referenced: Vec<u64>,
}

impl StorageManager {
    /// Creates a storage database at `root`, or reopens the one already there.
    ///
    /// Database creation is the only path that registers a device implicitly: a
    /// database cannot hold anything without physical capacity. The device it
    /// creates for itself takes the lowest free device identity, so no specific
    /// device number is special, and further devices are registered explicitly.
    ///
    /// Creation is idempotent: a root that already owns durable segments is
    /// rediscovered and reopened instead of accumulating a second segment 1, so
    /// running creation against an existing database can never truncate the
    /// pages of a segment that already holds data.
    pub fn create(root: &Path, pool_capacity: usize, segment_size_bytes: u64) -> Result<Self> {
        let layout = DatabaseLayout::new(root);
        reject_legacy_layout(&layout)?;
        layout.initialize()?;

        let registry = DeviceRegistry::discover(&layout)?;
        if registry.is_empty() {
            let capacity = default_device_capacity()?;
            register_device(&layout, registry.next_free_id(), capacity)?;
        }

        let segments = discover_segments(&layout, pool_capacity)?;
        let mut manager = Self::restore(layout, pool_capacity, segment_size_bytes, segments);
        if manager.segments.is_empty() {
            manager.allocate_segment()?;
        }
        Ok(manager)
    }

    /// Opens an existing storage database and rediscovers its segments.
    pub fn open(root: &Path, pool_capacity: usize, segment_size_bytes: u64) -> Result<Self> {
        let layout = DatabaseLayout::new(root);
        layout.validate()?;

        let registry = DeviceRegistry::discover(&layout)?;
        if registry.is_empty() {
            return Err(PlomidError::with_detail(
                ErrorKind::NotFound,
                "no physical devices are registered",
                format!("root={}", root.display()),
            ));
        }

        let segments = discover_segments(&layout, pool_capacity)?;
        let mut manager = Self::restore(layout, pool_capacity, segment_size_bytes, segments);
        if manager.segments.is_empty() {
            manager.allocate_segment()?;
        }
        Ok(manager)
    }

    /// Assembles a manager around the segments a root already owns.
    ///
    /// The newest segment continues to receive keys; older segments stay sealed,
    /// which is the state a clean shutdown leaves behind.
    fn restore(
        layout: DatabaseLayout,
        pool_capacity: usize,
        segment_size_bytes: u64,
        mut segments: Vec<Segment>,
    ) -> Self {
        segments.sort_unstable_by_key(|segment| segment.id);
        if let Some(active) = segments.last_mut() {
            active.state = SegmentState::Active;
        }
        let next_segment_id = segments
            .iter()
            .map(|segment| segment.id)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);
        Self {
            layout,
            pool_capacity,
            segment_size_bytes: segment_size_bytes.max(1),
            segments,
            next_segment_id,
            allocator: DeviceAllocator::new(),
            // Start empty.  Existing keys are routed through the authoritative
            // fallback on first mutation after restart; eagerly scanning every
            // segment here made the first bulk INSERT pay for a full database
            // read before doing any writes.
            key_segments: Some(HashMap::new()),
            filter_probes: 0,
            filter_skips: 0,
        }
    }

    /// Cumulative sealed-segment probe statistics `(attempted, skipped)`.
    ///
    /// `skipped` counts probes avoided by the negative-membership filter;
    /// `attempted` counts exact descents performed. Used by benchmarks and
    /// operators to verify the filter engages; not a correctness signal.
    #[must_use]
    pub fn filter_stats(&self) -> (u64, u64) {
        (self.filter_probes, self.filter_skips)
    }

    /// Whether sealed segment `index` must be probed exactly for `key`.
    ///
    /// Builds the segment's filter lazily on first need (one full key scan
    /// per segment lifetime) and records the decision in [`Self::filter_stats`].
    /// A `false` answer is definitive (the key is absent); `true` requires
    /// exact evaluation. Must only be called for sealed segments: the active
    /// segment has no filter and always probes.
    fn probe_needed(&mut self, index: usize, key: &[u8]) -> bool {
        let segment = &mut self.segments[index];
        debug_assert_ne!(segment.state, SegmentState::Active);
        // Exact key-range prune BEFORE the filter: a sealed segment never
        // gains keys, so out-of-range stays out-of-range forever (no false
        // negatives, even as replaces/deletes shrink the set — stored bounds
        // can only go stale-wide). This skips both the XorFilter hashing and
        // the lazy full-segment filter build for keys no sealed segment
        // could hold (e.g. fresh sequential ids during bulk load).
        segment.ensure_bounds();
        // A sealed segment proven empty holds no key now or ever (sealed
        // segments never gain keys), so the skip is definitive — no filter
        // hashing and no exact descent can change the answer. This also
        // covers segments drained after sealing, whose stale sidecar filter
        // could otherwise force an exact descent per probe.
        if segment.bounds_known_empty() {
            self.filter_skips += 1;
            return false;
        }
        let dominated = match segment.key_bounds() {
            Some((min, max)) if key < min || key > max => true,
            _ => {
                if !segment.has_filter() {
                    segment.ensure_filter();
                }
                !segment.may_contain(key)
            }
        };
        if dominated {
            self.filter_skips += 1;
            false
        } else {
            self.filter_probes += 1;
            true
        }
    }

    /// Root directory of this storage database.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.layout.root()
    }

    /// Durable storage-image generation: the highest committed segment ID.
    #[must_use]
    pub fn storage_generation(&self) -> GenerationId {
        GenerationId::new(
            self.segments
                .iter()
                .map(|segment| segment.id)
                .max()
                .unwrap_or(0),
        )
    }

    /// Number of segments the manager currently holds open.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Every segment, in creation order.
    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.segments.iter()
    }

    /// Reads `key`, searching the newest segment first.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for index in (0..self.segments.len()).rev() {
            // The active segment has no filter (it mutates); sealed segments
            // consult theirs, building it lazily on first need.
            if index + 1 < self.segments.len() && !self.probe_needed(index, key) {
                continue;
            }
            if let Some(value) = self.segments[index].get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    /// Inserts `key` and makes it durable.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_buffered(key, value)?;
        self.sync()
    }

    /// Inserts `key` without forcing a flush; the caller owns durability.
    pub fn insert_buffered(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.apply_batch(std::slice::from_ref(&(key.to_vec(), Some(value.to_vec()))))
    }

    /// Applies a transaction's storage mutations as one logical batch.
    ///
    /// The old path searched every segment for every put, then descended the
    /// active tree again for new keys.  This path builds a lazy ownership map,
    /// routes known keys directly to their authoritative segment, and handles
    /// unknown keys with one replacement traversal before inserting.  The
    /// map is an optimization only: a cache miss still performs the complete
    /// durable search, so replacement semantics remain unchanged.
    pub fn apply_batch(&mut self, operations: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
        let perf = tracing::enabled!(tracing::Level::DEBUG, target: "plomid::perf");
        let total_started = std::time::Instant::now();
        let mut routed_us = 0_u64;
        let mut probe_us = 0_u64;
        let mut insert_us = 0_u64;
        let mut probes = 0_u64;
        let mut inserts = 0_u64;
        let mut rotates = 0_u64;
        let mut filter_skipped = 0_u64;
        self.ensure_key_segments()?;
        for (key, value) in operations {
            if let Some(&index) = self.key_segments.as_ref().and_then(|m| m.get(key)) {
                if let Some(segment) = self.segments.get_mut(index) {
                    let routed_started = std::time::Instant::now();
                    match value {
                        Some(value) => {
                            if segment.replace_if_present(key, value)? {
                                routed_us += routed_started.elapsed().as_micros() as u64;
                                continue;
                            }
                        }
                        None => {
                            if segment.delete(key)? {
                                routed_us += routed_started.elapsed().as_micros() as u64;
                                if let Some(map) = self.key_segments.as_mut() {
                                    map.remove(key);
                                }
                                continue;
                            }
                        }
                    }
                    routed_us += routed_started.elapsed().as_micros() as u64;
                }
            }

            // A cache miss can be caused by a key written before the cache was
            // initialized, or by a stale location after recovery. Probe the
            // SEALED segments only, newest first, so the copy the reads would
            // resolve (`get` searches newest segment first) is the one
            // replaced.
            //
            // The active segment is deliberately excluded from the probe: its
            // write path (`BTree::insert`, reached through `Segment::insert`)
            // is already an upsert, so probing it here and then descending it
            // again to insert would double the tree descents for every new key
            // in a batch. The probe's only job is to find a copy in a segment
            // that must not be shadowed; the active segment is handled by the
            // single upsert descent below.
            let probe_started = std::time::Instant::now();
            let active = self.segments.len().checked_sub(1);
            let mut found = None;
            if let Some(active) = active {
                for index in (0..active).rev() {
                    // Negative prefilter: a sealed segment that definitively
                    // lacks the key is skipped without a B-tree descent. A
                    // `false` answer is exact (the filter is a superset built
                    // at seal time over an only-shrinking key set); `true`
                    // falls through to exact evaluation below.
                    if !self.probe_needed(index, key) {
                        filter_skipped += 1;
                        continue;
                    }
                    probes += 1;
                    let segment = &mut self.segments[index];
                    match value {
                        Some(value) if segment.replace_if_present(key, value)? => {
                            found = Some(index);
                            break;
                        }
                        None if segment.delete(key)? => {
                            found = Some(index);
                            break;
                        }
                        _ => {}
                    }
                }
            }
            probe_us += probe_started.elapsed().as_micros() as u64;
            if let Some(index) = found {
                if let Some(map) = self.key_segments.as_mut() {
                    if value.is_some() {
                        map.insert(key.clone(), index);
                    } else {
                        map.remove(key);
                    }
                }
                continue;
            }
            let insert_started = std::time::Instant::now();
            if value.is_none() {
                // A deletion the sealed probe did not satisfy may still match
                // the active segment.
                if let Some(index) = active {
                    if self.segments[index].delete(key)? {
                        insert_us += insert_started.elapsed().as_micros() as u64;
                        if let Some(map) = self.key_segments.as_mut() {
                            map.remove(key);
                        }
                    }
                }
                continue;
            }
            if self
                .segments
                .last_mut()
                .is_none_or(|segment| segment.is_full(self.segment_size_bytes))
            {
                self.rotate()?;
                rotates += 1;
            }
            let index = self.segments.len().checked_sub(1).ok_or_else(|| {
                PlomidError::new(ErrorKind::Internal, "storage manager has no active segment")
            })?;
            // One descent: `BTree::insert` replaces an existing entry in place
            // and inserts a new one otherwise, covering the active segment
            // without a preceding probe.
            self.segments[index].insert(key, value.as_deref().unwrap_or_default())?;
            insert_us += insert_started.elapsed().as_micros() as u64;
            inserts += 1;
            if let Some(map) = self.key_segments.as_mut() {
                map.insert(key.clone(), index);
            }
        }
        if perf {
            tracing::debug!(
                target: "plomid::perf",
                event = "storage_apply",
                ops = operations.len(),
                segments = self.segments.len(),
                routed_us,
                probe_us,
                insert_us,
                probes,
                filter_skipped,
                inserts,
                rotates,
                total_us = total_started.elapsed().as_micros() as u64,
            );
        }
        Ok(())
    }

    /// Deletes `key` and makes the deletion durable.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let removed = self.delete_buffered(key)?;
        self.sync()?;
        Ok(removed)
    }

    /// Deletes `key` without forcing a flush.
    pub fn delete_buffered(&mut self, key: &[u8]) -> Result<bool> {
        self.ensure_key_segments()?;
        if let Some(index) = self.key_segments.as_ref().and_then(|m| m.get(key)).copied() {
            if self.segments[index].delete(key)? {
                self.key_segments
                    .as_mut()
                    .expect("key map initialized")
                    .remove(key);
                return Ok(true);
            }
        }
        for index in (0..self.segments.len()).rev() {
            // Sealed segments consult the negative filter first (the active
            // segment always probes exactly).
            if index + 1 < self.segments.len() && !self.probe_needed(index, key) {
                continue;
            }
            if self.segments[index].delete(key)? {
                self.key_segments
                    .as_mut()
                    .expect("key map initialized")
                    .remove(key);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn ensure_key_segments(&mut self) -> Result<()> {
        // The routing map is intentionally lazy.  It is populated by every
        // mutation performed during this process; a key absent from it still
        // takes the complete durable lookup path in apply_batch().
        if self.key_segments.is_none() {
            self.key_segments = Some(HashMap::new());
        }
        Ok(())
    }

    /// Returns the ordered union of `[start, end)` across every segment.
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut values = Vec::new();
        for segment in &mut self.segments {
            values.extend(segment.range(start, end)?);
        }
        values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        values.dedup_by(|left, right| left.0 == right.0);
        Ok(values)
    }

    /// Returns the ordered union of `[start, end)` with per-segment scans
    /// running on scoped threads.
    ///
    /// The result is exactly what [`Self::range`] returns: each segment scan
    /// is independent (disjoint files, disjoint buffer pools), and the
    /// single-threaded caller merges with the same sort + dedup, so the
    /// thread schedule cannot influence the output. Used only where a full
    /// scan dominates wall time (mount-time version bootstrap); query paths
    /// keep the sequential [`Self::range`] so one scan never oversubscribes
    /// a server already serving concurrent connections.
    pub fn range_parallel(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.segments.len() < 4 {
            return self.range(start, end);
        }
        let parallelism = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(4)
            .max(1);
        let chunk_size = self.segments.len().div_ceil(parallelism).max(1);
        let mut values = Vec::new();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for chunk in self.segments.chunks_mut(chunk_size) {
                handles.push(scope.spawn(move || {
                    let mut chunk_values = Vec::new();
                    for segment in chunk {
                        chunk_values.extend(segment.range(start, end)?);
                    }
                    Ok::<_, PlomidError>(chunk_values)
                }));
            }
            for handle in handles {
                match handle.join() {
                    Ok(Ok(mut chunk_values)) => values.append(&mut chunk_values),
                    // Scans perform no mutation, so a failed scan leaves the
                    // durable state untouched and retrying observes the same
                    // image; report the first failure in chunk order.
                    Ok(Err(error)) => {
                        return Err(error);
                    }
                    Err(_) => {
                        return Err(PlomidError::new(
                            ErrorKind::Internal,
                            "parallel segment scan did not complete",
                        ));
                    }
                }
            }
            Ok(())
        })?;
        values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        values.dedup_by(|left, right| left.0 == right.0);
        Ok(values)
    }

    /// Makes every written page of every segment durable.
    pub fn sync(&mut self) -> Result<()> {
        for segment in &mut self.segments {
            segment.sync()?;
        }
        Ok(())
    }

    /// Reclaims sealed segments that are proven empty and provably unreferenced.
    ///
    /// A segment is reclaimed only when ALL of these hold:
    ///
    /// * it is sealed (the active segment, which always keeps the last slot,
    ///   is never a candidate even if it is currently empty);
    /// * an exact key-space descent proves it holds no keys
    ///   ([`Segment::bounds_known_empty`] — sealed segments never gain keys,
    ///   so emptiness is final, not heuristic);
    /// * its ID appears in no live reference: not in the published catalog
    ///   state, not in any reachable generation's physical references, not in
    ///   any checkpoint-pinned catalog state, and not in the caller-supplied
    ///   `pinned` set (in-memory reader leases the durable image cannot see).
    ///
    /// Crash safety follows the crate's single durability policy: the
    /// dereference is durable BEFORE the delete (only IDs absent from every
    /// durable reference are removed, and the published pointer plus catalog
    /// state are the authority). Each removal drops the open segment first so
    /// its file handles close before the unlink, then unlinks the segment
    /// file and its filter sidecar (absence counts as reclaimed, like
    /// generation GC) and fsyncs the packs directory, so an interrupted pass
    /// can only leave orphan files the next pass reclaims — never data loss.
    ///
    /// Callers: post-checkpoint maintenance and the open-time orphan sweep,
    /// both fail-open. This runs inline on the caller's thread: no new
    /// threads, no background work.
    pub fn reclaim_drained_segments(
        &mut self,
        pinned: &BTreeSet<u64>,
    ) -> Result<SegmentReclaimOutcome> {
        let mut outcome = SegmentReclaimOutcome::default();
        // Fast path: zero or one segment means nothing is sealed (restore
        // keeps exactly the newest segment active), so no metadata scan is
        // needed. This keeps the recurring maintenance call O(1) on small
        // databases.
        let Some(active) = self.segments.len().checked_sub(1) else {
            return Ok(outcome);
        };
        if active == 0 {
            return Ok(outcome);
        }
        let referenced = self.referenced_segment_ids(pinned)?;
        // Phase 1 (prove): exact emptiness descents only; no mutation, so an
        // error here changes nothing.
        let mut victims: Vec<(usize, u64, u64)> = Vec::new();
        for index in 0..active {
            let segment = &mut self.segments[index];
            if segment.state != SegmentState::Sealed {
                continue;
            }
            segment.ensure_bounds();
            if !segment.bounds_known_empty() {
                continue;
            }
            if referenced.contains(&segment.id) {
                outcome.retained_referenced.push(segment.id);
                continue;
            }
            let bytes = std::fs::metadata(&segment.path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            victims.push((index, segment.id, bytes));
        }
        if victims.is_empty() {
            return Ok(outcome);
        }
        // Old-to-new index map for the routing cache, built before the
        // removal shifts positions. Entries that pointed at a removed segment
        // (or past the end, which must not happen) are dropped: the next
        // mutation of that key takes the complete durable lookup path, which
        // is the correct fallback for a segment proven to hold nothing.
        let removed: BTreeSet<usize> = victims.iter().map(|(index, _, _)| *index).collect();
        let mut remap: Vec<Option<usize>> = vec![None; self.segments.len()];
        let mut next = 0_usize;
        for (index, slot) in remap.iter_mut().enumerate() {
            if !removed.contains(&index) {
                *slot = Some(next);
                next += 1;
            }
        }
        // Phase 2 (detach): drop the open segments so their file handles
        // close before the unlink; dropping a proven-empty segment discards
        // nothing any reader could observe.
        let mut detached: Vec<(PathBuf, u64, u64)> = Vec::with_capacity(victims.len());
        for (index, id, bytes) in victims.into_iter().rev() {
            let segment = self.segments.remove(index);
            detached.push((segment.path.clone(), id, bytes));
            drop(segment);
        }
        if let Some(map) = self.key_segments.as_mut() {
            let mut stale = Vec::new();
            for (key, slot) in map.iter_mut() {
                match remap.get(*slot).copied().flatten() {
                    Some(updated) => *slot = updated,
                    None => stale.push(key.clone()),
                }
            }
            for key in stale {
                map.remove(&key);
            }
        }
        // Phase 3 (unlink): the point of no return per file. A failure here
        // leaves at most orphan files: the in-memory state is already
        // consistent (empty segments are invisible to reads), and reopening
        // rediscovers orphans as sealed-empty candidates for the next pass.
        // `sync_dir` after each packs directory makes the removal itself
        // durable, matching generation reclamation.
        let mut synced: Vec<PathBuf> = Vec::new();
        for (path, id, bytes) in detached {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(PlomidError::from(error)),
            }
            // Best-effort: the sidecar is a pure probe cache. Removing it
            // keeps no stale filter beside a recycled segment ID; leaving it
            // would only cost a rebuild on load, never a wrong answer.
            let _ = std::fs::remove_file(path.with_extension("xor"));
            if let Some(parent) = path.parent() {
                if !synced.iter().any(|dir| dir == parent) {
                    durable::sync_dir(parent)?;
                    synced.push(parent.to_path_buf());
                }
            }
            outcome.reclaimed.push(id);
            outcome.reclaimed_bytes = outcome.reclaimed_bytes.saturating_add(bytes);
        }
        outcome.reclaimed.sort_unstable();
        Ok(outcome)
    }

    /// Collects every segment ID still referenced by durable metadata.
    ///
    /// The union of the published catalog state's physical references, every
    /// reachable generation's physical references (what
    /// [`crate::GenerationManager::validate_physical`] enforces), and every
    /// checkpoint-pinned catalog state's references, plus the caller-supplied
    /// `pinned` set (live leases and in-flight publications the durable image
    /// cannot see).
    ///
    /// Roots with no published state (fresh roots and the transaction
    /// engine's key-value image, whose catalog lives outside generation
    /// metadata) yield exactly `pinned`: nothing is durably referenced. Any
    /// metadata failure aborts the pass (fail-closed): when reachability
    /// cannot be proven, every segment is retained.
    fn referenced_segment_ids(&self, pinned: &BTreeSet<u64>) -> Result<BTreeSet<u64>> {
        let mut referenced: BTreeSet<u64> = pinned.iter().copied().collect();
        let root = self.layout.root();
        let pointer = match crate::generation::load_publication_pointer(root) {
            Ok(pointer) => pointer,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(referenced),
            Err(error) => return Err(error),
        };
        let current =
            crate::catalog::load_catalog_for_generation(root, pointer.catalog_generation)?
                .ok_or_else(|| {
                    PlomidError::with_detail(
                        ErrorKind::NotFound,
                        "published catalog state is missing",
                        format!("catalog_generation={}", pointer.catalog_generation.get()),
                    )
                })?;
        // Reachable generations mirror generation GC exactly: the published
        // state plus every checkpoint-pinned catalog state. In-memory reader
        // leases are the caller's `pinned` responsibility (see above).
        let mut generations: BTreeSet<GenerationId> =
            current.generation_ids().into_iter().collect();
        for path in crate::checkpoint::discover(root)? {
            let checkpoint = match crate::checkpoint::load_checkpoint(&path) {
                Ok(checkpoint) => checkpoint,
                // Mirrors generation GC: an unreadable checkpoint cannot pin
                // anything; the published state still governs.
                Err(_) => continue,
            };
            if let Some(state) = published_catalog_for_version(root, checkpoint.catalog_generation)?
            {
                generations.extend(state.generation_ids());
            }
        }
        // A generation that cannot be loaded aborts the pass: its references
        // are unknown, so nothing is provably unreferenced.
        for generation in generations {
            let metadata = crate::generation::load_generation(root, generation)?;
            for reference in &metadata.references {
                referenced.insert(reference.structure.segment_id.get());
            }
        }
        // Belt and braces: the current catalog's own reference copies must
        // agree with generation metadata by validation, but unioning them is
        // the safe direction if they ever disagree.
        for record in current.records() {
            for reference in &record.references {
                referenced.insert(reference.structure.segment_id.get());
            }
        }
        Ok(referenced)
    }

    /// Seals the active segment and opens a new one for subsequent keys.
    fn rotate(&mut self) -> Result<()> {
        if let Some(active) = self.segments.last_mut() {
            active.seal()?;
        }
        self.allocate_segment()
    }

    /// Places a new segment on a device and opens it.
    ///
    /// The device is chosen by the allocator, never by the manager: the manager
    /// only performs the placement the allocator decided. Capacity is committed
    /// before the segment file is created, so a failed allocation leaves the
    /// device set unchanged rather than half-committed.
    fn allocate_segment(&mut self) -> Result<()> {
        let id = self.next_segment_id;
        self.next_segment_id = self
            .next_segment_id
            .checked_add(1)
            .ok_or_else(segment_id_exhausted)?;

        // A segment may grow up to its configured size, so that is the capacity
        // the device must be able to commit for it.
        let request = round_up_to_extent(self.segment_size_bytes)?;
        // Devices are rediscovered for every segment: a device registered after
        // the manager was opened becomes eligible without reopening the
        // database, and this happens once per segment, not once per write.
        let registry = DeviceRegistry::discover(&self.layout)?;
        if registry.is_empty() {
            return Err(PlomidError::new(
                ErrorKind::NotFound,
                "no physical device is available for a new segment",
            ));
        }
        let target = self.allocator.select_device(&registry, request)?;
        let device_id = target.device_id;
        // The reservation is durable in the device's allocator: the extent
        // belongs to this segment until segment reclamation releases it.
        let _reservation = target.device.allocate(request)?;

        let path = placement::segment_path(&self.layout, device_id, SegmentId::new(id));
        let segment =
            Segment::create(path.clone(), id, device_id, self.pool_capacity).map_err(|error| {
                // A segment that cannot be created on a device is a placement
                // failure, not an anonymous I/O fault: report the device and the
                // pack path so the operator can see which physical capacity failed.
                PlomidError::with_detail(
                    error.kind(),
                    "new storage segment could not be created on its device",
                    format!(
                        "segment_id={id} device_id={} path={} error={}",
                        device_id.get(),
                        path.display(),
                        error.message()
                    ),
                )
            })?;
        self.segments.push(segment);
        Ok(())
    }
}

/// Reports whether a storage root already owns durable physical state.
///
/// A root with no registered device and no durable segment has never stored
/// anything, so a database may be created there. Any other root is an existing
/// database: it must be opened, never rebuilt over, because creating segments
/// again could truncate the pages of a segment that already holds data. This is
/// the explicit test a first-boot path uses instead of a silent fallback.
pub fn has_durable_state(root: &Path) -> Result<bool> {
    let layout = DatabaseLayout::new(root);
    if !layout.devices_dir().is_dir() {
        return Ok(false);
    }
    if !DeviceRegistry::discover(&layout)?.is_empty() {
        return Ok(true);
    }
    Ok(!durable_segment_ids(root)?.is_empty())
}

/// Discovers the durable segments of a database root, by device.
///
/// The pack listing of each registered device is the durable segment inventory;
/// a segment ID that appears on more than one device is corruption, because a
/// segment has exactly one physical owner.
///
/// Opens are independent (one file, one buffer pool per segment), so beyond a
/// small threshold they run on scoped threads: a restart over dozens of
/// segments paid one file-open + metadata-read latency per segment in series.
/// Inventory collection and duplicate detection stay single-threaded and run
/// first, so a corrupt inventory fails fast without opening anything.
fn discover_segments(layout: &DatabaseLayout, pool_capacity: usize) -> Result<Vec<Segment>> {
    let started = std::time::Instant::now();
    // Phase 1: inventory. Cheap directory reads plus file-name parsing only;
    // no segment file is touched here.
    let mut inventory: Vec<(u64, plomid_core::DeviceId, std::path::PathBuf)> = Vec::new();
    for device_id in layout.discover_device_ids()? {
        let packs_dir = layout.device_packs_dir(device_id);
        if !packs_dir.exists() {
            continue;
        }
        for entry in std::fs::read_dir(&packs_dir).map_err(PlomidError::from)? {
            let entry = entry.map_err(PlomidError::from)?;
            if !entry.file_type().map_err(PlomidError::from)?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            // A staging artifact is an unpublished segment and is never part of
            // the durable inventory.
            if name.ends_with(STAGING_SUFFIX) {
                continue;
            }
            let Some(segment_id) = segment_from_file_name(&name) else {
                continue;
            };
            inventory.push((segment_id.get(), device_id, entry.path()));
        }
    }
    inventory.sort_unstable_by_key(|(segment_id, _, _)| *segment_id);
    // Duplicate detection before any open: the same fail-closed signal the
    // interleaved walk produced, without depending on directory order.
    for window in inventory.windows(2) {
        if window[0].0 == window[1].0 {
            return Err(PlomidError::with_detail(
                ErrorKind::Corruption,
                "duplicate segment ID across devices",
                format!(
                    "segment_id={} first={} duplicate={}",
                    window[0].0,
                    window[0].2.display(),
                    window[1].2.display()
                ),
            ));
        }
    }
    // Phase 2: open. Each entry owns a distinct file and builds a private
    // buffer pool, so opens share nothing and run concurrently.
    let opened: Vec<Segment> = if inventory.len() > 4 {
        open_segments_parallel(&inventory, pool_capacity)?
    } else {
        inventory
            .iter()
            .map(|(segment_id, device_id, path)| {
                open_one_segment(*segment_id, *device_id, path.clone(), pool_capacity)
            })
            .collect::<Result<Vec<_>>>()?
    };
    let mut all_segments: BTreeMap<u64, Segment> = BTreeMap::new();
    for segment in opened {
        all_segments.insert(segment.id, segment);
    }
    // One summary line replaces the per-segment open log (now DEBUG in
    // `BTree::open`): the inventory that used to cost one INFO line per
    // segment is a single line at any segment count.
    tracing::info!(
        target: "storage",
        event = "segments_opened",
        segments = all_segments.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
    );
    Ok(all_segments.into_values().collect())
}

/// Opens one inventoried segment, attributing failures to its device and path.
///
/// A durable segment exists in the device listing but cannot be reopened:
/// report which device and pack file is unreadable instead of surfacing an
/// anonymous I/O error.
fn open_one_segment(
    segment_id: u64,
    device_id: plomid_core::DeviceId,
    path: std::path::PathBuf,
    pool_capacity: usize,
) -> Result<Segment> {
    Segment::open(path.clone(), segment_id, device_id, pool_capacity).map_err(|error| {
        PlomidError::with_detail(
            error.kind(),
            "durable storage segment could not be reopened",
            format!(
                "segment_id={segment_id} device_id={} path={} error={}",
                device_id.get(),
                path.display(),
                error.message()
            ),
        )
    })
}

/// Opens inventoried segments across scoped threads.
///
/// Chunked so one thread is not spawned per segment on large databases; every
/// chunk borrows disjoint inventory entries and returns owned segments, and
/// the single-threaded caller re-sorts into creation order, so the result is
/// identical to a sequential open whatever the thread schedule was. The
/// first failure in inventory order is reported, matching sequential order.
fn open_segments_parallel(
    inventory: &[(u64, plomid_core::DeviceId, std::path::PathBuf)],
    pool_capacity: usize,
) -> Result<Vec<Segment>> {
    let parallelism = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4)
        .max(1);
    let chunk_size = inventory.len().div_ceil(parallelism).max(1);
    let mut opened: Vec<Segment> = Vec::with_capacity(inventory.len());
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in inventory.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                chunk
                    .iter()
                    .map(|(segment_id, device_id, path)| {
                        open_one_segment(*segment_id, *device_id, path.clone(), pool_capacity)
                    })
                    .collect::<Result<Vec<Segment>>>()
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut segments)) => opened.append(&mut segments),
                // A thread panic here is an internal failure, never a data
                // signal: the opens perform no mutation, so retrying the mount
                // observes the same durable state.
                Ok(Err(error)) => {
                    return Err(error);
                }
                Err(_) => {
                    return Err(PlomidError::new(
                        ErrorKind::Internal,
                        "parallel segment open did not complete",
                    ));
                }
            }
        }
        Ok(())
    })?;
    // Chunks complete out of order; restore creation order deterministically.
    opened.sort_unstable_by_key(|segment| segment.id);
    Ok(opened)
}

/// Best-effort resolution of the published catalog state for one version.
///
/// Mirrors the generation collector's checkpoint handling: candidates are
/// examined newest-first and malformed ones are skipped, because an
/// unreadable historical catalog must not wedge segment reclamation. The
/// selected state (when any) is fully decoded before it pins anything.
fn published_catalog_for_version(
    root: &Path,
    version: CatalogVersion,
) -> Result<Option<crate::catalog::CatalogState>> {
    for path in crate::catalog::discover_catalogs(root)?.into_iter().rev() {
        let state = match crate::catalog::load_catalog(&path) {
            Ok(state) => state,
            Err(_) => continue,
        };
        if state.is_published() && state.catalog_version() == version {
            return Ok(Some(state));
        }
    }
    Ok(None)
}
