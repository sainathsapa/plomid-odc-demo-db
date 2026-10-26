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
//! The persistent B+Tree index surface.
//!
//! [`BTreeIndex`] owns the ordered key structure only. Durable bytes live in
//! 16 KiB pages of one file managed by [`BufferPool`]; integrity is the
//! page-level CRC32C; durability ordering is the existing WAL; structural
//! replay after a crash is the existing [`ReplayTarget`] recovery path driven
//! by `plomid_wal::recover_into`.
//!
//! Page 0 always holds the [`RootMeta`] block; tree nodes occupy pages 1..n.
//! The tree is single-writer: every operation takes `&mut self`, matching the
//! `plomid-storage` B+Tree convention.

use std::path::{Path, PathBuf};

use plomid_core::{ErrorKind, IndexId, ObjectId, PageId, PlomidError, Result, RowId};
use plomid_storage::BufferPool;

use super::constants::{
    INDEX_FLAG_UNIQUE, INDEX_FORMAT_VERSION, INTERNAL, LEAF, LEN_U16, NODE_HEADER_SIZE, NODE_MAGIC,
    NODE_MIN_ENTRIES, NO_PAGE, PAGE_PAYLOAD_SIZE, ROW_ID_SIZE,
};
use super::entry::{Bound, IndexEntry};
use super::node::{
    decode_node, encoded_len, internal_entry_size, leaf_entry_size, write_node, InternalEntry,
    LeafEntry, Node,
};
use super::root_meta::RootMeta;

/// Result of an insert into the index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    /// The key was new; a fresh entry was created.
    Inserted,
    /// The key existed and the RowId was appended to its set.
    RowAppended,
    /// The key and RowId were both already present; nothing changed.
    Unchanged,
}

/// A persistent ordered B+Tree SQL index over one page file.
pub struct BTreeIndex {
    pool: BufferPool,
    meta: RootMeta,
    path: PathBuf,
}

/// Structural summary returned by [`BTreeIndex::stats`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BTreeStats {
    /// Total indexed keys.
    pub entry_count: u64,
    /// Pages holding tree nodes.
    pub node_pages: u64,
    /// Depth of the tree (single leaf root has depth 1).
    pub depth: u64,
}

impl BTreeIndex {
    /// Creates an empty index file at `path`.
    ///
    /// `index_id` and `object_id` are stable catalog identities; `generation`
    /// binds this index build to a data generation for later publication.
    /// `unique` selects unique-key enforcement.
    pub fn create(
        path: &Path,
        pool_capacity: usize,
        index_id: IndexId,
        object_id: ObjectId,
        generation: plomid_core::GenerationId,
        unique: bool,
    ) -> Result<Self> {
        let mut pool = BufferPool::create(path, pool_capacity)?;
        let _meta_page = pool.allocate_page()?.id();
        let flags = if unique { INDEX_FLAG_UNIQUE } else { 0 };
        let meta = RootMeta {
            version: INDEX_FORMAT_VERSION,
            flags,
            index_id,
            object_id,
            generation,
            // NO_PAGE means "no root yet": the tree is empty.
            root_page: PageId::new(NO_PAGE),
            entry_count: 0,
        };
        let mut index = Self {
            pool,
            meta,
            path: path.to_path_buf(),
        };
        index.persist_meta()?;
        tracing::debug!(target: "index::btree", "created path={} index_id={} unique={}", path.display(), index_id.get(), unique);
        Ok(index)
    }

    /// Opens an existing index file, validating its root metadata.
    pub fn open(path: &Path, pool_capacity: usize) -> Result<Self> {
        let mut pool = BufferPool::open(path, pool_capacity)?;
        let handle = pool.get_page(PageId::new(0))?;
        let meta = RootMeta::decode(handle.data())?;
        if meta.root_page.get() != NO_PAGE && meta.root_page.get() == 0 {
            return Err(PlomidError::new(
                ErrorKind::Corruption,
                "root metadata points at the metadata page",
            ));
        }
        Ok(Self {
            pool,
            meta,
            path: path.to_path_buf(),
        })
    }

    /// Returns the index's stable catalog identity.
    #[must_use]
    pub fn index_id(&self) -> IndexId {
        self.meta.index_id
    }

    /// Returns the owning table's object identity.
    #[must_use]
    pub fn object_id(&self) -> ObjectId {
        self.meta.object_id
    }

    /// Returns the generation this index build is bound to.
    #[must_use]
    pub fn generation(&self) -> plomid_core::GenerationId {
        self.meta.generation
    }

    /// Returns true when this index enforces key uniqueness.
    #[must_use]
    pub fn is_unique(&self) -> bool {
        self.meta.is_unique()
    }

    /// Returns the number of indexed keys.
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.meta.entry_count
    }

    /// Returns the current root page, if the tree holds any entries.
    #[must_use]
    pub fn root_page(&self) -> Option<PageId> {
        self.root_id()
    }

    fn root_id(&self) -> Option<PageId> {
        (self.meta.root_page.get() != NO_PAGE).then_some(self.meta.root_page)
    }

    // -- metadata and node persistence ------------------------------------

    fn persist_meta(&mut self) -> Result<()> {
        let encoded = self.meta.encode()?;
        let mut handle = self.pool.get_page(PageId::new(0))?;
        let data = handle.data_mut();
        data.fill(0);
        data[..encoded.len()].copy_from_slice(&encoded);
        self.pool.unpin_page(handle, true)
    }

    fn set_root(&mut self, root: Option<PageId>) -> Result<()> {
        self.meta.root_page = root.unwrap_or(PageId::new(NO_PAGE));
        self.persist_meta()
    }

    fn read_node(&mut self, page_id: PageId) -> Result<Node> {
        let handle = self.pool.get_page(page_id)?;
        let node = decode_node(handle.data())?;
        self.pool.unpin_page(handle, false)?;
        Ok(node)
    }

    fn write_node_at(&mut self, page_id: PageId, node: &Node) -> Result<()> {
        let mut handle = self.pool.get_page(page_id)?;
        write_node(&mut handle, node)?;
        self.pool.unpin_page(handle, true)
    }

    fn alloc_node(&mut self, node: &Node) -> Result<PageId> {
        let mut handle = self.pool.allocate_page()?;
        let page_id = handle.id();
        write_node(&mut handle, node)?;
        self.pool.unpin_page(handle, true)?;
        Ok(page_id)
    }

    /// True when the encoded node still fits in one page payload.
    fn fits(node: &Node) -> Result<bool> {
        Ok(encoded_len(node)? <= PAGE_PAYLOAD_SIZE)
    }

    // -- search -----------------------------------------------------------

    /// Walks backward from `landing` across every page that may hold `key`,
    /// returning the pages oldest-first.
    ///
    /// A predecessor may hold `key` while its last entry sorts at-or-above
    /// it; empty predecessors are skipped over conservatively (they arise
    /// only when a partial-key deletion could not unlink, which reads
    /// tolerate). The walk provably terminates at the chain start, and every
    /// skipped page sorts strictly below `key`, so no page holding `key` is
    /// ever skipped: node order is physical key order, which deletions
    /// preserve.
    fn run_pages_before(&mut self, landing: PageId, key: &[u8]) -> Result<Vec<PageId>> {
        let mut pages = vec![landing];
        loop {
            let node = self.read_node(*pages.last().expect("non-empty"))?;
            let Node::Leaf { prev, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            let Some(prev_page) = prev else {
                break;
            };
            let prev_node = self.read_node(prev_page)?;
            let Node::Leaf { entries, .. } = prev_node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            // Empty predecessors carry nothing to compare: skip over them.
            // A non-empty predecessor whose maximum sorts below `key` ends
            // the run; anything at-or-above may still hold it.
            if let Some(last) = entries.last() {
                if last.key.as_slice() < key {
                    break;
                }
            }
            pages.push(prev_page);
        }
        pages.reverse();
        Ok(pages)
    }

    /// Merges every `key` posting in `pages` (ascending) into one ascending
    /// RowId list. Pages hold disjoint ordered slices of the run, so plain
    /// concatenation in page order preserves global order.
    fn collect_run_ids(&mut self, pages: &[PageId], key: &[u8]) -> Result<Vec<RowId>> {
        let mut merged = Vec::new();
        for page in pages {
            let node = self.read_node(*page)?;
            let Node::Leaf { entries, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            for entry in &entries {
                if entry.key.as_slice() == key {
                    merged.extend_from_slice(&entry.row_ids);
                }
            }
        }
        Ok(merged)
    }

    /// Rewinds `(page, index)` to the first entry at-or-after `key`,
    /// returning the run-start position for inclusive scans and lookups.
    /// Combines the backward page walk with an in-node lower bound.
    fn rewind_to_key(&mut self, page: PageId, key: &[u8]) -> Result<(PageId, usize)> {
        let pages = self.run_pages_before(page, key)?;
        let Some(&first) = pages.first() else {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "B+Tree run walk returned no pages",
            ));
        };
        let node = self.read_node(first)?;
        let Node::Leaf { entries, .. } = node else {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "B+Tree key run must stay within leaves",
            ));
        };
        let index = match Self::leaf_position(&entries, key) {
            Ok(found) => {
                // Binary search may land mid-run: retreat to its start.
                let mut start = found;
                while start > 0 && entries[start - 1].key.as_slice() == key {
                    start -= 1;
                }
                start
            }
            Err(insertion) => insertion,
        };
        Ok((first, index))
    }

    /// Binary-searches one leaf's entries for `key`.
    ///
    /// Returns the index of the matching entry, or the insertion point that
    /// keeps ordering (the `Err` arm of `binary_search_by`).
    fn leaf_position(entries: &[LeafEntry], key: &[u8]) -> std::result::Result<usize, usize> {
        entries.binary_search_by(|entry| entry.key.as_slice().cmp(key))
    }

    /// Maximum RowIds one entry with a `key_len`-byte key can carry while the
    /// encoded leaf still fits a page, or `None` when the key alone overflows
    /// (unfixable: no chunking can help, and the historical error stands).
    fn max_ids_per_entry(key_len: usize) -> Option<usize> {
        let overhead = NODE_HEADER_SIZE + LEN_U16 + LEN_U16 + key_len;
        let room = PAGE_PAYLOAD_SIZE.checked_sub(overhead)?;
        // At least one id per chunk, or chunking could never terminate.
        Some((room / ROW_ID_SIZE).max(1))
    }

    /// Splits an oversized posting list into page-fitting same-key chunks.
    ///
    /// Chunks keep ascending RowId order and reuse the exact entry encoding,
    /// so no format change, page type, or version bump is involved: a chunk
    /// is an ordinary entry that happens to share its key. Callers must merge
    /// same-key chunks on reads (see `collect_key_run`). Only an oversized
    /// *key* still errors, exactly as before.
    fn chunk_postings(key: &[u8], mut row_ids: Vec<RowId>) -> Result<Vec<LeafEntry>> {
        row_ids.sort_unstable();
        Self::chunk_postings_sorted(key, row_ids)
    }

    /// Splits an already-sorted posting list into page-fitting same-key
    /// chunks, without re-sorting. Same layout and even distribution as
    /// [`Self::chunk_postings`]; the caller guarantees ascending input
    /// (fresh-build appends, tail re-cuts). Only an oversized *key* errors.
    fn chunk_postings_sorted(key: &[u8], row_ids: Vec<RowId>) -> Result<Vec<LeafEntry>> {
        let Some(max_ids) = Self::max_ids_per_entry(key.len()) else {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "single entry exceeds the index page payload",
            ));
        };
        // A single fitting entry keeps the historical single-entry layout.
        if leaf_entry_size(key.len(), row_ids.len()) <= PAGE_PAYLOAD_SIZE - NODE_HEADER_SIZE {
            return Ok(vec![LeafEntry {
                key: key.to_vec(),
                row_ids,
            }]);
        }
        // Even distribution (sizes differ by at most one) instead of
        // full-plus-runt: no degenerate single-row tail chunk exists to
        // empty out later, which keeps middle-node deletions rare.
        let total = row_ids.len();
        let chunks = total.div_ceil(max_ids);
        let base = total / chunks;
        let extra = total % chunks;
        let mut out = Vec::with_capacity(chunks);
        let mut rest = row_ids;
        for index in 0..chunks {
            let take = (base + usize::from(index < extra)).min(rest.len());
            let tail = rest.split_off(take);
            out.push(LeafEntry {
                key: key.to_vec(),
                row_ids: std::mem::replace(&mut rest, tail),
            });
        }
        debug_assert!(rest.is_empty());
        Ok(out)
    }

    /// Splits every oversized entry of an in-memory leaf into fitting chunks.
    ///
    /// Used wherever a node is assembled before the fit check (inserts that
    /// grew a posting list, splits), so the "single entry exceeds the page"
    /// error survives only for genuinely oversized keys.
    fn chunk_oversized_entries(entries: Vec<LeafEntry>) -> Result<Vec<LeafEntry>> {
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            if leaf_entry_size(entry.key.len(), entry.row_ids.len())
                <= PAGE_PAYLOAD_SIZE - NODE_HEADER_SIZE
            {
                out.push(entry);
                continue;
            }
            out.extend(Self::chunk_postings(&entry.key, entry.row_ids)?);
        }
        Ok(out)
    }

    /// Routes `key` through an encoded internal node without allocating.
    ///
    /// Returns the child the owned descent ([`Self::path_to`]) would select:
    /// the last separator at or below `key`, or `first`. The whole node is
    /// still walked (not just to the routing decision) so truncated or
    /// overflowing pages fail with the same corruption errors decoding
    /// would produce. This is the bulk-insert hot path: the owned decode
    /// allocated one key `Vec` per separator on every insert at every level.
    fn route_internal_borrowed(data: &[u8], key: &[u8]) -> Result<PageId> {
        if data.len() < NODE_HEADER_SIZE || data[..4] != NODE_MAGIC || data[4] != INTERNAL {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "B+Tree internal routing on a non-internal node",
            ));
        }
        let count = usize::from(u16::from_le_bytes([data[6], data[7]]));
        let mut child =
            PageId::new(u64::from_le_bytes(data[8..16].try_into().map_err(
                |_| PlomidError::new(ErrorKind::Corruption, "invalid internal child"),
            )?));
        let mut cursor = NODE_HEADER_SIZE;
        for _ in 0..count {
            // Internal entry layout (see `encode_node`): key_len[u16] |
            // child[u64] | key[key_len].
            let key_len = usize::from(u16::from_le_bytes(
                data.get(cursor..cursor + 2)
                    .ok_or_else(|| PlomidError::new(ErrorKind::Corruption, "separator truncated"))?
                    .try_into()
                    .map_err(|_| PlomidError::new(ErrorKind::Corruption, "separator truncated"))?,
            ));
            let child_start = cursor + 2;
            let child_end = child_start.checked_add(8).ok_or_else(|| {
                PlomidError::new(ErrorKind::Corruption, "separator length overflow")
            })?;
            let key_end = child_end.checked_add(key_len).ok_or_else(|| {
                PlomidError::new(ErrorKind::Corruption, "separator length overflow")
            })?;
            let stored = data
                .get(child_end..key_end)
                .ok_or_else(|| PlomidError::new(ErrorKind::Corruption, "separator truncated"))?;
            // Lower-bound routing, mirroring `path_to`: the last separator
            // at or below `key` wins. Later separators are still validated
            // below so a corrupt tail cannot hide behind an early decision.
            if key >= stored {
                child = PageId::new(u64::from_le_bytes(
                    data.get(child_start..child_end)
                        .ok_or_else(|| {
                            PlomidError::new(ErrorKind::Corruption, "separator truncated")
                        })?
                        .try_into()
                        .map_err(|_| {
                            PlomidError::new(ErrorKind::Corruption, "invalid child page")
                        })?,
                ));
            }
            cursor = key_end;
            if cursor > data.len() {
                return Err(PlomidError::new(
                    ErrorKind::Corruption,
                    "B+Tree node is truncated",
                ));
            }
        }
        Ok(child)
    }

    /// Descends from the root to the leaf that owns `key`, returning
    /// ancestor pages (root first) and the leaf page, decoding only the
    /// leaf. An empty tree returns `None`. Internal levels route borrowed
    /// (see [`Self::route_internal_borrowed`]); callers needing an ancestor
    /// for mutation read it then (split path only).
    fn descend_to_leaf(&mut self, key: &[u8]) -> Result<Option<(Vec<PageId>, PageId)>> {
        let Some(mut page_id) = self.root_id() else {
            return Ok(None);
        };
        let mut ancestors = Vec::new();
        loop {
            let handle = self.pool.get_page(page_id)?;
            let data = handle.data();
            let is_leaf =
                data.len() >= NODE_HEADER_SIZE && data[..4] == NODE_MAGIC && data[4] == LEAF;
            if is_leaf {
                self.pool.unpin_page(handle, false)?;
                return Ok(Some((ancestors, page_id)));
            }
            let child = Self::route_internal_borrowed(data, key)?;
            self.pool.unpin_page(handle, false)?;
            ancestors.push(page_id);
            page_id = child;
        }
    }

    /// Bulk-builds the tree from sorted `(key, row_id)` pairs in one pass.
    ///
    /// Used by generation builds, where the whole key set is known up front
    /// and pairs are unique: per-row insertion would re-descend, re-chunk,
    /// and re-split on every row (quadratic on low-cardinality runs),
    /// while bottom-up packing touches each entry once. The on-disk result
    /// is indistinguishable from incremental insertion — same chunking
    /// ([`Self::chunk_postings_sorted`]), same leaf chain, same separator
    /// convention (each separator is its child's first key, exactly as
    /// [`Self::split_leaf`] publishes), same counts — so every read,
    /// delete, split, and reopen path behaves identically.
    ///
    /// The input is sorted here (by key, then RowId); callers must not rely
    /// on input order beyond the uniqueness contract shared with
    /// [`Self::insert_append`].
    pub fn build_bulk(&mut self, mut rows: Vec<(Vec<u8>, RowId)>) -> Result<()> {
        rows.sort_unstable();
        rows.dedup();
        // One chunked entry list per distinct key, in ascending key order.
        let mut entries: Vec<LeafEntry> = Vec::new();
        let mut cursor = 0_usize;
        while cursor < rows.len() {
            let end = rows[cursor..]
                .iter()
                .position(|(key, _)| *key != rows[cursor].0)
                .map_or(rows.len(), |offset| cursor + offset);
            let ids: Vec<RowId> = rows[cursor..end].iter().map(|(_, id)| *id).collect();
            // Rows arrive grouped by key with ascending RowIds after the
            // sort above, so the no-sort chunker applies directly.
            entries.extend(Self::chunk_postings_sorted(&rows[cursor].0, ids)?);
            cursor = end;
        }
        let entry_count = entries.len() as u64;
        if entries.is_empty() {
            self.meta.entry_count = 0;
            self.set_root(None)?;
            return Ok(());
        }
        // Pack leaves greedily by encoded size; every leaf but the last is
        // guaranteed non-empty, and no leaf exceeds the page payload.
        // Successor links are fixed up as each leaf lands (allocation order
        // is sequential, but the code reads back and rewrites instead of
        // assuming id arithmetic).
        let mut leaves: Vec<(Vec<u8>, PageId)> = Vec::new();
        let mut start = 0_usize;
        let mut used = NODE_HEADER_SIZE;
        let mut prev: Option<PageId> = None;
        for index in 0..=entries.len() {
            let boundary = index == entries.len()
                || used + leaf_entry_size(entries[index].key.len(), entries[index].row_ids.len())
                    > PAGE_PAYLOAD_SIZE;
            if boundary && index > start {
                let page = self.alloc_leaf(&entries[start..index], prev)?;
                if let Some(prev_page) = prev {
                    let mut prev_node = self.read_node(prev_page)?;
                    if let Node::Leaf { next, .. } = &mut prev_node {
                        *next = Some(page);
                        self.write_node_at(prev_page, &prev_node)?;
                    }
                }
                prev = Some(page);
                leaves.push((entries[start].key.clone(), page));
                start = index;
                used = NODE_HEADER_SIZE;
            }
            if index < entries.len() {
                used += leaf_entry_size(entries[index].key.len(), entries[index].row_ids.len());
            }
        }
        // The loop above always emits the trailing leaf: when the final
        // entry overflows the current page it is emitted alone (a lone
        // oversized entry errors inside `alloc_leaf` exactly like the
        // incremental path's single-entry error).
        let mut children = leaves;
        // Raise internal levels until one root remains. Each parent takes a
        // maximal run of children that fits; separators are the exact
        // first-keys the split path would publish, so routing (including
        // the upper-bound duplicate rule) is unchanged.
        while children.len() > 1 {
            let mut parents: Vec<(Vec<u8>, PageId)> = Vec::new();
            let mut start = 0_usize;
            let mut used = NODE_HEADER_SIZE;
            for index in 0..=children.len() {
                let boundary = index == children.len()
                    || (index > start
                        && used + internal_entry_size(children[index].0.len()) > PAGE_PAYLOAD_SIZE);
                if boundary && index > start {
                    let page = self.alloc_internal(&children[start..index])?;
                    parents.push((children[start].0.clone(), page));
                    start = index;
                    used = NODE_HEADER_SIZE;
                }
                if index < children.len() {
                    used += internal_entry_size(children[index].0.len());
                }
            }
            children = parents;
        }
        self.meta.entry_count = entry_count;
        let (_, root) = children.pop().expect("at least one child");
        self.set_root(Some(root))?;
        // `set_root` persists; keep the explicit persist for symmetry with
        // the incremental path (harmless duplicate write of page 0).
        self.persist_meta()?;
        Ok(())
    }

    /// Allocates one leaf page over `entries` with the given predecessor
    /// link, returning its page id. The successor link is fixed up by the
    /// caller (or left `None` for the final leaf).
    fn alloc_leaf(&mut self, entries: &[LeafEntry], prev: Option<PageId>) -> Result<PageId> {
        let node = Node::Leaf {
            prev,
            next: None,
            entries: entries.to_vec(),
        };
        if Self::fits(&node)? {
            return self.alloc_node(&node);
        }
        Err(PlomidError::new(
            ErrorKind::InvalidArgument,
            "single entry exceeds the index page payload",
        ))
    }

    /// Allocates one internal page over `(first_key, child)` pairs; the
    /// first pair supplies `first`, the rest become separators.
    fn alloc_internal(&mut self, children: &[(Vec<u8>, PageId)]) -> Result<PageId> {
        let first = children.first().expect("non-empty level").1;
        let separators: Vec<InternalEntry> = children[1..]
            .iter()
            .map(|(key, child)| InternalEntry {
                key: key.clone(),
                child: *child,
            })
            .collect();
        self.alloc_node(&Node::Internal { first, separators })
    }

    /// Descends from the root to the leaf that owns `key`, returning the full
    /// path (root first, leaf last). An empty tree returns an empty path.
    fn path_to(&mut self, key: &[u8]) -> Result<Vec<(PageId, Node)>> {
        let mut path = Vec::new();
        let Some(mut page_id) = self.root_id() else {
            return Ok(path);
        };
        loop {
            let node = self.read_node(page_id)?;
            let is_leaf = node.is_leaf();
            path.push((page_id, node));
            if is_leaf {
                return Ok(path);
            }
            let Node::Internal { first, separators } = &path.last().expect("pushed").1 else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree path type mismatch",
                ));
            };
            // Lower-bound routing: the child is the last separator whose key
            // is <= `key`, or `first` below all separator keys.
            page_id = separators
                .iter()
                .rev()
                .find(|entry| key >= entry.key.as_slice())
                .map(|entry| entry.child)
                .unwrap_or(*first);
        }
    }

    /// Returns the leftmost leaf page, if the tree is non-empty.
    fn leftmost_leaf(&mut self) -> Result<Option<PageId>> {
        let Some(mut page_id) = self.root_id() else {
            return Ok(None);
        };
        loop {
            let node = self.read_node(page_id)?;
            match node {
                Node::Leaf { .. } => return Ok(Some(page_id)),
                Node::Internal { first, .. } => page_id = first,
            }
        }
    }

    /// Returns the rightmost leaf page, if the tree is non-empty.
    fn rightmost_leaf(&mut self) -> Result<Option<PageId>> {
        let Some(mut page_id) = self.root_id() else {
            return Ok(None);
        };
        loop {
            let node = self.read_node(page_id)?;
            match node {
                Node::Leaf { .. } => return Ok(Some(page_id)),
                Node::Internal { separators, .. } => {
                    page_id = separators.last().map(|entry| entry.child).ok_or_else(|| {
                        PlomidError::new(ErrorKind::Corruption, "internal node has no children")
                    })?;
                }
            }
        }
    }

    /// Point lookup: all RowIds registered under `key`.
    ///
    /// Same-key chunks (see `chunk_postings`) merge here: the leaf is located
    /// by routing, then the run is collected backward to its start and
    /// forward to its end, so a key split across nodes by earlier compactions
    /// still resolves completely. Single-entry keys take the historical
    /// single-leaf path plus one predecessor read.
    pub fn lookup(&mut self, key: &[u8]) -> Result<Option<Vec<RowId>>> {
        let Some(landing) = self.leaf_page_for(key)? else {
            return Ok(None);
        };
        let pages = self.key_run_pages(landing, key)?;
        let merged = self.collect_run_ids(&pages, key)?;
        if merged.is_empty() {
            return Ok(None);
        }
        Ok(Some(merged))
    }

    /// All pages that may hold `key`, oldest-first: backward to the run start
    /// (skipping empty nodes conservatively), then forward past the landing
    /// to the run end. Empty nodes contribute nothing at collection time.
    fn key_run_pages(&mut self, landing: PageId, key: &[u8]) -> Result<Vec<PageId>> {
        let mut pages = self.run_pages_before(landing, key)?;
        // Forward past the landing while the next node may still hold `key`
        // (first entry at-or-below it, or an empty node we conservatively
        // cross). Stops at the first node sorting strictly above `key`.
        loop {
            let last = *pages.last().expect("non-empty run pages");
            let node = self.read_node(last)?;
            let Node::Leaf { next, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            let Some(next_page) = next else {
                break;
            };
            let next_node = self.read_node(next_page)?;
            let Node::Leaf { entries, .. } = next_node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            match entries.first() {
                None => pages.push(next_page),
                Some(first) if first.key.as_slice() <= key => pages.push(next_page),
                _ => break,
            }
        }
        Ok(pages)
    }

    /// Returns true when `key` is present.
    ///
    /// Run-aware like [`Self::lookup`]: chunked same-key entries may span
    /// nodes, so presence checks the whole run, stopping at the first hit.
    pub fn contains(&mut self, key: &[u8]) -> Result<bool> {
        let Some(landing) = self.leaf_page_for(key)? else {
            return Ok(false);
        };
        for page in self.key_run_pages(landing, key)? {
            let node = self.read_node(page)?;
            let Node::Leaf { entries, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            if entries.iter().any(|entry| entry.key.as_slice() == key) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Descends from the root to the leaf that owns `key`, decoding only the
    /// internal routing nodes. An empty tree returns `None`.
    fn leaf_page_for(&mut self, key: &[u8]) -> Result<Option<PageId>> {
        let Some(mut page_id) = self.root_id() else {
            return Ok(None);
        };
        loop {
            let node = self.read_node(page_id)?;
            match &node {
                Node::Leaf { .. } => return Ok(Some(page_id)),
                Node::Internal { first, separators } => {
                    page_id = separators
                        .iter()
                        .rev()
                        .find(|entry| key >= entry.key.as_slice())
                        .map(|entry| entry.child)
                        .unwrap_or(*first);
                }
            }
        }
    }

    // -- insert ------------------------------------------------------------

    /// Inserts `key -> row_id`.
    ///
    /// Unique indexes reject a second distinct RowId under an existing key
    /// with the PLOMID `Conflict` error. Non-unique indexes append the RowId
    /// to the key's ordered set. Structural splits happen whenever the encoded
    /// node no longer fits a page; the parent chain is updated bottom-up.
    pub fn insert(&mut self, key: &[u8], row_id: RowId) -> Result<InsertOutcome> {
        self.insert_inner(key, row_id)
    }

    fn insert_inner(&mut self, key: &[u8], row_id: RowId) -> Result<InsertOutcome> {
        // Borrowed descent: only the landing leaf is decoded. Ancestor pages
        // travel as ids; the split path below reads them back if (and only
        // if) the leaf overflows, which is rare next to the per-insert
        // routing that used to decode every level.
        let Some((path, leaf_page)) = self.descend_to_leaf(key)? else {
            // First key in an empty tree: create a root leaf.
            let node = Node::Leaf {
                prev: None,
                next: None,
                entries: vec![LeafEntry {
                    key: key.to_vec(),
                    row_ids: vec![row_id],
                }],
            };
            let page_id = self.alloc_node(&node)?;
            self.meta.entry_count = 1;
            self.set_root(Some(page_id))?;
            return Ok(InsertOutcome::Inserted);
        };

        let node = self.read_node(leaf_page)?;
        let (prev, next, mut entries) = match node {
            Node::Leaf {
                prev,
                next,
                entries,
            } => (prev, next, entries),
            Node::Internal { .. } => {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree path must end at a leaf",
                ))
            }
        };
        let mut inserted = false;
        // Full-run duplicate suppression: the row may already sit in any
        // chunk of this key (here or in earlier nodes after splits). The
        // walks are read-only; mutation below touches only the landing node,
        // whose ancestor path is in hand. Append mode (`insert_append`)
        // skips this O(run length) scan by caller contract (unique pairs);
        // the landing node's own portion is still checked below.
        let run_pages = self.run_pages_before(leaf_page, key)?;
        let mut key_present = false;
        let mut duplicate = false;
        'scan: for page in &run_pages {
            let node = self.read_node(*page)?;
            let Node::Leaf { entries, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree key run must stay within leaves",
                ));
            };
            for entry in &entries {
                if entry.key.as_slice() == key {
                    key_present = true;
                    if entry.row_ids.contains(&row_id) {
                        duplicate = true;
                        break 'scan;
                    }
                }
            }
        }
        if duplicate {
            // Nothing changed: no leaf rewrite and no metadata write.
            return Ok(InsertOutcome::Unchanged);
        }
        if self.is_unique() && key_present {
            return Err(PlomidError::new(
                ErrorKind::Conflict,
                "duplicate key violates unique index",
            ));
        }
        // Entry-count delta for this node (chunk splits add entries).
        let delta: i64;
        match Self::leaf_position(&entries, key) {
            Ok(index) => {
                // Append into this node's run portion and re-chunk it: the
                // portion stays ordered and page-fitting as one unit.
                let mut start = index;
                while start > 0 && entries[start - 1].key.as_slice() == key {
                    start -= 1;
                }
                let mut end = index + 1;
                while end < entries.len() && entries[end].key.as_slice() == key {
                    end += 1;
                }
                let before = (end - start) as i64;
                let mut ids: Vec<RowId> = entries[start..end]
                    .iter()
                    .flat_map(|entry| entry.row_ids.iter().copied())
                    .collect();
                ids.push(row_id);
                let chunks = Self::chunk_postings(key, ids)?;
                delta = chunks.len() as i64 - before;
                entries.splice(start..end, chunks);
            }
            Err(position) => {
                entries.insert(
                    position,
                    LeafEntry {
                        key: key.to_vec(),
                        row_ids: vec![row_id],
                    },
                );
                inserted = !key_present;
                delta = 1;
            }
        }
        if delta != 0 {
            self.meta.entry_count = (self.meta.entry_count as i64)
                .checked_add(delta)
                .and_then(|count| u64::try_from(count).ok())
                .ok_or_else(|| {
                    PlomidError::new(ErrorKind::Internal, "index entry count overflow")
                })?;
        }

        let leaf_node = Node::Leaf {
            prev,
            next,
            entries,
        };
        if Self::fits(&leaf_node)? {
            self.write_node_at(leaf_page, &leaf_node)?;
            // Persist whenever the entry count changed: new keys and chunk
            // splits both alter it, while pure appends leave page 0 alone.
            if inserted || delta != 0 {
                self.persist_meta()?;
            }
            return Ok(outcome_of(inserted));
        }
        self.insert_with_split(leaf_page, leaf_node, path)
    }
}

/// Maps the inserted flag onto the public outcome.
const fn outcome_of(inserted: bool) -> InsertOutcome {
    if inserted {
        InsertOutcome::Inserted
    } else {
        InsertOutcome::RowAppended
    }
}

impl BTreeIndex {
    /// Writes a leaf that no longer fits, splitting upward as needed.
    fn insert_with_split(
        &mut self,
        leaf_page: PageId,
        leaf_node: Node,
        path: Vec<PageId>,
    ) -> Result<InsertOutcome> {
        // Split the oversized leaf: the first key of the right half becomes
        // the separator that routes to the sibling page.
        let (right_page, separator) = self.split_leaf(leaf_page, leaf_node)?;
        // `path` already excludes the leaf (the caller popped it to apply the
        // change); ascend through the ancestors only.
        self.ascend_with_split(path, separator, right_page)?;
        self.persist_meta()?;
        Ok(InsertOutcome::Inserted)
    }

    /// Pushes `(separator, new_child)` into the ancestor chain, splitting
    /// internal nodes upward and creating a new root when the old root splits.
    /// Ancestors arrive as page ids (borrowed descent) and are decoded here
    /// only because a split below forces their mutation — the rare path.
    fn ascend_with_split(
        &mut self,
        mut path: Vec<PageId>,
        mut separator: Vec<u8>,
        mut new_child: PageId,
    ) -> Result<()> {
        loop {
            let Some(parent_page) = path.pop() else {
                // The old root split. Its left half still lives at the old
                // root page (leaf or internal); build the new root above it.
                let old_root = self.meta.root_page;
                let new_root = Node::Internal {
                    first: old_root,
                    separators: vec![InternalEntry {
                        key: separator,
                        child: new_child,
                    }],
                };
                let page = self.alloc_node(&new_root)?;
                self.set_root(Some(page))?;
                return Ok(());
            };
            let parent_node = self.read_node(parent_page)?;
            let Node::Internal {
                first,
                mut separators,
            } = parent_node
            else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree ancestor must be internal",
                ));
            };
            // Upper-bound insertion: the new child sorts AFTER all existing
            // equal separators. With duplicate keys (chunked posting runs share
            // one key) lower-bound insertion would file the newest sibling
            // mid-array while routing ("last separator <= key") sends queries
            // to the last equal — landing mid-run and breaking global RowId
            // order on subsequent appends. Appending after equals keeps
            // creation order == chain order.
            let position =
                separators.partition_point(|entry| entry.key.as_slice() <= separator.as_slice());
            separators.insert(
                position,
                InternalEntry {
                    key: separator,
                    child: new_child,
                },
            );
            let updated = Node::Internal { first, separators };
            if Self::fits(&updated)? {
                self.write_node_at(parent_page, &updated)?;
                return Ok(());
            }
            let (sibling_page, up_separator) = self.split_internal(parent_page, updated)?;
            separator = up_separator;
            new_child = sibling_page;
        }
    }

    /// Byte-aware leaf split: chooses the boundary closest to the encoded
    /// byte midpoint so neither half exceeds the page payload. Maintains the
    /// previous/next leaf chain in both directions.
    fn split_leaf(&mut self, leaf_page: PageId, leaf: Node) -> Result<(PageId, Vec<u8>)> {
        let Node::Leaf {
            prev,
            next,
            entries,
        } = leaf
        else {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "leaf split applied to an internal node",
            ));
        };
        // Chunk oversized postings first: a lone oversized entry becomes
        // ordinary same-key chunks (which split freely below), so the error
        // below survives only for genuinely oversized keys.
        let mut entries = Self::chunk_oversized_entries(entries)?;
        if entries.len() < 2 {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "single entry exceeds the index page payload",
            ));
        }
        let entry_bytes = |entry: &LeafEntry| leaf_entry_size(entry.key.len(), entry.row_ids.len());
        let split_at = byte_midpoint(entries.iter().map(entry_bytes));
        let right_entries = entries.split_off(split_at);
        let separator = right_entries[0].key.clone();
        let right = Node::Leaf {
            prev: Some(leaf_page),
            next,
            entries: right_entries,
        };
        let right_page = self.alloc_node(&right)?;
        // Re-point the old successor's `prev` at the new sibling.
        if let Some(next_page) = next {
            let mut next_node = self.read_node(next_page)?;
            if let Node::Leaf { prev, .. } = &mut next_node {
                *prev = Some(right_page);
            }
            self.write_node_at(next_page, &next_node)?;
        }
        let left = Node::Leaf {
            prev,
            next: Some(right_page),
            entries,
        };
        self.write_node_at(leaf_page, &left)?;
        tracing::trace!(target: "index::btree", "leaf split page={} right={}", leaf_page.get(), right_page.get());
        Ok((right_page, separator))
    }

    /// Byte-aware internal split mirroring [`Self::split_leaf`]. The first
    /// separator of the right half is promoted upward and its child moves
    /// into the sibling's `first` slot.
    fn split_internal(&mut self, internal_page: PageId, node: Node) -> Result<(PageId, Vec<u8>)> {
        let Node::Internal {
            first,
            mut separators,
        } = node
        else {
            return Err(PlomidError::new(
                ErrorKind::Internal,
                "internal split applied to a leaf",
            ));
        };
        let entry_bytes = |entry: &InternalEntry| internal_entry_size(entry.key.len());
        let split_at = byte_midpoint(separators.iter().map(entry_bytes));
        let right_separators = separators.split_off(split_at);
        let up_separator = right_separators[0].key.clone();
        let right_first = right_separators[0].child;
        let right = Node::Internal {
            first: right_first,
            separators: right_separators.into_iter().skip(1).collect(),
        };
        let right_page = self.alloc_node(&right)?;
        let left = Node::Internal { first, separators };
        self.write_node_at(internal_page, &left)?;
        tracing::trace!(target: "index::btree", "internal split page={} right={}", internal_page.get(), right_page.get());
        Ok((right_page, up_separator))
    }
}

/// Splits an entry-size sequence at the boundary closest to the byte midpoint.
///
/// Mirrors the storage-layer B+Tree rule: minimize the larger half's encoded
/// size so no half can exceed the page payload when a valid boundary exists.
fn byte_midpoint(sizes: impl Iterator<Item = usize>) -> usize {
    let sizes: Vec<usize> = sizes.collect();
    let total: usize = sizes.iter().sum();
    let mut cumulative = 0_usize;
    let mut split_at = 1_usize;
    let mut best = usize::MAX;
    for (index, size) in sizes.iter().enumerate() {
        if index > 0 {
            let larger = usize::max(cumulative, total - cumulative);
            if larger < best {
                best = larger;
                split_at = index;
            }
        }
        cumulative += size;
    }
    split_at
}

impl BTreeIndex {
    // -- delete -------------------------------------------------------------

    /// Removes one RowId from `key`, or the whole entry when `all` is set.
    ///
    /// Returns true when the index changed. A leaf that empties during
    /// deletion is unlinked from the leaf chain and detached from its parent;
    /// the detachment propagates upward, contracting the root when only one
    /// child remains. Freed pages are not reclaimed in place; generation-level
    /// collection owns reclamation.
    pub fn delete(&mut self, key: &[u8], row_id: Option<RowId>, all: bool) -> Result<bool> {
        if !all && row_id.is_none() {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "delete requires a RowId unless deleting the whole key",
            ));
        }
        // Tail-loop: every iteration re-descends, so the mutated node always
        // carries a valid ancestor path (required for separator maintenance
        // and unlinking). Under exact routing the descent lands on the run
        // tail; each iteration removes at least one chunk there, so the loop
        // terminates. A landing that lacks the key ends the loop with prior
        // miss behavior.
        let mut removed_any = false;
        let mut delta: i64 = 0;
        loop {
            let mut path = self.path_to(key)?;
            let Some((leaf_page, node)) = path.pop() else {
                // Empty tree.
                break;
            };
            let (prev, next, mut entries) = match node {
                Node::Leaf {
                    prev,
                    next,
                    entries,
                } => (prev, next, entries),
                Node::Internal { .. } => {
                    return Err(PlomidError::new(
                        ErrorKind::Internal,
                        "B+Tree path must end at a leaf",
                    ))
                }
            };
            // This node's run portion (possibly empty: stale landings miss).
            let start = match Self::leaf_position(&entries, key) {
                Ok(index) => {
                    let mut start = index;
                    while start > 0 && entries[start - 1].key.as_slice() == key {
                        start -= 1;
                    }
                    start
                }
                Err(_) => break,
            };
            let mut end = start;
            while end < entries.len() && entries[end].key.as_slice() == key {
                end += 1;
            }
            if all {
                let removed = (end - start) as i64;
                entries.drain(start..end);
                delta -= removed;
                removed_any = true;
            } else {
                let target = row_id.expect("checked above");
                // Single-row deletes normally land on the run tail holding
                // the row; when the row sits in an earlier chunk (only
                // possible across chunked nodes), locate it with a backward
                // walk and rewrite that page by id. Structural work below
                // still applies only to freshly-descended nodes.
                let mut found: Option<(PageId, usize, usize)> = None;
                for (index, entry) in entries[start..end].iter().enumerate() {
                    if let Some(position) = entry.row_ids.iter().position(|id| *id == target) {
                        found = Some((leaf_page, start + index, position));
                        break;
                    }
                }
                let (target_page, entry_index, row_position) = match found {
                    Some(located) => located,
                    None => {
                        // Search earlier run nodes (read-only walk).
                        let mut located = None;
                        for page in self.run_pages_before(leaf_page, key)? {
                            if page == leaf_page {
                                continue;
                            }
                            let node = self.read_node(page)?;
                            let Node::Leaf { entries, .. } = node else {
                                return Err(PlomidError::new(
                                    ErrorKind::Internal,
                                    "B+Tree key run must stay within leaves",
                                ));
                            };
                            for (index, entry) in entries.iter().enumerate() {
                                if entry.key.as_slice() != key {
                                    continue;
                                }
                                if let Some(position) =
                                    entry.row_ids.iter().position(|id| *id == target)
                                {
                                    located = Some((page, index, position));
                                    break;
                                }
                            }
                            if located.is_some() {
                                break;
                            }
                        }
                        match located {
                            Some(located) => located,
                            // Absent everywhere reachable: miss, like before.
                            None => break,
                        }
                    }
                };
                // Rewrite by page id when the row lives outside the descended
                // node; structural maintenance below only runs for the
                // descended node itself (valid path in hand).
                if target_page != leaf_page {
                    let mut node = self.read_node(target_page)?;
                    let Node::Leaf { entries, .. } = &mut node else {
                        return Err(PlomidError::new(
                            ErrorKind::Internal,
                            "B+Tree key run must stay within leaves",
                        ));
                    };
                    let Some(entry) = entries.get_mut(entry_index) else {
                        break;
                    };
                    entry.row_ids.remove(row_position);
                    if entry.row_ids.is_empty() {
                        entries.remove(entry_index);
                        delta -= 1;
                    }
                    self.write_node_at(target_page, &node)?;
                    removed_any = true;
                    if delta != 0 {
                        self.meta.entry_count = (self.meta.entry_count as i64)
                            .checked_add(delta)
                            .and_then(|count| u64::try_from(count).ok())
                            .ok_or_else(|| {
                                PlomidError::new(ErrorKind::Internal, "index entry count underflow")
                            })?;
                        self.persist_meta()?;
                    }
                    return Ok(removed_any);
                }
                let entry = &mut entries[entry_index];
                entry.row_ids.remove(row_position);
                if entry.row_ids.is_empty() {
                    entries.remove(entry_index);
                    delta -= 1;
                }
                removed_any = true;
            }
            // Separator maintenance for the descended node: its first key may
            // have changed (entry removal), and the parent separator must
            // track it — otherwise later descents misroute. Order is safe:
            // removal only raises a first key, which stays below the next
            // sibling's minimum.
            let leaf_node = Node::Leaf {
                prev,
                next,
                entries,
            };
            // Borrow the first key before moving entries into the node.
            let first_key: Option<Vec<u8>> = match &leaf_node {
                Node::Leaf { entries, .. } => entries.first().map(|entry| entry.key.clone()),
                _ => None,
            };
            let underfull = leaf_node.len() < NODE_MIN_ENTRIES;
            self.write_node_at(leaf_page, &leaf_node)?;
            // N.B. merging the two `if let`s needs let-chains (Rust 1.88+);
            // MSRV here is 1.84.1, so the nest stays.
            #[allow(clippy::collapsible_match)]
            if let (Some(first), Some((parent_page, parent_node))) = (first_key, path.last_mut()) {
                if let Node::Internal {
                    first: parent_first,
                    separators,
                } = parent_node
                {
                    let stale = separators
                        .iter()
                        .any(|sep| sep.child == leaf_page && sep.key != first);
                    if stale {
                        for sep in separators.iter_mut() {
                            if sep.child == leaf_page {
                                sep.key = first.clone();
                            }
                        }
                        let rebuilt = Node::Internal {
                            first: *parent_first,
                            separators: std::mem::take(separators),
                        };
                        self.write_node_at(*parent_page, &rebuilt)?;
                    }
                }
            }
            if underfull {
                self.unlink_leaf(prev, next, leaf_page)?;
                self.detach_empty(path, leaf_page)?;
            }
            if !all {
                break;
            }
        }
        // Entry-count and metadata persistence for net removals.
        if delta != 0 {
            self.meta.entry_count = (self.meta.entry_count as i64)
                .checked_add(delta)
                .and_then(|count| u64::try_from(count).ok())
                .ok_or_else(|| {
                    PlomidError::new(ErrorKind::Internal, "index entry count underflow")
                })?;
            self.persist_meta()?;
        }
        Ok(removed_any)
    }

    /// Removes an emptied leaf from the prev/next chain: the neighbors link
    /// to each other across the removed page.
    fn unlink_leaf(
        &mut self,
        prev: Option<PageId>,
        next: Option<PageId>,
        removed: PageId,
    ) -> Result<()> {
        if let Some(prev_page) = prev {
            let mut node = self.read_node(prev_page)?;
            if let Node::Leaf { next: link, .. } = &mut node {
                if *link == Some(removed) {
                    *link = next;
                }
            }
            self.write_node_at(prev_page, &node)?;
        }
        if let Some(next_page) = next {
            let mut node = self.read_node(next_page)?;
            if let Node::Leaf { prev: link, .. } = &mut node {
                if *link == Some(removed) {
                    *link = prev;
                }
            }
            self.write_node_at(next_page, &node)?;
        }
        Ok(())
    }

    /// Detaches an emptied node from its parent, collapsing the ancestor chain
    /// upward. `path` holds the ancestors from the root down, exclusive of the
    /// emptied node itself.
    ///
    /// An internal node "empties" only by losing its last routing entry; the
    /// collapse then repeats at the grandparent. When the root collapses to a
    /// single child the root contracts to that child; when it collapses to
    /// nothing the tree becomes empty.
    fn detach_empty(&mut self, mut path: Vec<(PageId, Node)>, mut removed: PageId) -> Result<()> {
        loop {
            let Some((page, node)) = path.pop() else {
                // The removed node was the root: the tree is empty.
                self.set_root(None)?;
                return Ok(());
            };
            let Node::Internal {
                first,
                mut separators,
            } = node
            else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "B+Tree ancestor must be internal",
                ));
            };
            let original_len = separators.len();
            let mut new_first = first;
            if first == removed {
                match separators.first() {
                    // Promote the first separator's child into `first`.
                    Some(entry) => {
                        new_first = entry.child;
                        separators.remove(0);
                    }
                    None => {
                        // The parent had one child, which just emptied.
                        if path.is_empty() {
                            self.set_root(None)?;
                        } else {
                            removed = page;
                            continue;
                        }
                        return Ok(());
                    }
                }
            } else {
                separators.retain(|entry| entry.child != removed);
                if separators.len() == original_len {
                    // A deeper level already unlinked this child.
                    return Ok(());
                }
            }
            if separators.is_empty() {
                // The root contracted to a single child: root becomes that child.
                self.set_root(Some(new_first))?;
                return Ok(());
            }
            self.write_node_at(
                page,
                &Node::Internal {
                    first: new_first,
                    separators,
                },
            )?;
            return Ok(());
        }
    }
}

impl BTreeIndex {
    // -- ordered scans ------------------------------------------------------

    /// Resolves the starting leaf position for a scan: either the leftmost
    /// leaf, or the run-normalized position for `key`.
    ///
    /// Inclusive bounds rewind to the run start (a binary search may land
    /// mid-run after chunking); exclusive bounds advance past the whole run,
    /// including chunks spilling into later nodes. Both normalizations are
    /// read-only page walks bounded by the run length.
    fn scan_start(
        &mut self,
        from: Option<&[u8]>,
        inclusive: bool,
    ) -> Result<Option<(PageId, usize)>> {
        let leaf_page = match from {
            Some(key) => match self.path_to(key)?.last() {
                Some((page, _)) => *page,
                None => return Ok(None),
            },
            None => match self.leftmost_leaf()? {
                Some(page) => page,
                None => return Ok(None),
            },
        };
        let Some(key) = from else {
            return Ok(Some((leaf_page, 0)));
        };
        let (mut page, mut index) = self.rewind_to_key(leaf_page, key)?;
        if !inclusive {
            // Skip the whole matching run, even across nodes.
            loop {
                let node = self.read_node(page)?;
                let Node::Leaf { entries, next, .. } = node else {
                    return Err(PlomidError::new(
                        ErrorKind::Internal,
                        "scan descent must end at a leaf",
                    ));
                };
                while index < entries.len() && entries[index].key.as_slice() == key {
                    index += 1;
                }
                if index < entries.len() {
                    return Ok(Some((page, index)));
                }
                match next {
                    Some(next_page) => {
                        page = next_page;
                        index = 0;
                    }
                    None => return Ok(Some((page, index))),
                }
            }
        }
        Ok(Some((page, index)))
    }

    /// Collects every entry in the requested window, walking the leaf chain
    /// from the starting leaf without re-descending from the root.
    ///
    /// Same-key chunks merge into single entries with concatenated ascending
    /// RowIds, preserving the historical one-entry-per-key output contract.
    fn collect_range(
        &mut self,
        start: Option<&[u8]>,
        start_inclusive: bool,
        end: Option<&[u8]>,
        end_inclusive: bool,
    ) -> Result<Vec<IndexEntry>> {
        let mut output: Vec<IndexEntry> = Vec::new();
        let mut pending: Option<IndexEntry> = None;
        // Merges each visited entry into the pending output row when keys
        // match, so chunked postings surface as one entry per key.
        let mut emit = |key: Vec<u8>, ids: Vec<RowId>| match pending.as_mut() {
            Some(current) if current.key == key => {
                current.row_ids.extend_from_slice(&ids);
            }
            _ => {
                if let Some(finished) = pending.take() {
                    output.push(finished);
                }
                pending = Some(IndexEntry { key, row_ids: ids });
            }
        };
        let Some((mut leaf_page, mut index)) = self.scan_start(start, start_inclusive)? else {
            return Ok(output);
        };
        loop {
            let node = self.read_node(leaf_page)?;
            let Node::Leaf { entries, next, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "leaf chain must contain leaves only",
                ));
            };
            while index < entries.len() {
                let entry = &entries[index];
                if let Some(bound) = end {
                    let ordering = entry.key.as_slice().cmp(bound);
                    let beyond = if end_inclusive {
                        ordering == std::cmp::Ordering::Greater
                    } else {
                        ordering != std::cmp::Ordering::Less
                    };
                    if beyond {
                        if let Some(finished) = pending.take() {
                            output.push(finished);
                        }
                        return Ok(output);
                    }
                }
                emit(entry.key.clone(), entry.row_ids.clone());
                index += 1;
            }
            match next {
                Some(next_page) => {
                    leaf_page = next_page;
                    index = 0;
                }
                None => {
                    if let Some(finished) = pending.take() {
                        output.push(finished);
                    }
                    return Ok(output);
                }
            }
        }
    }

    /// Full ordered scan of the index.
    pub fn scan_all(&mut self) -> Result<Vec<IndexEntry>> {
        self.collect_range(None, true, None, false)
    }

    /// Ordered scan between bounds; see [`Bound`] for the boundary algebra.
    pub fn range_scan(&mut self, start: Bound, end: Bound) -> Result<Vec<IndexEntry>> {
        let (from, from_inclusive) = match &start {
            Bound::Included(key) => (Some(key.as_slice()), true),
            Bound::Excluded(key) => (Some(key.as_slice()), false),
            Bound::Unbounded => (None, true),
        };
        let (to, to_inclusive) = match &end {
            Bound::Included(key) => (Some(key.as_slice()), true),
            Bound::Excluded(key) => (Some(key.as_slice()), false),
            Bound::Unbounded => (None, false),
        };
        self.collect_range(from, from_inclusive, to, to_inclusive)
    }

    /// First entry whose key is greater than or equal to `key`.
    pub fn lower_bound(&mut self, key: &[u8]) -> Result<Option<IndexEntry>> {
        Ok(self
            .collect_range(Some(key), true, None, false)?
            .into_iter()
            .next())
    }

    /// First entry whose key is strictly greater than `key`.
    pub fn upper_bound(&mut self, key: &[u8]) -> Result<Option<IndexEntry>> {
        Ok(self
            .collect_range(Some(key), false, None, false)?
            .into_iter()
            .next())
    }

    /// Smallest key in the index.
    pub fn first(&mut self) -> Result<Option<IndexEntry>> {
        Ok(self.scan_all()?.into_iter().next())
    }

    /// Largest key in the index, read from the rightmost leaf.
    ///
    /// Merges a trailing same-key run backward so chunked postings report
    /// the complete RowId set, mirroring the forward merge.
    pub fn last(&mut self) -> Result<Option<IndexEntry>> {
        let Some(mut page) = self.rightmost_leaf()? else {
            return Ok(None);
        };
        let mut merged: Option<IndexEntry> = None;
        loop {
            let node = self.read_node(page)?;
            let Node::Leaf { entries, prev, .. } = node else {
                return Ok(None);
            };
            // Walk entries back-to-front, merging one trailing run.
            let mut index = entries.len();
            let mut key: Option<Vec<u8>> = merged.as_ref().map(|entry| entry.key.clone());
            while index > 0 {
                index -= 1;
                let entry = &entries[index];
                match &key {
                    Some(current) if *current == entry.key => {
                        let ids =
                            std::mem::take(&mut merged.as_mut().expect("merged present").row_ids);
                        let mut combined = entry.row_ids.clone();
                        combined.extend(ids);
                        merged.as_mut().expect("merged present").row_ids = combined;
                    }
                    Some(_) => return Ok(merged),
                    None => {
                        key = Some(entry.key.clone());
                        merged = Some(IndexEntry {
                            key: entry.key.clone(),
                            row_ids: entry.row_ids.clone(),
                        });
                    }
                }
            }
            match prev {
                // An empty predecessor ends the walk only when a run is
                // already open... it never opens one: keep walking past
                // empties (they carry no keys to extend the run, but the
                // run may continue beyond them).
                Some(prev_page) => {
                    let prev_node = self.read_node(prev_page)?;
                    let Node::Leaf { entries, .. } = prev_node else {
                        return Ok(merged);
                    };
                    if entries.is_empty() {
                        page = prev_page;
                        continue;
                    }
                    let Some(last) = entries.last() else {
                        page = prev_page;
                        continue;
                    };
                    match &key {
                        // Run continues backward only on equal keys; a lower
                        // key ends it (chain order guarantees nothing earlier
                        // can rejoin it).
                        Some(current) if last.key == *current => {
                            page = prev_page;
                        }
                        Some(_) => return Ok(merged),
                        // No run open yet (rightmost node was empty): adopt
                        // the predecessor's trailing run instead of stopping.
                        None => {
                            page = prev_page;
                        }
                    }
                }
                None => return Ok(merged),
            }
        }
    }

    /// Reverse ordered scan using the `prev` leaf links. The forward scan
    /// remains the default executor path; this serves descending requests.
    /// Same-key chunks merge exactly like the forward scan (in reverse).
    pub fn scan_all_reverse(&mut self) -> Result<Vec<IndexEntry>> {
        let mut reversed: Vec<IndexEntry> = Vec::new();
        let mut pending: Option<IndexEntry> = None;
        let mut emit = |key: Vec<u8>, ids: Vec<RowId>| {
            match pending.as_mut() {
                Some(current) if current.key == key => {
                    // Descending collection: prepend to preserve order.
                    let mut combined = ids;
                    combined.extend(std::mem::take(&mut current.row_ids));
                    current.row_ids = combined;
                }
                _ => {
                    if let Some(finished) = pending.take() {
                        reversed.push(finished);
                    }
                    pending = Some(IndexEntry { key, row_ids: ids });
                }
            }
        };
        let Some(mut leaf_page) = self.rightmost_leaf()? else {
            return Ok(reversed);
        };
        loop {
            let node = self.read_node(leaf_page)?;
            let Node::Leaf { entries, prev, .. } = node else {
                return Err(PlomidError::new(
                    ErrorKind::Internal,
                    "leaf chain must contain leaves only",
                ));
            };
            for entry in entries.iter().rev() {
                emit(entry.key.clone(), entry.row_ids.clone());
            }
            match prev {
                Some(prev_page) => leaf_page = prev_page,
                None => {
                    if let Some(finished) = pending.take() {
                        reversed.push(finished);
                    }
                    return Ok(reversed);
                }
            }
        }
    }

    // -- persistence --------------------------------------------------------

    /// Flushes all dirty pages and fsyncs the index file.
    pub fn sync(&mut self) -> Result<()> {
        self.pool.sync()
    }

    /// Flushes and closes the index, returning the underlying file path.
    pub fn close(mut self) -> Result<PathBuf> {
        self.pool.sync()?;
        Ok(self.path)
    }

    /// Returns structural statistics: entry count, node pages, and depth.
    ///
    /// `node_pages` counts internal nodes plus all leaves reachable through
    /// the leaf chain (the chain is authoritative for leaf coverage).
    pub fn stats(&mut self) -> Result<BTreeStats> {
        let Some(mut page_id) = self.root_id() else {
            return Ok(BTreeStats {
                entry_count: 0,
                node_pages: 0,
                depth: 0,
            });
        };
        let mut depth = 0_u64;
        let mut internal_pages = 0_u64;
        loop {
            depth += 1;
            let node = self.read_node(page_id)?;
            match node {
                Node::Leaf { .. } => break,
                Node::Internal { first, .. } => {
                    internal_pages += 1;
                    page_id = first;
                }
            }
        }
        let mut leaf_pages = 0_u64;
        if let Some(leaf) = self.leftmost_leaf()? {
            let mut cursor = leaf;
            loop {
                leaf_pages += 1;
                let node = self.read_node(cursor)?;
                match node {
                    Node::Leaf { next, .. } => match next {
                        Some(next_page) => cursor = next_page,
                        None => break,
                    },
                    Node::Internal { .. } => {
                        return Err(PlomidError::new(
                            ErrorKind::Corruption,
                            "leaf chain reached an internal node",
                        ))
                    }
                }
            }
        }
        Ok(BTreeStats {
            entry_count: self.meta.entry_count,
            node_pages: internal_pages + leaf_pages,
            depth,
        })
    }
}

// -- WAL recovery integration ----------------------------------------------

/// The index is a WAL replay target: committed `Put` records carry an 8-byte
/// little-endian RowId as their value, and `Delete` records remove the whole
/// key. Replay goes through the existing `plomid_wal` recovery machinery —
/// `plomid_wal::recover_into(path, &mut index)` — with no index-specific WAL
/// format, reader, or recovery loop.
impl plomid_wal::ReplayHandler for BTreeIndex {
    fn on_record(&mut self, _record: &plomid_wal::Record) -> Result<bool> {
        Ok(true)
    }
}

impl plomid_wal::ReplayTarget for BTreeIndex {
    fn apply_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let bytes: [u8; ROW_ID_SIZE] = value.try_into().map_err(|_| {
            PlomidError::new(
                ErrorKind::Corruption,
                "index WAL value must be an 8-byte little-endian RowId",
            )
        })?;
        self.insert(key, RowId::from_le_bytes(bytes))?;
        Ok(())
    }

    fn apply_delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete(key, None, true)?;
        Ok(())
    }

    fn apply_sync(&mut self) -> Result<()> {
        self.sync()
    }
}

// -- Transaction adapter ----------------------------------------------------

/// Transaction adapter: interprets the KV-shaped [`DataStore`] surface over
/// key → RowId entries. A stored value is the 8-byte little-endian RowId of
/// the indexed row; the authoritative row remains in the row/storage layer,
/// and MVCC visibility continues to be applied there by the existing
/// transaction engine.
impl plomid_txn::DataStore for BTreeIndex {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .lookup(key)?
            .and_then(|row_ids| row_ids.first().map(|row| row.to_le_bytes().to_vec())))
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let bytes: [u8; ROW_ID_SIZE] = value.try_into().map_err(|_| {
            PlomidError::new(
                ErrorKind::InvalidArgument,
                "index transaction value must be an 8-byte little-endian RowId",
            )
        })?;
        self.insert(key, RowId::from_le_bytes(bytes))?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.delete(key, None, true)
    }

    fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let from = start
            .map(|key| Bound::Included(key.to_vec()))
            .unwrap_or(Bound::Unbounded);
        let to = end
            .map(|key| Bound::Excluded(key.to_vec()))
            .unwrap_or(Bound::Unbounded);
        Ok(self
            .range_scan(from, to)?
            .into_iter()
            .map(|entry| {
                let value = entry
                    .row_ids
                    .first()
                    .map(|row| row.to_le_bytes().to_vec())
                    .unwrap_or_default();
                (entry.key, value)
            })
            .collect())
    }

    fn sync(&mut self) -> Result<()> {
        BTreeIndex::sync(self)
    }
}
