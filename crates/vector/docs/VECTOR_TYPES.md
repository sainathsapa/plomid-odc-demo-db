# Vector Types (`plomid-vector`)

User-facing reference for vector columns, literals, queries, and index maintenance.

## Types

| Type | Meaning | Storage |
|---|---|---|
| `VECTOR(N)` dense | Fixed `N`-dim embedding | Standalone codec bytes (magic + version + CRC framing) |
| `BVECTOR(B)` binary | `B`-bit packed vector, MSB-first, zero-padded | Standalone codec bytes, `ceil(B/8)` payload |
| `SPARSEVECTOR(N)` | Sorted `(index, value)` pairs over `N` dims, zeros omitted | Standalone codec bytes |
| Multi-vector | Up to `max_vectors` dense constituents per row | Standalone codec bytes |

`VectorType` / `VectorValue` in `plomid-vector` are the canonical
in-memory forms. Row bytes use the self-describing standalone codec
(`codec::encode_standalone`); adapters enforce dense dims when
constructed with `Some(dims)` and return `DimensionMismatch` otherwise.

## Literals

Native dense literals are bare float lists (whitespace, signs, exponents
allowed; empty and non-finite rejected with `VECTOR_INVALID_LITERAL`):

```sql
CREATE TABLE docs (id BIGINT, embedding VECTOR(FLOAT32, 3));
INSERT INTO docs VALUES (1, [1.0, 2.0, 3.0]);
UPDATE docs SET embedding = [0, 0, 1] WHERE id = 1;
SELECT [1.0, 2.0, 3.0];
```

Quoted strings still coerce, and explicit casts keep working:

```sql
SELECT '[1.0, 2.0, 3.0]'::VECTOR(3);
SELECT '101011'::BVECTOR(6);            -- bit string, MSB first at ingest
SELECT '{0:1.5, 9:0.2}'::SPARSEVECTOR(128);
```

## Functions

| Function | Returns | Notes |
|---|---|---|
| `vec_dims(v)` | `INT` | Logical dims of the value |
| `l2_distance(a, b)` | `FLOAT` | Squared Euclidean |
| `cosine_distance(a, b)` | `FLOAT` | `1 - cos`; `1.0` on zero vectors |
| `inner_product(a, b)` | `FLOAT` | Larger = closer |
| `topk_exact(q, k)` | rows | Brute-force, always correct |
| `ann_search(q, k [, ef])` | rows | Index-assisted; CPU falls back to exact |

## Operators

`<->` L2, `<=>` cosine, `<#>` inner product, e.g.:

```sql
SELECT id FROM docs ORDER BY embedding <-> '[1,0,0]'::VECTOR(3) LIMIT 10;
```

## Indexes

- **HNSW / IVF / PQ / SQ / DiskANN**: built via
  `VectorComputeBackend::build_index`; CPU validates and returns an
  `IndexHandle`, GPU stub returns `Unsupported` until linked.
- **Binary / sparse / multi-vector / hybrid fusion**: exact-first;
  ANN is an acceleration, not a semantic change.

## Examples

```sql
CREATE TABLE docs (id BIGINT, embedding VECTOR(128));
INSERT INTO docs VALUES (1, [0.1, ...]);
SELECT id FROM docs ORDER BY embedding <-> :q LIMIT 5;   -- exact
SELECT id FROM docs_ann(:q, 5, ef := 64);                -- ANN
```

## Limits

- `dims > 0`, `base.len() % dims == 0`, else `InvalidArgument`.
- Dense encode max: `u32` dims; binary bits: `u32`; pages: 8 KiB
  (`PAGE_MAX_PAYLOAD = 8160`), header 32 B (`PLMV`, version `u16 LE`).
- Manifest idents ≤ 1024 B; unknown magic/version/tag → `Corrupt`.
- `topk_exact(k = 0)` returns empty; `k > n` clamps to `n`.

## Exact vs ANN

- **Exact** (`topk_exact`, `distance_batch`): deterministic recall 1.0.
  Cost `O(n·dims)`. Use for eval, small tables, correctness gates.
- **ANN** (`ann_search`): same signature plus probe budget `ef`.
  CPU implementation delegates to exact; GPU/index builds may
  approximate. Always re-verify SLAs with exact on a sample.

## Monitoring
- `Snapshot { lsn, row_count }` (`PLMS`, 24 B LE) marks read watermarks.
- `VectorManifest` carries `checkpoint_lsn`, `format_version`,
  `table`/`column` identity, `dims`, `row_count`.
- Visibility: `is_visible(creator, watermark, active_set)`;
  version form adds `deleted_lsn`. A row deleted at/before the
  snapshot by a committed txn is invisible; in-flight deleters keep
  the row visible.
- Deltas: `CommittedDelta::{latest_op, is_deleted, tombstone_count,
  max_lsn, truncate_through}`; `PendingWrites::{insert, delete,
  rollback, commit_into}`.

## Rebuild

`maintenance::{fragmentation_metric_full, tombstone_ratio,
should_cleanup_tombstones, should_rebuild, advise}` with defaults:

- cleanup at tombstone ratio ≥ 0.20 or count ≥ 10 000;
- rebuild at fragmentation ≥ 0.35 and rows ≥ 1 000.

```rust
match advise(total, live, tombstones, &MaintenanceConfig::default()) {
    MaintenanceAdvice::Healthy => {},
    MaintenanceAdvice::CleanupTombstones { .. } => vacuum(),
    MaintenanceAdvice::RebuildIndex { .. } => reindex(),
}
```

Clean tombstones (vacuum) before rebuilding; then checkpoint the
manifest (`checkpoint_lsn = delta.max_lsn()`) and truncate the delta
log through it.

## Capability matrix

Single source of truth: `capability::capability` in `plomid-vector`
(`SUPPORTED` / `SUPPORTED_WITH_CONVERSION` / `UNSUPPORTED` for every
kind × element × metric × index; 1120 combinations, tested total).
DDL, the planner, and EXPLAIN all consult it.

| Index | Serves |
|---|---|
| `Flat` (exact oracle) | every supported kind/metric pair |
| `HNSW`, `IVF_FLAT`, `DISKANN` | dense/multi numeric (`FLOAT32/16`, `BFLOAT16`, `INT8/UINT8`, `INT4`) × `L2/Cosine/Dot` |
| `IVF_SQ` | above floats + `INT8/UINT8` natively; `INT4` with float promotion |
| `IVF_PQ` | `FLOAT32/16`, `BFLOAT16` natively; `INT8/UINT8` with float promotion; no `INT4`/binary |
| `Sparse inverted` | sparse × `L2/Cosine/Dot` (any non-`BINARY` element) |
| `Binary` | binary × `Hamming/Jaccard` only |

`SUPPORTED_WITH_CONVERSION` combinations quantize via documented float
promotion at build time; `UNSUPPORTED` combinations fail `CREATE INDEX`
with `VECTOR_INDEX_UNSUPPORTED`, never silently.
