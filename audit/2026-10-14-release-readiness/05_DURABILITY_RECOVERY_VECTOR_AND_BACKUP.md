# 05 — Durability, recovery, vector and backup

Commit contract (code): WAL-prefix durability — `commit_prepare` appends the
WAL batch, `commit_publish` installs MVCC then crosses the group barrier;
no per-commit data fsync (PG-like). Checkpoint order: flush+fsync data →
write+fsync record → publish boundary → final sync. One `recover_storage`
pipeline on open (crash-tail truncate, dangling repair, orphan sweep, WAL
replay). GC reachability from newest catalog + reader leases; stale-pointer
guard aborts lagging passes leak-tolerant.

## Live drills (this audit, `artifacts/drill_durability.sh`)

- Graceful SIGTERM → restart: sentinel row present. PASS.
- `kill -9` with open uncommitted txn: committed `id=1` present,
  uncommitted `id=2` absent; vector table 201/201 intact. PASS.
- Cold backup (`backup --data --to`): 52 files, manifest format 1. PASS.
- `backup-inspect`: versions + file/byte counts. PASS.
- `restore --from --to` clean dir → server starts → counts/sentinel/batch
  rows all present. PASS.
- Backup against live (locked) dir: refused, **exit 2**. PASS.
- No hot `BACKUP` SQL by design (only offline CLI callers) — P1-004
  re-scoped: cold path verified; hot backup/PITR explicitly absent.

## Vector convergence (founder hypothesis: "Vector complete?")

Mostly yes for the converged design, no for every knob. Live-verified:
exact L2 / `<->` / cosine `<=>` / HNSW ANN all match independent Python
oracles (200 seeded dim-8 vectors); zero-vectors; txn rollback; post-crash
k-NN identical; 100K-scale exact group-by parity in §6. Design (traced):
vector payloads are **ordinary row values** under txn/MVCC/WAL/generations —
no vector sidecar authority; ANN graphs are derived caches keyed by
watermark+count, rebuilt on miss, excluded from backup (`txn/backup.rs:47-48`)
and rebuilt after restore/GC/VACUUM; MVCC enforced by snapshot-materialized
rows with pinned-RR ANN decline and exact rerank/verify; SIMD AVX2/NEON +
scalar fallback live (AVX-512/SVE execute narrower kernels, labelled).
Restrictions (live-or-traced): GPU is a stub (CPU fallback); FLAT/BINARY/
multi-vector ANN parse but serve exact; filtered/DESC/pinned/small/`k=0`
shapes stay exact; `CREATE VECTOR INDEX` refused in explicit txn; dims
1..65536, payload 64 MiB cap, `k ≤ 1M`. Legacy `vector::{storage,mvcc,backup}`
modules are test-only non-authoritative scaffolding (headers say so).

## NOT RUN

Torn-WAL / partial-checkpoint / missing-file / disk-full / permission-denied
live matrix (no fault-injection CLI; unit fail-point tests cited §11);
large-WAL restart timing; PITR (doesn't exist).
