# 06 — Real workloads and anti-overfitting

Portfolio run (all with independent oracles; scripts in `artifacts/`):

1. **OLTP-ish**: point lookups, 8-way unique race (exactly-once ×2),
   read-modify-write via PK, rollback discipline. Small scale only.
2. **Analytical**: 100K rows (seeded RNG, NULLs 1/11, skew `g=i%97`,
   low-card text) — `COUNT/SUM/filter/DISTINCT/group-by-97` all exact vs
   Python; selective filter 76 ms, group-by 488 ms (debug, loaded box).
3. **Vector retrieval**: 200 seeded dim-8 vectors; exact/ANN/cosine vs
   oracle; zero-vector, duplicate-adjacent, ties by `(dist,pos)`; restart
   identical. ANN recall 5/5 on oracle top-5 (small-n sanity, not a recall
   benchmark).
4. **Protocol shape**: psql simple + psycopg extended throughout; prepared
   params (`%s` binds incl. vectors); 8 concurrent writers; disconnect-mid-txn
   (kill-9 drill) — pooled-vs-fresh NOT RUN.
5. **Lifecycle**: 100K bulk ingest → restart → backup → restore → verify.
6. **Held-out / anti-overfitting**: cosine-oracle check (I initially
   mis-compared `<=>` to L2 — the engine was right, my first expectation
   wrong); batch-shape sweep (100/500/2000-row INSERTs: linear per-row cost,
   no batch-quadratic); executemany single-row (180 rows/s — per-statement
   overhead dominates, worst-case pattern documented).
7. **Degradation**: cold vs warm cache, memory-limit, slow-disk, WAL-backlog,
   1M+ rows, sustained HTAP with p99 — NOT RUN (bounded; §7/§13).

What PLOMID credibly runs today (tested): single-node OLTP + analytics up
to 100K rows, metadata-filtered vector k-NN at small scale, all with
restart/crash/backup safety. Not demonstrated: 1M+ scale, high-concurrency
sustained mixed load, GPU ANN, distributed anything (none claimed).
