# 07 — Real workloads and anti-overfitting

Workloads constructed before any tuning; generated data, deterministic shapes
(mod-97 skew + sequential ids), independent hand/Python oracles. Raw:
`artifacts/probes/workloads.log`, `scale100k.log`. Correctness runs and
performance runs share a server but timings are reported separately with
their own provenance; no contaminated comparison is claimed.

- OLTP: single-row insert/point-read/update/delete, batch insert (500–2000/
  stmt), mixed read/write, 8-way hot-key unique race exactly-once, connection
  churn (fresh conn per worker). Uniform + skewed (mod-97) keys. PASS.
- OLAP/HTAP: full/selective scans, COUNT/SUM/AVG, GROUP BY cardinality sweep
  (97 groups), ORDER BY/LIMIT, join-size/skew sweep (3 cust × 4 orders +
  NULL key), concurrent ingestion not run against scans (gap, see below).
  Pruning vs full-scan equivalence NOT proven via plans — result equality
  only. Core queries PASS vs oracles.
- Vector+mixed: exact L2/cosine vs brute oracle PASS; selective filters +
  joins with vectors NOT RUN; writes-concurrent-with-search NOT RUN.
- Durability/maintenance under load: checkpoint/restart + backup/restore
  done; sustained-load WAL growth/amplification NOT measured.
- Anti-overfitting: data shapes (mod-97 groups, sequential keys) differ from
  unit-test fixtures; oracles are independent (hand computation + Python
  brute force), not mirrored from implementation branches. Property/fuzz
  harnesses NOT RUN live this session.
- Gaps: 1M+ rows, concurrent-ingest-during-scan snapshot checks, adversarial
  distributions beyond mod-97 skew — all NOT RUN.
