# 13 — Test quality and benchmark integrity

- Public PGWire boundary: exercised live throughout (psycopg extended +
  simple paths, prepared binds, error/Sync recovery, SCRAM/TLS handshakes).
  In-tree `extended_batch`/`rbac`/`tls`/`transactions` suites target the wire
  (ran: extended_batch, rbac).
- Persistent storage + reopen/recovery: covered live (restart, kill -9,
  backup/restore roundtrips). Genuine concurrency: 8-way race + two-session
  catalog probes live. Fragmentation/coalescing/pipelining: codec tests +
  live pipelined/repeat probes (raw-socket live gap noted, not hidden).
- Folded vs non-folded arithmetic + type preservation: covered (§02; OID
  assertions in-tree and live).
- Overfitting risks reviewed: (a) untyped-param division returning TEXT is a
  real restriction tests could mask with explicit casts — flagged REAUDIT-002
  rather than passed silently; (b) no ANN recall measured — exact fallback
  must not be reported as ANN performance (§06); (c) single-sample timings
  reported as samples, no p99/tail claims (§08); (d) index-vs-scan asserted
  by result equality, not plans — marked restriction (§03).
- Benchmarks use the real server + persistent dirs (no test-only paths);
  100K ceiling is measured, 1M+ explicitly NOT RUN.
- Full-suite-passing claims: none made — full suite NOT RUN per operator
  instruction; targeted suites mapped to risks above.
