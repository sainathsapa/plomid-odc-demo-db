# Beta 2 — 11 Performance Matrix (Phases 14 + 15)

Date (UTC): 2026-10-09. Host: Darwin arm64 (Apple Silicon, dev laptop).
Build: debug unless noted. `rustc 1.99.0`.

## Scale matrix (Phase 14) — measured

`cargo test -p plomid-gauntlet --test scale_matrix -- --include-ignored`
(1 test, 262.9 s wall, GREEN):

```text
RESULT point=1000 load_rps=2207 pk_p50_ms=0.039 pk_p99_ms=0.053 pk_max_ms=0.053 range_rps=1197 count_ms=0.6 bytes_per_row=1126
RESULT point=10000 load_rps=2044 pk_p50_ms=0.042 pk_p99_ms=0.053 pk_max_ms=0.053 range_rps=1087 count_ms=1.5 bytes_per_row=640
RESULT point=100000 load_rps=1667 pk_p50_ms=0.044 pk_p99_ms=0.054 pk_max_ms=0.054 range_rps=944 count_ms=13.4 bytes_per_row=614
RESULT point=300000 load_rps=1608 pk_p50_ms=0.047 pk_p99_ms=0.062 pk_max_ms=0.062 range_rps=938 count_ms=42.0 bytes_per_row=425
```

Gates asserted in-test: exact counts + spot values + range widths at every
point; per-row load ≤5x baseline; PK p99 ≤10x+100 ms; COUNT ≤2x linear+5 s;
absolute 30 s ceiling (`scale_matrix.rs:217-272`). Scaling rule holds on
this host: PK p99 flat 0.053→0.062 ms across 300x rows (no linear
dependence); load_rps degrades 2207→1608 (1.4x for 300x rows); COUNT
0.6→42 ms (~linear scan, expected); bytes/row falls as fixed costs
amortize. 1M/5M/10M not run here (hardware-gated; harness supports any
`POINTS` via env in follow-up; CI default matrix is 1K–300K).

## 1M-request workload (Phase 15) — measured + running

Harness: `crates/gauntlet/tests/million_requests.rs` (ignored gate):
1M mixed requests — 55% point reads, 10% indexed, 10% range, 5% INSERT,
5% UPDATE, 3% DELETE, 7% aggregates/GROUP BY, 2% vector top-k, 2% mixed
txn batches — with p50/p95/p99/p99.9/max, throughput, wall, WAL/data
deltas, RSS, VACUUM+checkpoint, final checksum.

Measured (debug, same host, `PLOMID_MILLION_N=20000` smoke, GREEN, 91.2 s):

```text
MILLION_RESULT completed=20000 failed=0 total=20000 wall_s=91.2 throughput_rps=219
p50_us=45 p95_us=21780 p99_us=49058 p999_us=403245 max_us=681914 count=10800 sum=230391000
```

Point reads dominate p50 (45 µs); aggregates/vector tail drives
p95+ (expected: full-scan `SUM`/GROUP BY + ANN on debug build).
Zero failures; index + vector probes correct after the run.

Full 1M (release, same host) — measured 2026-10-09
(`cargo test --release -p plomid-gauntlet --test million_requests --
--include-ignored`, 5711.9 s wall, GREEN):

```text
MILLION_RESULT completed=1000000 failed=0 total=1000000 wall_s=5703.4 throughput_rps=175
p50_us=10 p95_us=24845 p99_us=57984 p999_us=581571 max_us=1950226 count=50000 sum=5639991000
```

Zero failures across 1M mixed requests; index + vector probes correct
after the run (final checksum `count=50000`). Point reads dominate p50
(10 µs release); aggregates/vector tail drives p95+ (expected: full-scan
`SUM`/GROUP BY + ANN). No 1M-ops/sec claim is made (175 rps sustained on
this single-node debug-host class; the 10K→1M/sec saturation ladder is
explicitly not run).

## Provenance

- Scale: this-machine debug numbers above; release numbers will differ.
- No cloud/CPU-SKU matrix claimed (Apple Silicon only here; x86/ARM
  cloud + RPi + Docker-quota matrix per plan §Phase 14/24 not measured).
- HTAP mixes, OLAP shapes, vector recall benches: gauntlet families
  (`make gauntlet`) unchanged, unasserted at scale — see existing
  `gauntlet_report.md` artifact in CI.

## Status

GREEN (1K–300K asserted matrix + 20K mixed-request smoke, zero failures).
YELLOW: full 1M line pending release run; 1M/5M/10M row scales and
hardware matrix unmeasured (documented, not claimed).
