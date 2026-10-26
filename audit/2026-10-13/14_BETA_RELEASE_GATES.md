# 14 — Beta release gates

Objective thresholds. No gate is green without recorded proof. Commands are the
project-prescribed ones (`Makefile`/CI).

## Correctness / durability (must all PASS for any GO)

| Gate | Threshold | Result |
|---|---|---|
| G-C1 extended-protocol batch integrity | psycopg `BEGIN;INSERT;COMMIT` batch commits; status `COMMIT`; no stuck txn | **FAIL (P0-001)** — `probe17` |
| G-C2 float arithmetic over PGWire | division matrix incl. `3.0/2.0=1.5`, `1.5/2=0.75`, `pg_typeof(1.0)≠int4` | **FAIL (P0-002)** — `probe11/12b` |
| G-C3 txn semantics (separate statements) | commit/rollback/mid-txn-reads/savepoints/abort-behavior per `04` | **PASS** — `probe2` |
| G-C4 concurrent conflicting load | 8-way inserts 100% + unique race exactly-once | **PASS** — `probe5` |
| G-C5 graceful restart fidelity | row/vector/k-NN identical | **PASS** — `probe6` |
| G-C6 crash (`kill -9`) fidelity | committed kept, uncommitted dropped, replay logged | **PASS** — `probe7` |
| G-C7 catalog visibility | DDL visible on live conns ≤1 statement (or versioned error) | **FAIL (P1-003)** — `probe9` |
| G-C8 vector exact correctness | 3-metric k-NN + ties/zero/dup + rollback + restart | **PASS** — `probe3/6` |
| G-C9 workspace suite green | `cargo test --workspace --lib --tests` exit 0 | **NOT RUN (G-001)** |
| G-C10 lint/format/policy | `fmt-check` + `clippy-ci` + `deny` exit 0 | **PARTIAL** (fmt+deny pass; clippy NOT RUN) |

## Security / deployment

| Gate | Threshold | Result |
|---|---|---|
| G-S1 TLS live | handshake OK + plaintext refused on required + non-loopback guard | **PASS** — `probe14` |
| G-S2 authN/RBAC live | wrong-pw FATAL; deny/allow + 42501; COPY denied | **PASS** — `probe8/8b` |
| G-S3 caps/timeouts enforced | defaults in startup dump; over-limit behavior | **PASS by code+defaults** (adversarial NOT RUN) |

## Performance / scale

| Gate | Threshold | Result |
|---|---|---|
| G-P1 100K reference | bulk ≤10 s; filter ≤50 ms; group-by ≤2 s; count ≤2 s | **PASS** (2.3 s / 2 ms / 110 ms / 70 ms) |
| G-P2 1M matrix + p95/p99 | thresholds TBD after first measurement | **NOT RUN (G-004)** |
| G-P3 ANN recall@k | recall vs exact at claimed scale | **NOT RUN (G-008)** |

## Operations / supply chain

| Gate | Threshold | Result |
|---|---|---|
| G-O1 backup + restore drill | cold backup during writes → clean-dir restore → identical results | **FAIL-open/MISSING (P1-004)** |
| G-O2 chaos beyond SIGKILL | torn-WAL/checksum/missing-file matrix green | **NOT RUN (G-005)** |
| G-O3 pressure behavior | disk-full/OOM runs show clean errors + recovery | **NOT RUN (G-005)** |
| G-R1 SBOM + provenance | SPDX ships; images signed/attested | **PARTIAL** (SPDX yes; signing no) |
| G-R2 multi-arch runtime | login→CRUD→restart on linux x86_64 + ARM64 | **NOT RUN (G-006)** |

## Gate-to-verdict mapping

Any GO requires G-C1…C8 + G-S1…S2 green. Today G-C1, G-C2 fail → **NO-GO all
modes**. G-C7 + G-O1 fail → remote modes additionally blocked even after P0s.
G-C9/C10/P2/P3/O2/O3/R1/R2 gate the step from limited-local to wider releases.
