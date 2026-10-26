# 14 — Beta release gates

Every gate needs recorded proof; no green without it.

| Gate | Threshold | Result | Proof |
| ---- | --------- | ------ | ----- |
| Extended multi-statement batches apply fully | batch `(2,30)` both protocols | **PASS** | `artifacts/probe_current.log` |
| Float literals/division exact incl. wire | `3.0/2.0=1.5`, `1/2=0`, debug+release | **PASS** | probes + in-tree tests |
| Commit/rollback/unique-race/concurrency | 8-way exactly-once; rollback absent | **PASS** | probes |
| Restart + kill-9 durability | committed kept / uncommitted dropped / vectors intact | **PASS** | `drill_durability.sh` |
| Cold backup → restore → verify; live refused exit 2 | counts/sentinel present | **PASS** | §5 |
| Vector vs independent oracles | exact/ANN/cosine match; restart identical | **PASS** | probes |
| Table RBAC + wrong-pw + TLS-required | 42501/FATAL behaviors | **PASS** | §8 |
| DATABASE DDL authorized | lowpriv CREATE/DROP denied with side-effect proof | **FAIL (P0-003)** | `rbac_check.py` |
| ILIKE semantics decided | implemented or documented limit | **FAIL (open P2)** | §3 |
| Unsupported set documented | FK/CTAS/CHECKPOINT/DROP ROLE/etc. in docs | **NOT RUN to docs** | code errors verified; docs update unverified |
| 1M release-mode scale | matrix green | **NOT RUN** | §7 |
| Chaos beyond SIGKILL | live matrix green | **NOT RUN** | §9 |
| Disk-full/OOM/conn-exhaustion behavior | drills green | **NOT RUN** | §9 |
| Multi-arch runtime | Linux x64/ARM64 smoke green | **NOT RUN** | §10 |
| Full `make test-ci` on HEAD | green ≤30 min | **NOT RUN here** | §11 (CI scope) |
| `cargo deny check` | exit 0 | **PASS** | this audit |
| Release binary from HEAD builds+runs | exit 0 + smoke | **PASS** | 58.93s build, smoke green |
| SBOM/signing/provenance | pipeline evidence | **PARTIAL** | config-traced, not re-run |

Release rule: local modes shippable with the §00 limits documented in defaults;
shared modes blocked on the two FAIL gates; cloud additionally on every NOT RUN row above.
