

# 26. PHASE 22 — SECURITY CERTIFICATION-READY DESIGN RULES

Design every security-relevant feature so evidence can later be produced.

For each control maintain:

- requirement
- threat
- implementation
- invariant
- test
- negative test
- evidence artifact
- owner
- status

Do not claim:

- ISO certification
- SOC 2 attestation
- Common Criteria certification
- FIPS validation
- DO-178C qualification
- HIPAA compliance
- PCI DSS compliance

from source code alone.

Where certification is targeted, create the evidence bundle and engage the appropriate auditor/evaluator/lab/organizational program.

---

# 27. PHASE 23 — PERFORMANCE ARCHITECTURE RULES

The existing successful performance architecture must be preserved:

- streaming row execution
- bounded analytical memory
- vectorized columnar execution
- zone-map/BRIN pruning
- XorFilter where proven useful
- BTree durable index
- ART derived/cache index
- lazy ART
- HTAP governor
- spill
- result streaming
- hardware dispatch
- exact vector fallback
- ANN as accelerator
- fail-closed freshness

Do not inflate the optimizer into a full cost-based planner until workload evidence proves the need.

Do not enable staged-off Parallel TopK or oversampled ANN merely to make architecture look complete.

Every performance feature must be:

- live
- reachable
- measurable
- fallback-correct
- portable
- memory-bounded
- crash-safe

---

# 28. PHASE 24 — HARDWARE / PORTABILITY QUALIFICATION

PLOMID CPU vector execution currently uses:

- scalar fallback
- x86 AVX2
- x86 AVX-512 where supported
- ARM64 NEON

Do not claim every hardware family is benchmarked.

Create a representative hardware matrix:

```text
Apple Silicon
Raspberry Pi ARM64
AMD x86-64
Intel x86-64
Linux ARM64
Linux x86-64
AWS ARM64 vCPU
AWS x86 vCPU
Docker constrained CPU/memory
```

For each where available:

- build
- startup
- SQL
- OLTP
- OLAP
- HTAP
- vector exact
- ANN
- SIMD backend selection
- scalar fallback
- correctness

GPU remains optional and unimplemented unless demand proves the need.

Do not market the current GPU seam as GPU support.

---

# 29. PHASE 25 — PRODUCTION CANDIDATE QUALIFICATION

PLOMID Beta 2 should not be declared production-ready for remote/cloud/multi-user until all mandatory gates below pass.

## Mandatory blockers

- TLS
- authentication scope
- real authorization/RBAC if multi-user is claimed
- audited crypto
- explicit transaction semantics resolved
- Vector persistence convergence OR formally accepted two-tier architecture with full crash/backup proof
- single lifecycle authority
- filters fate resolved
- sane production defaults
- observability metrics
- 1M/5M/10M scale matrix where hardware permits
- HTAP sustained mixed workload gate
- chaos tests
- backup/restore drill
- upgrade/rollback policy/drill
- SBOM
- artifact signing/attestation
- dependency/advisory process
- full workspace suite
- qualification harness green

## Strongly recommended before remote production

- disk-full chaos
- WAL-full chaos
- fsync-error chaos
- corruption/recovery drill
- restart under load
- long-running soak
- readiness/liveness
- effective-config logging
- operator runbooks
- audit traceability matrix

---

# 30. BETA 2 RELEASE GATE

This program is intended to produce a hardened **PLOMID Beta 2**.

Beta 2 may be released only with an explicit deployment scope.

At minimum record one of:

```text
BETA 2 — LOCAL / EMBEDDED / DEVELOPMENT
BETA 2 — SECURE SINGLE-TENANT REMOTE
BETA 2 — MULTI-USER REMOTE
BETA 2 — CLOUD / SaaS
```

Do not call the product “Production 1.0” if remote security/RBAC/durability gates are still open.

If security blockers are not closed, make the product secure-by-default loopback-only rather than shipping insecure remote defaults.

---

# 31. REQUIRED AUDIT ARTIFACTS

Maintain the existing `/audit/` structure and extend it with:

```text
/audit/beta2/
  00_BASELINE.md
  01_CHANGELOG_SINCE_AUDIT.md
  02_SECURITY.md
  03_AUTHZ_RBAC.md
  04_TLS.md
  05_CRYPTO.md
  06_TXN_SEMANTICS.md
  07_VECTOR_DURABILITY.md
  08_LIFECYCLE_AUTHORITY.md
  09_FILTERS_DECISION.md
  10_OBSERVABILITY.md
  11_PERFORMANCE_MATRIX.md
  12_CHAOS.md
  13_BACKUP_RESTORE.md
  14_UPGRADE_ROLLBACK.md
  15_SUPPLY_CHAIN.md
  16_SBOM.md
  17_IP_PROVENANCE.md
  18_COMPLIANCE_TRACEABILITY.md
  19_EXTENSION_CONTRACT.md
  20_PRODUCTION_GATE.md
  21_FINDINGS_REMAINING.md
  22_EVIDENCE_INDEX.md
  23_BETA2_RELEASE_REPORT.md
```

Every significant claim must point to:

- source file/line
- test
- benchmark
- artifact
- log
- configuration

Never write an unsupported “PASS”.

Use:

- GREEN
- YELLOW
- ORANGE
- RED
- UNKNOWN

---

# 32. MULTI-AGENT RULE

Use multiple agents only when they work on clearly isolated read-only investigation or independent test/evidence tasks.

Recommended split:

### Agent A — Security / auth / TLS

Read/write only security-owned files and tests.

### Agent B — Vector durability convergence

Own only vector/storage/txn integration and related tests.

### Agent C — Transaction semantics

Own only executor/network/txn semantics and regression tests.

### Agent D — Lifecycle / filters / observability

Own ownership decisions and metrics.

### Agent E — Performance/scale qualification

Read-only benchmark execution and evidence generation unless explicitly assigned a scoped implementation.

### Agent F — Documentation / compliance evidence

No production runtime changes.

Agents must not concurrently modify the same core files without coordination.

No agent may silently overwrite another agent's changes.

---

# 33. REQUIRED TEST STRATEGY

## Targeted after each phase

Run only relevant suites during iteration.

Examples:

- network/auth/TLS
- security/RBAC
- txn/concurrency
- vector/recovery
- storage/recovery
- lifecycle
- filters
- observability
- backup/restore

## Final gate

After all production code changes:

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --locked
cargo deny check
cargo test --workspace --locked
```

Then run:

- gauntlet qualification
- release benchmark matrix
- crash/chaos matrix
- backup/restore
- upgrade/rollback drill

Do not change code after a clean final gate without reopening the gate.

---

# 34. PERFORMANCE ACCEPTANCE MODEL

Do NOT promise that PLOMID beats PostgreSQL/Snowflake/ClickHouse/etc. globally.

Use internal engineering gates.

Track:

```text
OLTP:
  throughput
  p50/p95/p99/p99.9

OLAP:
  rows/sec
  bytes/sec
  query latency
  CPU
  memory

HTAP:
  OLTP p99 alone
  OLTP p99 under HTAP
  penalty ratio
  analytical throughput
  CPU efficiency

Vector:
  exact throughput
  ANN throughput
  recall@k
  p99
  index freshness
  rebuild duration

Storage:
  bytes/row
  WAL bytes/op
  index bytes
  maintenance debt
  segment count
```

Every performance regression must identify whether the cause is:

- CPU
- allocation
- lock
- engine mutex
- WAL
- fsync
- storage I/O
- segment routing
- index depth
- page split
- memory pressure
- network/PGWire
- planner/parser
- maintenance

Do not fix blind.

---

# 35. SECURITY ACCEPTANCE MODEL

For remote/multi-user readiness:

- secure transport
- real authentication
- real authorization
- no default credentials
- secure credential rotation
- audited cryptography
- secret redaction
- rate limiting
- input/resource caps
- COPY isolation
- audit events
- admin-operation traceability
- secure Docker defaults
- principle of least privilege

Negative tests are mandatory.

A positive-login test is not sufficient.

---

# 36. FINAL PRODUCTION READINESS DECISION

At the end generate a one-page decision table:

| Area | Status | Evidence | Blocking? | Next action |
|---|---|---|---|---|
| SQL | | | | |
| JSON | | | | |
| Row storage | | | | |
| Columnar | | | | |
| Vector execution | | | | |
| Vector durability | | | | |
| Transactions | | | | |
| MVCC | | | | |
| WAL | | | | |
| Recovery | | | | |
| TLS | | | | |
| AuthN | | | | |
| AuthZ/RBAC | | | | |
| Backup/restore | | | | |
| Observability | | | | |
| Supply chain | | | | |
| IP/provenance | | | | |
| Performance | | | | |
| HTAP | | | | |
| Portability | | | | |
| Auditability | | | | |
| Extension architecture | | | | |

Final decision must be one of:

```text
BETA 2 — LOCAL/DEVELOPMENT READY
BETA 2 — SINGLE-TENANT REMOTE READY
BETA 2 — MULTI-USER REMOTE READY
BETA 2 — CLOUD/SaaS READY
NOT READY — BLOCKERS REMAIN
```

Do not claim a higher level than the evidence supports.

---

# 37. WHAT HAPPENS AFTER BETA 2

Only after foundation gates are closed:

```text
BETA 2 FOUNDATION
        ↓
KV + AI MEMORY
        ↓
GRAPH
        ↓
GEO
        ↓
BLOB / OBJECT
        ↓
DISTRIBUTED / MULTI-REGION
```

Each future model must use `docs/EXTENSION_CONTRACT.md` and pass the same architectural review.

No parallel persistence engines.

---

# 38. CERTIFICATION POSITION

The program is intended to make PLOMID **audit-ready and certifiability-friendly**, not to claim that source code alone creates certification.

For ISO/IEC 27001, 27017, 27018, 27701, Common Criteria, FIPS 140-3, SOC 2, IEC 62443, aviation qualification, HIPAA, PCI DSS or similar programs:

- determine applicability
- identify technical controls
- identify organizational controls
- produce objective evidence
- engage the appropriate auditor/evaluator/lab where required
- do not use “compliant” or “certified” unless the applicable program permits and the required assessment has actually occurred

For aviation:

- ASD-STE100 is a technical-writing/documentation standard
- DO-178C/DO-330 are not something this code audit can self-award
- pursue only if the actual market scope requires them

---

# 39. STOP CONDITIONS

STOP only when:

1. all current source changes are accounted for;
2. Vector uses the canonical durability foundation or a formally accepted, fully proven two-tier architecture;
3. explicit transaction semantics are correct or explicitly narrowed and documented;
4. remote security posture is secure-by-default for the declared deployment scope;
5. authorization is real if multi-user is claimed;
6. crypto is audited rather than hand-rolled;
7. lifecycle authority is singular;
8. filters ownership is resolved;
9. audit-relevant observability exists;
10. scale matrix is measured;
11. chaos/recovery gates are executed;
12. backup/restore/upgrade drills pass;
13. supply-chain/release-integrity evidence is generated;
14. extension contracts are frozen;
15. full workspace tests are green;
16. gauntlet qualification is green;
17. Beta 2 release decision is explicit;
18. remaining blockers are documented and owned.

Final sequence:

**INSPECT → MAP → VERIFY → IMPLEMENT FOUNDATION → SECURITY HARDEN → CONVERGE DURABILITY → FIX TXN SEMANTICS → FREEZE EXTENSION CONTRACTS → MEASURE SCALE → CHAOS TEST → BACKUP/RESTORE → SUPPLY-CHAIN EVIDENCE → FULL TEST → GAUNTLET → RELEASE DECISION → REPORT → STOP**

---

# 40. FINAL PRINCIPLE

PLOMID should become one database, not a collection of databases sharing a SQL parser.

Its strength should come from one coherent foundation:

```text
                   PLOMID
                      |
        +-------------+-------------+
        |             |             |
       SQL           JSON          Vector
        |             |             |
        +-------------+-------------+
                      |
                  Executor
                      |
              Transaction / MVCC
                      |
                WAL / Storage
                      |
         +------------+------------+
         |            |            |
        ROW        COLUMNAR      indexes
         |            |            |
         +------------+------------+
                      |
            common recovery/backup
                      |
            common security model
                      |
            common observability
                      |
             common audit evidence
```

Every future capability must strengthen this foundation rather than fork it.

**No parallel storage. No parallel WAL. No parallel MVCC. No hidden background owner. No unmeasured “performance optimization.” No unsupported certification claim.**

Build Beta 2 as the clean foundation on which the rest of PLOMID can safely grow.
