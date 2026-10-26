# Beta 2 — 17 IP / Provenance / Ownership (Phase 17)

Date (UTC): 2026-10-09. Commit: `7acf160` + working tree.
Scope: engineering evidence only — **not legal advice**. Counsel review
still required before commercial release (see §7).

## 1. Git history scan — single-author tree

```text
$ git log --format='%an|%ae' | sort | uniq -c
245 Sainath Sapa|info.djsai@gmail.com
$ git log --oneline | wc -l
245
```

All 245 commits are by one author (`Sainath Sapa
<info.djsai@gmail.com>`). No external contributors exist in history, so
there is no third-party commit authorship to reconcile. Author map:

| Author | Email | Commits | Role |
|---|---|---|---|
| Sainath Sapa | info.djsai@gmail.com | 245 | sole author / owner (`CODEOWNERS` `@plomid`, `@sainathsapa`) |

## 2. Employment / contractor ownership

Single-owner project (`PLOMID Technologies INDIA`, `NOTICE`). No
contractor or employee commits exist in history, so no assignment
verification is outstanding. If contractors contribute in the future,
written IP assignment must precede merge — tracked as a process gate,
not claimed as done.

## 3. CLA / DCO — NOT configured (gap, owned)

- No CLA action, no `DCO` sign-off requirement, no `Signed-off-by`
  trailer policy in `CONTRIBUTING.md` (verified by text search for
  `DCO|CLA|Signed-off`: no hits outside this paragraph's scope).
- With a single author this blocks nothing today. Recommendation before
  accepting outside contributions: enable DCO (Developer Certificate of
  Origin sign-off) as the lightweight gate; CLA only if counsel requires
  it. Status: YELLOW (process missing, no code impact).

## 4. Third-party assets

- `assets/` (37 files): `plomid-emblem-*/plomid-lockup-*` PNG/SVG brand
  artwork. Owner-attested as original PLOMID artwork; no stock-art,
  font, or icon-package provenance to reconcile. No `*.ttf/*.otf/*.woff*`
  anywhere in the tree (filesystem search, excluding `target/`).
- No vendored third-party source: `grep -rL "PLOMID Technologies INDIA"
  --include="*.rs" crates/` returns only files with
  `//!`-style module docs or the four new files added in Phases 12–15
  (headers added in this phase); a foreign-copyright scan
  (`Copyright.*(Google|Facebook|…)`, excluding PLOMID lines) returns zero
  hits. No copied implementation was found by scan; continued vigilance
  rule from the plan stands (do not paste third-party code to accelerate
  without license review).

## 5. Benchmarks / tests provenance

All qualification harnesses are hand-written in-tree (`crates/gauntlet`,
`benchmarks/harness`, `tests/python`, `tests/*.sql`); no TPC/YCSB or
third-party suite code is vendored. Deterministic datasets use closed-form
generators (`v = id*7+3`), never copied corpora. Status: GREEN.

## 6. Algorithm origins and standards references

| Area | Origin / standard | In-tree note |
|---|---|---|
| ANN (HNSW) | Malkov & Yashunin 2018 (approximate K-NN graph) | `crates/vector` ANN backends; exact fallback authoritative |
| IVF / PQ | Jégou, Douze & Schmid 2011 (product quantization) | IVF/PQ/SQ backends |
| DiskANN | Jayaram et al. (graph-based disk ANN) | DiskANN backend (derived cache only) |
| XorFilter | Graf & Lemire (xor filter) | owned `XorFilter`; Roaring copy removed (Phase: filters fate resolved) |
| BTree / ART | standard structures; ART is derived/cache, BTree durable | `docs/v0.1.0/architecture/index-algorithms.mdx` |
| SCRAM-SHA-256 | RFC 5802 / RFC 7677 | audited `sha2/hmac/pbkdf2/subtle/base64` (Phase: crypto) |
| PG wire | PostgreSQL frontend/backend protocol | `crates/network` |
| MVCC / WAL / ARIES-style replay | standard textbook techniques, clean-room implementation | `docs/v0.1.0/architecture/*` |

## 7. NOTICE automation

`NOTICE` (owner header) + `LICENSE` (Apache-2.0) present at root.
Dependency licenses are machine-enforced, not hand-curated:
`deny.toml` allow-list + `cargo deny check` CI gate + SPDX SBOM
(`dist/artifacts/plomid-0.1.0-beta.2-sbom.spdx.json`, 201 packages).
No separate NOTICE-generation step exists; the SBOM is the
machine-readable third-party record. Status: GREEN (coherent Apache-2.0
tree, zero foreign-copyright hits, license gate enforced).

## 8. Patent counsel review — explicitly not done

No patent search or counsel review has been performed for
ANN/compression/index areas. Per the plan this is pursued only if target
markets justify it. Nothing here constitutes a freedom-to-operate
opinion. Status: UNKNOWN / owned by counsel when scope requires.

## Status

GREEN: single-author history scanned, no foreign code found, Apache-2.0
coherence kept (headers added to the four Phase 12–15 files).
YELLOW: CLA/DCO policy missing (no outside contributors yet — enable DCO
before accepting any). UNKNOWN: patent review (counsel-gated).
