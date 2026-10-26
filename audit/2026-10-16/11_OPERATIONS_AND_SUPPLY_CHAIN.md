# 11 — Operations, maintenance, supply chain

Evidence: `artifacts/probes/logs/backup_crash_probe.log`,
`server_00.log` maintenance lines, `04`/`05`/`08`.

## Operator workflows verified this cycle

- **Backup**: cold backup + manifest inspect + restore-to-clean-dir +
  verification (relational, vector, index state) all PASS; hot backup
  is explicitly refused while the data dir is locked (deliberate stop
  requirement).
- **Crash**: `kill -9` recovery clean; post-crash writes work.
- **Torn WAL tail**: server refuses to start with a framed error —
  operator must intervene (restore from backup/checkpoint flow); this is
  honest fail-closed behavior, and the runbook should state the
  recovery procedure explicitly (currently implicit).
- **Restart**: index state `READY` after restart; ANN warm-up cost as
  in `06` (DiskANN rebuilds from rows even with checkpoints present).

## Maintenance / freshness

- Background maintenance passes are observable
  (`sql::maintenance: background pass complete … published_fresh` with
  per-pass timings) and drive columnar freshness accounting surfaced in
  `plomid_stat_tables` (writes / is_stale).
- **Ops gap (F-002/F-003)**: over SQL there is *no way to force* stats
  collection (`ANALYZE` no-op) or graph/cache invalidation (`REINDEX`
  no-op) on a running server. For Beta operations this means: after
  large loads, statistics never refresh through the documented operator
  command. Until fixed, treat `ANALYZE`/`REINDEX` as non-operational and
  document that.
- Cost note for capacity planning: DiskANN index storage lives under
  `data/vector_ann/<db>/<table>/<col>/` (`graph.dat` + `vectors.dat`;
  ~5 ms/vector build, ~366 KB for 8 k×64) and rebuilds from snapshot
  rows on cold start.
- Connection pooling is required in front of large catalogs (F-006,
  ~5 s/connection).

## Supply chain

- `cargo-deny 0.20.2` present; licence/advisory gate **NOT RUN** this
  cycle (workspace chain interrupted before reaching it — see `12`;
  no `deny.txt` exists).
- Release build reproducibility gate: the `--locked` re-run did **not**
  complete (same interruption). The audited binary was built from this
  tree earlier in the cycle and sha-256-verified
  (`8b0a6b60…972f`); all probe evidence was produced against it. A
  fresh `--locked` build remains an open evidence item.
- SBOM/SLSA attestation, Docker image check, dependency count audit:
  **NOT RUN** this cycle (G-08 unchanged; Beta scope should state this
  explicitly).
- Benchmark/probe scripts ran locally; no product source was modified
  (tree clean at report time; audit artifacts committed separately —
  see `README` and the final commit).
