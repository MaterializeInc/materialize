# envd DDL scalability audit — working notes

Living document. Append to it as the investigation progresses. Keep this
short and load-bearing — anything worth keeping for the long term graduates
to README.md or a code comment.

## Mission

Audit DDL and catalog-transaction code paths in `environmentd` for code
that scales with the number of catalog objects (O(n) or worse), starting
from empirical scaling measurements, narrowing via tracing, and ending
with concrete design proposals for the worst offenders.

Anchor signal (`test/cluster-spec-sheet`, `envd_scalability_mvs`, branch
`envd-ddl-scalability`): with N MVs in the catalog, `CREATE TABLE` p50
grows from 13 ms at N=1 to 88 ms at N=5000 (~6.8×). Peeks stay flat
(~5 ms), so the regression is on the write/catalog path, not the read
path.

## Working agreement

- Drive autonomously to a good conclusion; ask the user when blocked or
  before doing anything irreversible.
- Commit + push as we go — this machine may go down, so unsaved work is
  lost work. Branch: `envd-ddl-scalability`.
- This file is the durable context. Re-read it on session restart.

## Tooling we built

- `test/envd-ddl-scalability/audit.py` — tight standalone harness that
  connects to a running envd, pads the catalog with N objects of one
  type (`tables`, `views`, `mvs`, `indexes`), and times CREATE / DROP /
  ALTER / RENAME of various object types at each scale point. Captures
  trace IDs from `emit_trace_id_notice` for Tempo lookup.
- `test/envd-ddl-scalability/README.md` — how to run; profiling notes.

## Decisions log

- 2026-05-15 — Use a standalone Python script (not mzcompose) so the
  iteration loop is tight: one envd startup, many harness runs. Profiling
  goes through the canonical `bin/environmentd --optimized --monitoring`
  flow.
- 2026-05-15 — Cover all four padding axes (tables / views / mvs /
  indexes) and CREATE/DROP/ALTER/RENAME up front; we don't yet know
  which axis exposes the worst loops.
- 2026-05-15 — Pad MVs/indexes are sharded across `audit_pad_c_<k>`
  clusters with 400 dataflows per cluster, so small replica sizes can
  host pad load without dataflow pressure dominating the catalog signal.

## Findings log

_Empty — to be filled as we collect data._

## Open questions

- Does the scaling pattern differ across padding axes? If `views` (no
  dataflows) shows the same DDL slowdown as `mvs`, the hotspot is purely
  in the catalog/coordinator. If `mvs` is dramatically worse, controller
  state matters too.
- Are CREATE-side and DROP-side regressions caused by the same loops, or
  different ones (dependency walks tend to live on DROP)?

## Next steps

1. Start the local stack and run the tables-padding pass.
2. Record results here, then expand to views / mvs / indexes.
3. Pull traces for the slowest cells; identify dominant spans.
4. Read the source at those spans; find the O(n) / O(n²) loops.
5. Draft design proposals; check in with user before any implementation.
