# Decoupled coordination: current handoff

## Current objective

The cached storage-frontier repair is complete. Native publication now reuses
`storage_collections` upper tracking instead of polling each collection. Exact
bindings, catalog read bounds and cooperative monotonic publication are preserved.
The [report](20260903_decoupled_coordination_cached_frontiers.md) and
[measurements](20260903_decoupled_coordination_cached_frontiers.json) show about
106 fewer consensus scans/s, a 46% deployment-wide reduction. Total CPU and RAM
show no clear improvement in these windows. Debug join/restart/removal checks,
storage tests and targeted Clippy passed.

The broader [idle comparison](20260903_decoupled_coordination_idle.md) remains
valid for its recorded pre-repair build. All owned runtime processes are stopped.

## Next concrete action

Agree the next bounded task with Aljoscha. Remaining polling is concentrated in
replica reconciliation and other adapter/controller refreshes. Frontier reductions
remain the largest named compute hotspot. No further architectural repair is
underway. Preserve freshness, catalog authorization and replica ownership.

External-sink handover across promotion and explicit retirement remains M3 work.
It must verify one authorized execution and survivor continuity. Long write/read
stalls remain separate. Other opportunities are in the
[follow-up inventory](20260903_decoupled_coordination_followups.md).

## Verified outcomes and remaining limits

The debug two-version prototype lifecycle passed: SQL, DDL and table writes
before/after promotion, surviving old SQL sessions and warmed replicas, and
explicit retirement fencing/cleanup without losing the survivor's held snapshot.
Both builds used the same implementation and plan encoding. This is not proof of
arbitrary-release compatibility or completion of M3.

The durable authority projection decoupled retained introspection from raw
catalog history, with compaction and restart evidence. The scoped txn-WAL repair
handles lawful concurrent table retirement without partial or collateral writes.
The original four-adapter workload has not been rerun with that repair.

Last broad CI evidence remains non-green:
[Build141419](https://buildkite.com/materialize/test/builds/141419) failed on the
unpublished `pubsubd` image after Rust compilation. Registry visibility and
upstream merge-skew failures remain unresolved in this handoff. The prior
Persist `state_inspect_serde_json` object-key-order failure and the adapter
suite's stale `hydration_times_filters_catalog_protection_before_backpressure`
expectation were not repaired here. This experiment does not establish green CI.
The PR stays draft.

## Essential evidence and local state

- Cached frontier repair: report above and
  `~/.local/share/aj/m3-evidence/cached-storage-frontiers/`. Fresh and restarted
  lifecycle checks passed. The measured candidate's idle windows passed, but its
  first post-window diagnostic failed on harness privileges. The report preserves
  that failure, its reproduction and the corrected checks.
- Idle resources: report above, raw evidence under
  `~/.local/share/aj/m3-evidence/native-main-idle/`, and reusable scripts under
  `misc/benchmarks/native-idle/`. The original upstream cleanup assertion and
  invalid early scheduling probes remain recorded. Corrected diagnostic fixtures
  and native/matched-main cleanup passed.
- Lifecycle: `~/.local/share/aj/m3-evidence/lifecycle/verification.md` and
  `verified-summary.json`, passing `run-1793821876`. Final whole-environment stop
  needed the witness's owned-process fallback. Generation retirement passed.
- Shared plans: `~/.local/share/aj/m3-evidence/shared-plans/verification.md`,
  passing `run-1793800485`. SQL regression: `test/controllerd/shared-plans.py`.
- Authority projection:
  `~/.local/share/aj/m3-evidence/authority-projection/replacement/`,
  `run-1793731479/verified-summary.json`. The old environment was discarded.
- Txn-WAL retirement: `~/.local/share/aj/m3-evidence/txn-retirement/repair.md`.
- Earlier details: [archive](20260903_decoupled_coordination_log_archive.md).

Owned runtimes are stopped and their disposable state is removed. Pinned
measurement binaries and raw evidence remain. Temporary controller profiling
routes are absent from production source. Compiler scratch was removed, leaving
about 16 GiB free. The unrelated persist-traffic worktree is untouched. Keep
heavy work sequential and low-concurrency.
