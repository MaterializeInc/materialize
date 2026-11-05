# Decoupled coordination: current handoff

## Current objective

The fresh idle-resource comparison is complete. Read the
[report](20260903_decoupled_coordination_idle.md) and its
[measured evidence](20260903_decoupled_coordination_idle.json).
Native uses about 2.1× idle CPU and 2.2× PSS versus upstream main. Configuration
controls preserve the gap. Repeated upper polling drives backend read
amplification, frontier reductions are the largest named compute hotspot, and
bounded blob-cache growth explains most of the observed heap growth.

No production architectural repair was made. All comparison fixtures are stopped
and their disposable databases/blobs are removed. Raw evidence and pinned
binaries remain in `~/.local/share/aj/m3-evidence/native-main-idle/`.

## Next concrete action

Agree the next bounded repair with Aljoscha. The smallest measured opportunity
is deduplicating transaction-WAL upper reads within each storage sampling round.
The report also identifies frontier reduction work and repeated structural
reconciliation. Preserve freshness, catalog authorization and replica ownership.
Do not resume sink handover or change settlement policy without agreement.

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

All owned runtime processes are stopped. The temporary controller profiling
routes are absent from production source. Pinned native measurement binaries retain
that instrumentation. The unused upstream worktree and compiler scratch were
removed, leaving about 30 GiB free. The unrelated persist-traffic worktree was
left untouched. Keep heavy work sequential and low-concurrency. Scratch is
optional.
