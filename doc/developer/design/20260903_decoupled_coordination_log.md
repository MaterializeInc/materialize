# Decoupled coordination: current handoff

## Current objective

The consensus-read removal goal is implemented and verified. Scans fell 93%, from
122–123/s to 7.9–8.4/s. Explicit upper fetches fell 99%. CPU, PSS and metadata
payload changed little. See the
[report](20260903_decoupled_coordination_observed_progress.md) and
[measurements](20260903_decoupled_coordination_observed_progress.json).

## Next concrete action

Choose the next bounded goal with Aljoscha. Prioritize reproducing the separate
critical-reader birth-ordering risk before removing physical-since checks.
Catalog replica CPU/frontier reductions are the next performance candidate.
Neither investigation is authorized by the completed optimization alone.

## Live limitations

Last broad CI remains non-green. Native still exceeds matched upstream in CPU,
PSS, metadata payload and writes. The birth-ordering concern is source-inspected,
not reproduced. Arbitrary-release compatibility and external-sink handover remain
M3 work. See the [follow-up inventory](20260903_decoupled_coordination_followups.md).

The final debug witness passed both idle windows and two-adapter join/restart/
removal under survivor writes. Controller PubSub delivery was observed directly.
Targeted tests and combined Clippy passed. Physical-readability checks and all
certification/fencing boundaries remain. No owned runtime is running.

## Verified outcomes and remaining limits

Detailed consensus-read checks, intermediate failures, binary hashes, remaining
read attribution and cleanup records are linked by the report above. Raw evidence
is under `~/.local/share/aj/m3-evidence/consensus-read-removal/`. The final run is
`connected/observed-progress-1793966552/`. Earlier sampler and upstream measurements
remain in their reports, not evidence for newer unmeasured code.

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

Owned runtimes are stopped. Pinned idle-measurement binaries and raw evidence
remain. Obsolete shared-plan, full-writer and authority-projection demonstration
binaries were removed, retaining their reports and logs. Temporary controller profiling routes are absent from production source. The unrelated persist-traffic
worktree is untouched. Keep heavy work sequential and low-concurrency.
