# Decoupled coordination: current handoff

## Current objective

The fresh resource comparison is complete. Native uses about twice upstream's
CPU and 2.2 times its PSS. Consensus scan counts are close, but writes and blob
traffic remain higher. Caller-side frontier computation is the largest identified
CPU target. See the [report](20260903_decoupled_coordination_resource_review.md)
and [measurements](20260903_decoupled_coordination_resource_review.json).
Persist-internal changes are deferred. No production repair was made.

The replica-scan investigation and broader observer survey are complete. Actual
scan events reconcile with counters on native and matched upstream. Independent
consumers duplicate empty recovery reads, but the catalog helper itself is a
small cost. See the [report](20260903_decoupled_coordination_replica_scans.md) and
[measured attribution](20260903_decoupled_coordination_replica_scans.json).

The preceding consensus-read removal goal is implemented and verified. Scans fell 93%, from
122–123/s to 7.9–8.4/s. Explicit upper fetches fell 99%. CPU, PSS and metadata
payload changed little. See the
[report](20260903_decoupled_coordination_observed_progress.md) and
[measurements](20260903_decoupled_coordination_observed_progress.json).

## Next concrete action

Agree on the next bounded caller-side repair. First inspect frontier index plans
for reusable observation/placement work, preserving their public and retirement
semantics. Sharing the query snapshot WAL reader is a separate concrete ownership
cleanup. Immutable producer-fact reuse is smaller: plan decoding accounts for
only 1.2% of catalog-replica CPU samples. Backend producer batching needs evidence
of fragmentation before changing the existing deduplication/batching path.

## Live limitations

Last broad CI remains non-green. Native still exceeds matched upstream in CPU,
PSS, metadata payload and writes. The birth-ordering concern is source-inspected,
not reproduced. Arbitrary-release compatibility and external-sink handover remain
M3 work. See the [follow-up inventory](20260903_decoupled_coordination_followups.md).

The final debug witness passed both idle windows and two-adapter join/restart/
removal under survivor writes. Controller PubSub delivery was observed directly.
Targeted tests and combined Clippy passed. Physical-readability checks and all
certification/fencing boundaries remain. The new native/main diagnostic fixtures
also passed and stopped cleanly. The latest CPU/heap/scheduling comparison passed.
All owned runtimes are stopped. Remaining caveats are in the resource report.

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

- Resources: `~/.local/share/aj/m3-evidence/native-main-usage/`, runs
  `main-1793988015/` and `native-1793989320/`. Includes source/binary verification,
  raw counters, CPU/heap profiles, scheduling, memory mappings and reviewed source
  notes. The upstream-only setting failure and exact retry are retained separately.
- Replica scans: report above and
  `~/.local/share/aj/m3-evidence/replica-scan-necessity/`. Completed runs are
  `v2/scan-audit-1793974261/` and `main/scan-audit-main-1793979420/`.
  Source audits cover replica and non-replica owners, with the heartbeat-recovery
  correction retained. The initial upstream linker failure, successful cold
  retry and disk guard are recorded under `main/`.
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

Pinned final baselines and raw evidence remain. Obsolete intermediate binaries
were removed with hash inventories. Temporary scan logging and controller
profiling routes are removed from production source, with reproducible patches
under `misc/benchmarks/native-idle/`. The isolated upstream checkout was restored
and removed. The unrelated persist-traffic worktree is untouched. Keep heavy work
sequential and low-concurrency, and stop builds before free disk falls below
30 GiB. Do not accumulate unused incremental/debug artifacts across rebuilds.
