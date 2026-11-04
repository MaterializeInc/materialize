# Decoupled coordination: current handoff

## Current outcome

Native M3 deployments now share committed plans under the explicit assumption
that both prototype builds understand the same full encoding. There is no new
compatibility flag or durable setting. Real-build admission, object/dependency
validation, read protection and deployment fencing remain intact.

The default-debug two-version demonstration passed: both adapters joined before
either created its indexed table. Each version read and wrote both tables through
the same SQL sessions. All process identities and replica restart counts remained
unchanged. Selected plans use the shared namespace. This proves propagation, not
arbitrary-release compatibility or a latency improvement.

Earlier work decoupled retained introspection from raw catalog history through
the durable authority projection, with compaction and cold-restart evidence. The
scoped txn-WAL repair tolerates redundant application after lawful table retirement
and rejects new writes to forgotten targets without partial or collateral commits.
The original four-adapter workload has not been rerun with that repair.

## Next concrete action and remaining work

The next M3 walkthrough is promotion followed by explicit retirement, preserving
warmed replicas, surviving deployments and one authorized execution per external
sink. That walkthrough, authority transitions and sink handover remain unverified.
Do not treat the shared-plan demonstration as completing M3. Long write/read stalls
remain a separate investigation. Other scoped opportunities are in the
[follow-up inventory](20260903_decoupled_coordination_followups.md).

Broad CI is not green. [Build141419](https://buildkite.com/materialize/test/builds/141419)
failed on the unpublished `pubsubd` image after Rust compilation. Registry
visibility and upstream merge-skew failures remain unresolved. The prior Persist
`state_inspect_serde_json` object-key-order failure was not repaired or masked.
The full adapter suite's stale
`hydration_times_filters_catalog_protection_before_backpressure` expectation still
requires separate correction after the authority projection. Current targeted
checks pass, not broad CI. The PR stays draft.

## Essential evidence and local state

- Shared plans: `~/.local/share/aj/m3-evidence/shared-plans/verification.md`.
  Passing run: `run-1793800485`. Exact debug artifacts, source patches, setup
  script, test output, before/after process identities and check logs are retained.
  Reusable SQL regression: `test/controllerd/shared-plans.py`.
- Authority projection: `~/.local/share/aj/m3-evidence/authority-projection/replacement/`,
  `run-1793731479/verified-summary.json`. Failed runs and exact artifacts remain
  beside it. The original environment was discarded, not migrated.
- Txn-WAL retirement: `~/.local/share/aj/m3-evidence/txn-retirement/repair.md`.
- Earlier details and worker history: the
  [archive](20260903_decoupled_coordination_log_archive.md).

All demonstration processes and databases are stopped. Each saved run's
`database.json` and `launch.json` identify its private metadata, blobs and ports.
Its artifact directory supplies the exact binaries. Generation 1 shares generation
0's pubsub endpoint. Do not mix artifact sets or convert the saved exact-build
plan environment to the new shared namespace.

Keep heavy work low-concurrency on this 18 GiB VM. Scratch is optional. Stale
incremental compiler caches were pruned, without deleting saved evidence or
binaries.
