# Decoupled coordination: current handoff

## Current outcome

The default-debug two-version lifecycle demonstration passed. Both versions
served SQL, DDL and table writes before and after promotion. Promotion restarted
only the candidate adapter, preserving the old SQL session and warmed replica
execution. Relay provisioning transferred without replacing the relay.

Explicit HTTP retirement rejected old-generation writes and removed that
generation's processes and read grants. Surviving process identities, a held read
snapshot and its durable grant remained intact. The newer version continued SQL,
DDL and table writes, and the retired controller could not rejoin.

Both builds use the same implementation and understand the same committed-plan
encoding. No runtime change was needed for this walkthrough. This proves the
prototype lifecycle, not arbitrary-release compatibility or a latency improvement.

Earlier work decoupled retained introspection from raw catalog history through
the durable authority projection, with compaction and cold-restart evidence. The
scoped txn-WAL repair tolerates redundant application after lawful table retirement
and rejects new writes to forgotten targets without partial or collateral commits.
The original four-adapter workload has not been rerun with that repair.

## Next concrete action and remaining work

The next scoped M3 work is an external-sink handover walkthrough across promotion
and explicit retirement, checking one authorized execution and survivor continuity.
Sink handover remains unverified. Do not treat the SQL lifecycle demonstration as
completing M3. Long write/read stalls remain a separate investigation. Other scoped
opportunities are in the
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

- Lifecycle: `~/.local/share/aj/m3-evidence/lifecycle/verification.md` and
  `verified-summary.json`. Passing run: `run-1793821876`, with the exact witness,
  authority and protection snapshots, process/dataflow identities and logs.
  Generation-specific retirement cleanup passed. Final whole-environment cleanup
  required the witness's owned-process fallback, which remains a verification
  limitation. Existing launcher tests pass.
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
Its artifact directory supplies the exact binaries. The lifecycle run reuses the
shared-plan artifacts and transfers relay provisioning to generation 1 before
retiring generation 0, preserving the endpoint. Do not mix artifact sets or convert
the saved exact-build plan environment to the new shared namespace.

Keep heavy work low-concurrency on this 18 GiB VM. Scratch is optional. Stale
incremental compiler caches were pruned, without deleting saved evidence or
binaries.
