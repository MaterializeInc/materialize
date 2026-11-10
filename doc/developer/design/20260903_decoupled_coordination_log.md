# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Performance changes remain
separate. Do not change Persist or the 60s publication default.

## Next concrete action

Finish the native index-protection runtime, then repeat with a larger stable
cohort. The uncommitted conversion checks both candidate replicas and live catalog
bounds, using the existing publication-pause flag. Independent review caught and
closed a missing initial-pause assertion. See `native-index-review.md` and
`native-index-runtime.log`. No runtime acceptance is claimed yet.

After the index work, implement the approved singleton CDC-reader ownership repair
and complete the uncommitted native `workflow_ddl` conversion. The catalog chooses
the desired reader and replica followers converge. Promotion transfers singleton
readers alongside sink authority while both deployments remain SQL/table writers.
Candidate compute warms from shared Persist source data. Temporary overlap is
allowed, but superseded readers must eventually stop reconnecting. Use existing
connector takeover/recovery, without stop acknowledgements, leases or a new
coordination protocol. Verify continued ingestion and eventual cessation of
contention, not zero overlap or uninterrupted upstream connections.
`native-cdc-reader-ownership.md` contains the matched actors and source evidence.

Helper checks, preservation comparison and independent review pass. Partial runtime
checks passed candidate/peer writes and DDL, but full lifecycle acceptance remains
unverified. OOMs, a telemetry soft panic and an unattributed five-second FETCH
failure are recorded in `native-ddl-runtime-findings.md`. The latest full fixture
was stopped because the known source conflict prevents healthy-upgrade proof.
Its `interrupted-cleanup.json` records verified removal of the owned project.

Parallel-workload's coexistence wiring is committed with helper checks but no
stress rerun. Zippy's native actions passed with continuing ingestion in both
storage topologies. Full randomized zippy and runtime restore remain unrun.

The same-build and bumped-version platform scenarios now share their relay and
explicitly retire predecessors before cleanup. Helper checks preserve check phases,
version-bump steps and historical-image behavior. The unchanged `CreateTable`
check passed the same-build action sequence across four generations, including
three retirements and relay continuity. Full platform and bumped-version runtime
acceptance remain unverified. See `native-platform-runtime-verification.md`,
`native-platform-verification.md` and `native-bumped-platform-verification.md`.
Other platform scenarios remain to be converted.

The managed MV snapshot-pruning workflow passes after waiting for candidate
execution readiness. Its original SQL, correction-buffer assertions and pruning
deadline remain intact. See `native-pruning-verification.md`.

Remaining 0dt work includes
read-only/fencing expectations, forced-migration cases and temporary-item cleanup.
Preserve the source/sink workloads, in-place predecessors and compatible-image
lane. Then convert remaining platform/stress callers. Keep the old promotion
cleanup parameter until callers migrate. `native-lifecycle-harness-seams.md` is
an inventory, not current status. Its PG CDC reporting gap is already fixed.

Two scope decisions remain pending:

- Replica-side catalog-free ingress is still present. Proposed replacement for
  the headless driver's programmable coverage is a test-only native runtime.
  Keep shared query transport, native nonleader workers and both Timely topologies.
  See `native-replica-execution-boundary.md`. No implementation is claimed.
- The dormant final-preflight hook supports manually configured old-to-old runs.
  Its proposed explicit retirement is not yet approved. Leave it unchanged.
  See `final-preflight-boundary-research.md`.

Temporary-item ownership is approved for a separate repair after CDC: retain the
session UUID for visibility and commit the existing adapter execution incarnation
with each Item. Keep disconnect cleanup, reclaim only catalog-confirmed replaced
or retired executions, and preserve items on promotion. No per-connection catalog
records or correctness dependency on `mz_sessions`. Details are in the follow-up
inventory. The implementation and temporary-item lifecycle conversion remain open.

The user approved explicit unsupported reporting for pre-native-to-native
upgrades, not conversion/reset workarounds. Preserve old-only tests and separate
compatible native coexistence coverage.

## Verified cleanup outcome

Adapter provisioning and controller-owned storage/compute execution are removed.
Controllerd owns processes and replica followers own maintained execution.
Adapters retain policy, validated catalog transactions, serving-timeline protection
and transient query work. Combined/read-only serving, private migration execution,
alternate protection catalogs and exclusive age-based process cleanup are removed.
Required formats and native introspection publishers/consumers remain.

Targeted debug checks cover native startup/restart, readiness, durable protection,
SQL query operations, Kafka ALTER SINK, retained writers and sessions across
promotion, explicit retirement and relay continuity. Native harness checks also
cover warmed adapter recovery and peer DDL visibility without restart. These are
local same-build results, not broad CI or a fresh distinct-version demonstration.

## Live limitations

Broad CI is not green. Last reviewed broad evidence:
[Build141419](https://buildkite.com/materialize/test/builds/141419), including the
unpublished pubsubd image gate. Registry visibility, merge skew, a Persist JSON-order
check and stale hydration expectations remain unresolved. Full formatting last
failed on an unchanged generic call in `src/catalog/src/catalog.rs`. Changed-file
formatting and scoped combined all-target Rust checks pass. The PR stays draft.

The current-source preservation, upsert-stress, data-ingest and parallel-workload
container/stress workflows have not been rerun after their lifecycle conversion. Broad native container/upgrade
coverage remains outstanding. Use matching post-cleanup builtin schemas for the
next version demonstration. Arbitrary-release compatibility, external-sink handover
and matched performance acceptance remain unverified.

A source-status query attempt took 673s in a capped debug fixture. Its cause is
untraced. Do not assume resource limits explain it or call it responsive SQL.
The query-client startup activation race and birth-ordering concern remain
source-inspected, not reproduced/fixed. The original four-adapter workload has not
been rerun with the txn-WAL retirement repair. These and the temporary-item gap
are recorded in the [follow-up inventory](20260903_decoupled_coordination_followups.md).

## Essential evidence and local state

Evidence root: `~/.local/share/aj/m3-evidence/native-only/`.

- `native-lifecycle-slice-verification.md`, `native-source-lifecycle-verification.md`,
  `native-readiness-verification.md` and `native-id-pool-verification.md` link actual
  workflows, provenance, failures, reviews and cleanup. Cached images mounted current
  debug binaries. The restart test establishes warmup rather than assuming it from
  a fixed sleep. Its recovery bound is unchanged.
- `native-ddl-review.md`, `native-ddl-helper-tests.log` and
  `native-ddl-preserved-workload.txt` record current DDL checks. Runtime evidence
  remains provisional under `native-ddl-latest`.
- `native-zippy-verification.md` and `native-zippy-review.md` cover standalone
  four-worker recovery, managed multi-process promotion/retirement and continuing
  load-generator ingestion. Backend and full-workload limitations remain explicit.
- `native-current-source-verification.md`, `native-upsert-lifecycle-verification.md`
  `native-data-ingest-verification.md` and `native-parallel-lifecycle-verification.md`
  distinguish checked harness contracts
  from unexecuted container/stress coverage.
- `native-compute-owner-verification.md` records scoped Rust checks, native tests,
  binary hashes and Kafka execution. The four compute introspection inputs remain
  intentionally, as explained in `native-compute-introspection-boundary.md`.
- `native-storage-owner-verification.md`, `query-copy-verification.md`,
  `private-migration-verification.md`, `adapter-serving-mode-verification.md`,
  `alternate-protection-verification.md`, `native-bootstrap-verification.md`,
  `adapter-actuator-verification.md` and `exclusive-cleanup-review.md` cover earlier
  removals. `container-runtime/verification.md` and the boundary-reporting reports
  preserve container and unsupported-upgrade evidence.

Two index-protection attempts timed out on the first catalog dump after 30 seconds
and cleaned up. A diagnostic dump returned 117 MB in 28.4 seconds. Oracle task 240
is reviewing compact authority observations without weakening coverage. The
diagnostic was interrupted after exceeding the paused-startup budget. Its owned
resources were removed and no runtime remains active. See
`native-index-dump-investigation.md`. Follow `native-index-latest`
and `native-index-runtime.log`, using `run-native-index-protection.py` in the
evidence root. Previous owned fixtures were removed. About 41 GiB is free. Scratch SSO
was expired on two checks. Local fixture guards enforce backend/OOM checks,
2 GiB host available memory and 30 GiB disk reserve. Keep heavy work sequential
and use one low-priority Cargo job.
Current service binaries contain the compute-owner removal. Retain baseline
artifacts, measurement reports and third-party cache. Superseded build cleanup is
recorded in `native-compute-*-cleanup.log` and `native-compute-reserve-reclaim.log`.
Leave the unrelated added `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
and persist-traffic worktree untouched.

Earlier compatible-version proof is in `lifecycle/verification.md` under the evidence
parent. It predates this cleanup. The [publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `logical-component-load/` remain the load baseline, not a cleanup performance
claim. Further history is linked from the [archive](20260903_decoupled_coordination_log_archive.md).
