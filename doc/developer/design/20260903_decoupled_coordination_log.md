# Decoupled coordination: current handoff

## Current objective

Make the branch native-only while preserving adapter policy, validated catalog
transactions, shared query/table machinery, durable protection and compatible
multi-version writers with explicit retirement. Performance changes remain
separate. Do not change Persist or the 60s publication default.

## Next concrete action

Continue native lifecycle-harness conversion. Remaining 0dt work includes
read-only/fencing expectations, forced-migration cases and temporary-item cleanup.
Preserve the source/sink workloads, in-place predecessors and compatible-image
lane. Then convert remaining platform/stress callers. Keep the old promotion
cleanup parameter until callers migrate. `native-lifecycle-harness-seams.md` is
an inventory, not current status. Its PG CDC reporting gap is already fixed.

Three scope decisions are pending:

- Replica-side catalog-free ingress is still present. Proposed replacement for
  the headless driver's programmable coverage is a test-only native runtime.
  Keep shared query transport, native nonleader workers and both Timely topologies.
  See `native-replica-execution-boundary.md`. No implementation is claimed.
- The dormant final-preflight hook supports manually configured old-to-old runs.
  Its proposed explicit retirement is not yet approved. Leave it unchanged.
  See `final-preflight-boundary-research.md`.
- Native retirement does not reclaim orphaned temporary Items. Session rows carry
  execution identity but publish asynchronously and can be reaped first. Proposed
  separate repair: put the existing execution owner on temporary Items and let
  adapters reclaim only catalog-retired owners. Leave the temporary-item workflow
  unchanged pending this decision. Details are in the follow-up inventory.

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

The current-source preservation, upsert-stress and data-ingest container workflows
have not been rerun after their lifecycle conversion. Broad native container/upgrade
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
- `native-current-source-verification.md`, `native-upsert-lifecycle-verification.md`
  and `native-data-ingest-verification.md` distinguish checked harness contracts
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

No verification task or managed runtime remains running. All owned fixture
containers, networks and volumes were removed. About 45 GiB is free. Preserve a
30 GiB reserve, use one low-priority Cargo job and keep heavy work sequential.
Current service binaries contain the compute-owner removal. Retain baseline
artifacts, measurement reports and third-party cache. Superseded build cleanup is
recorded in `native-compute-*-cleanup.log` and `native-compute-reserve-reclaim.log`.
Leave the unrelated added `tmp_dec1fdd35343b4a99fe5b65d9e20e2b4b7085ecc719a42c7915575284b56c512.toml`
and persist-traffic worktree untouched.

Earlier compatible-version proof is in `lifecycle/verification.md` under the evidence
parent. It predates this cleanup. The [publication comparison](20260903_decoupled_coordination_publication_interval.md)
and `logical-component-load/` remain the load baseline, not a cleanup performance
claim. Further history is linked from the [archive](20260903_decoupled_coordination_log_archive.md).
