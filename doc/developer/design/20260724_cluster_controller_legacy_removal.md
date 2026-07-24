# Remove the legacy cluster scheduling and staged reconfiguration paths

- Associated (controller work, merged): #37214, #37452, #37671, #37767
- Associated (config owns builtin replicas, merged): #37929

## The Problem

The cluster controller is merged, default-on since v26.29, and owns the
replica set of every *user* managed cluster in production. But it still lives
behind the break-glass dyncfg `ENABLE_CLUSTER_CONTROLLER`, and system/builtin
clusters are excluded from its ownership (`ManagedClusterIds` filters
`is_user()`). Those two facts keep three legacy code paths alive:

1. **The legacy REFRESH scheduler** (`src/adapter/src/coord/cluster_scheduling.rs`),
   driven by a coordinator timer, superseded by the controller's
   `OnRefreshStrategy`. Both of its entry points return before doing any work
   while the gate is on, so it is a strict no-op in production. Only the gate
   keeps it reachable.
2. **The legacy staged reconfiguration machine**: the `WaitForHydrated` and
   `Finalize` cluster stages, the `-pending` overlap replicas, and their
   connection-lifecycle cleanup, superseded by the controller's durable
   reconfiguration record plus the `AwaitReconfiguration` wait-shim. With the
   gate on it is reachable through exactly one door: a config-shape
   `ALTER CLUSTER <system cluster> ... WITH (WAIT ...)`.
3. **The legacy direct replica create/drop path** in
   `sequence_alter_cluster_managed_to_managed`: synchronous whole-set
   recreation on a size change, direct create/drop on a replication-factor
   change. With the gate on it runs only for system clusters, which is the
   system-cluster exception in action: the sequencer must materialize their
   replicas itself because the controller will not. This path does not go
   away entirely, it shrinks to a single explicitly requested cut-over
   branch (see Part 3).

The system-cluster exception was originally load-bearing: the boot-time
builtin replica migration and the controller would have been two conflicting
writers of one replica set. #37929 removed that conflict.
`reconcile_builtin_cluster_replicas` now converges a builtin cluster's
replica set on the cluster's own managed config at catalog open, the same
config the controller derives its targets from, so the two converge on the
same set by construction. The exception no longer prevents a bug. It only
forces the sequencer to keep a second replica-materialization implementation
alive for system clusters.

## Success Criteria

- The cluster controller owns the replica set of *all* managed clusters, user
  and system alike. The `is_user()` ownership conjuncts are gone.
- `ENABLE_CLUSTER_CONTROLLER` is removed. The controller runs unconditionally
  (it still quiesces while the deployment is read-only).
- The legacy REFRESH scheduler and the legacy staged reconfiguration machine
  are deleted, not fenced off. The direct create/drop path shrinks to a single
  cut-over branch, kept as an explicitly requested escape hatch (see Part 3).
- That escape hatch is reachable on any deployment, so the statement that
  requests it is no longer feature-gated (see Part 4).
- Boot ordering and 0dt read-only behavior are preserved: system clusters have
  their replicas up and hydrated before the serve loop starts and before a 0dt
  cutover, provided by `reconcile_builtin_cluster_replicas` at catalog open,
  which stays.

## Out of Scope

- Removing the controller sub-behavior kill-switches
  `ENABLE_BACKGROUND_ALTER_CLUSTER` and `ENABLE_HYDRATION_BURST`. They gate
  controller behaviors, not legacy paths, and retire separately after burn-in.
- Removing the feature-acceptance flags `enable_cluster_schedule_refresh` and
  `enable_auto_scaling_strategy`. Those gate user-facing SQL surface under a
  staged rollout.
- Removing the durable `pending` field on `ReplicaLocation::Managed` and
  `remove_pending_cluster_replicas_migration`. The field becomes vestigial
  (always false) once the staged machine is gone, but the migration is the
  only remaining cleaner for `pending` replicas stranded by a crash on a
  pre-deletion version, so both go together in a later release (see Part 3).

## Solution Proposal

Four parts. Parts 1 and 2 each just widen `controller_owns` (drop one
conjunct each), Part 3 deletes everything that is dead once `controller_owns`
is constant-true, and Part 4 makes the surface that reaches the one surviving
direct path unconditional. 1 and 2 are separable and safe in either order, 3
needs both, 4 needs 3.

### Part 1: remove `ENABLE_CLUSTER_CONTROLLER` and the legacy scheduler

Delete the dyncfg (definition, registration, and its five read sites, plus the
sqllogictest binary's force-on block) and everything only it kept alive:

- `cluster_scheduling.rs` wholesale: `check_scheduling_policies`,
  `check_refresh_policy`, `handle_scheduling_decisions`, and the
  `SchedulingDecision` / `RefreshDecision` types.
- The plumbing: `Message::CheckSchedulingPolicies` and
  `Message::SchedulingDecisions` with their handlers, the coordinator timer
  and select-loop tick, and the `cluster_scheduling_decisions` coordinator
  state.
- The two metrics `check_scheduling_policies_seconds` and
  `handle_scheduling_decisions_seconds`.
- The system var `cluster_check_scheduling_policies_interval` (its sole
  consumer is the timer).
- `ReplicaCreateDropReason::ClusterScheduling`, constructed only by the
  scheduler.

The persisted audit vocabulary stays: `SchedulingDecisionsWithReasonsV2` and
the audit-log types are written by the controller's `OnRefresh` path too
(`refresh_window_decision_to_audit_log`), and old events must remain
decodable regardless.

### Part 2: controller owns system clusters

Drop the `is_user()` conjunct from `ManagedClusterIds` and from the two
`controller_owns` computations in the sequencer. Runtime ALTERs of system
clusters then flow through the controller like any other managed cluster:
config-shape changes reshape into a durable reconfiguration record, factor
changes update the config and the controller converges the replica set.

Ownership and the boot migration compose rather than conflict, in both
directions:

- The controller matches replicas by shape and count, never by name, so the
  migration-created `r1..rN` replicas satisfy its baseline and a steady
  system cluster reconciles to no decisions.
- The migration converges by canonical name, so a boot after a reshape (or
  with a reconfiguration in flight) renames or re-creates replicas the
  controller had materialized under generator names. That is harmless churn:
  every replica is a cold process at boot anyway, so the cost is replica-id
  and audit-log noise, and an in-flight record is durable, so the controller
  picks the reconfiguration back up on its first tick.

Points to settle in the implementation, none of them blockers:

- `validate_reconfiguration_resource_limits` early-returns for non-user
  clusters with the comment "a system cluster never reshapes into a record",
  which this part makes false. The early return itself stays, since system
  clusters are exempt from `max_replicas_per_cluster` and credit accounting
  everywhere else, but the comment must state the exemption instead.
- Audit attribution: boot-time creates by the migration keep the `System`
  reason, runtime creates by the controller audit `Manual` (the tag for
  replicas the cluster's own config calls for), the same as user clusters.
- The controller's create path validates sizes via the cluster owner's
  allowed sizes. Builtin clusters owned by `mz_system` bypass
  `allowed_cluster_replica_sizes` (`get_role_allowed_cluster_sizes`), so no
  new failure mode there. `mz_support` / `mz_analytics` are owned by their
  own roles and see the same restriction their owners' ALTERs already see.

### Part 3: delete the staged machine and slim the direct path to an escape hatch

With `controller_owns` constant-true, `NeedsFinalization::Yes` has no
producer. Delete:

- `ClusterStage::WaitForHydrated` and `ClusterStage::Finalize`, their structs,
  dispatch arms, and handlers.
- `NeedsFinalization` and `PENDING_REPLICA_SUFFIX`.
- The `pending_cluster_alters` connection state and its retire paths:
  `drop_reconfiguration_replicas`,
  `retire_cluster_reconfigurations_for_conn`,
  `cancel_cluster_reconfigurations_for_conn`, and their call sites in
  connection cleanup and cancellation.
- The `AlterClusterWhilePendingReplicas` error (its raise site goes with the
  machine, its catch site went with the scheduler).
- In `sequence_alter_cluster_managed_to_managed` (single caller left once the
  scheduler is gone): the pending-replica arm and the separate scale-up/down
  branches. Factor-only changes are config-only writes the controller
  converges, like user clusters today.

The direct reshape branch is deliberately **kept**, as the synchronous
cut-over path: converge the replica set onto the target and settle any carried
reconfiguration record, all in one catalog transaction with no controller
involvement. It is routed to by a zero-timeout commit strategy
(`WITH (WAIT FOR '0s')`, or `WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT
'COMMIT')`) instead of by `AlterClusterPlanStrategy::None` as today. Two
reasons:

- **Escape hatch.** Once the gate is gone there is no break-glass flag, and
  every other reshape depends on the controller ticking and applying. The
  direct path is the one reshape that still works when the controller itself
  is the problem, and it simultaneously unsticks a wedged reconfiguration by
  retiring the record. Requesting it stays safe under a live controller: the
  config write invalidates any in-flight tick's compare-and-append witness,
  so a stale controller batch is rejected, the same as any user DDL landing
  mid-tick.
- **Honest semantics.** A zero timeout with commit already means "cut over
  now, hydrated or not". Doing it synchronously in the ALTER instead of one
  controller tick later is the same outcome, minus the tick.

"The same outcome" is a constraint, not an observation, so the cut-over does
not get to improvise. It reuses the two things that define what a reshape
means:

- The target is folded onto an in-flight one exactly as the record path folds
  it (`alter_reconfiguration_target`, shared by both). Otherwise the same
  statement would mean different things depending on which path took it: an
  `ALTER` mentioning one dimension would silently revert the in-flight
  transition along every dimension it did not mention.
- The replica set is converged with the controller's own reconcile kernel
  (`reconcile_replicas`, made public for this), against the cluster as
  `observe_cluster_state` sees it. So a replica that already carries the
  target shape is kept rather than bounced, which is what makes forcing a
  stuck-but-hydrating resize to commit cost nothing beyond the tick it saves.
  A blind whole-set recreate would instead cold-restart a replica that was
  already up, and would turn a cancel into a full bounce of a healthy set.

Settling the carried record follows from where the cut-over landed
(`retire_carried_reconfiguration`): on the record's own target it *is* the
finalization the record was waiting for, so it settles `Finalized` marked
`forced` (nothing waited for hydration); anywhere else the target was
abandoned, which is a cancel.

The branch keeps its supporting plumbing alive: the replica-id pre-allocation
and the replica eval contexts for created replicas. It drops by observed id
rather than by canonical name, so it works after controller name drift and
with overlap replicas present.

What stays, and why:

- `AlterClusterPlanStrategy` and `ClusterStage::AwaitReconfiguration`: the
  controller reshape path and its foreground wait-shim consume them, as does
  `cluster_alter_check_ready_interval`.
- `remove_pending_cluster_replicas_migration`: an upgrade can come from a
  version whose staged machine crashed between the pending-create commit and
  finalize. Those replicas are durably `pending: true`, excluded from the
  controller's ownership test, and after this deletion no runtime path would
  ever clean them, so the catalog-open migration is the only remaining
  cleaner. It is removed together with the durable `pending` field once no
  supported upgrade source can still write pending replicas.
- `reconcile_builtin_cluster_replicas`: still load-bearing in the two windows
  the controller cannot cover. `Coordinator::bootstrap` brings up only
  replicas already durable and runs before the controller task is spawned,
  and the controller is inactive in 0dt read-only mode, where the migration
  (running against the savepoint catalog) is what gets system clusters up and
  hydrated before cutover.

### Part 4: accept `WITH (WAIT ...)` unconditionally

Part 3 leaves the synchronous cut-over reachable only through a
`WITH (WAIT ...)` clause, and the planner gates any such clause on
`enable_zero_downtime_cluster_reconfiguration`, default off. So on a deployment
running the compiled default the escape hatch does not exist, which defeats the
reason for keeping it. A break-glass path behind a default-off flag is not a
break-glass path.

Remove the feature flag and its planner gate. The two rejections that share
that code path stay: a `WAIT` without a replica-shape change, and a `WAIT` on
an unmanaged cluster.

This makes the graceful-reconfiguration surface generally available, so the
private-preview badges on `ALTER CLUSTER`'s reference page come off with it.

`enable_cluster_schedule_refresh` is the near-identical sibling gate a dozen
lines below, and stays at its current default. It gates a separate SQL surface
under its own staged rollout (see Out of Scope).

## Behavior changes

- **System-cluster shape ALTERs become controller-driven.**
  `ALTER CLUSTER mz_system SET (SIZE ...)` changes from a synchronous
  whole-set recreation to a background graceful reconfiguration (default
  deadline 24h, `ROLLBACK` on timeout). This is strictly more capable: the
  graceful path is available when wanted (previously it did not exist for
  system clusters), and `WITH (WAIT FOR '0s')` requests an immediate
  cut-over for operators who cannot afford the overlap set, for example when
  resizing on a deployment with no headroom.
- **Factor changes on system clusters converge asynchronously.**
  `ALTER CLUSTER mz_support SET (REPLICATION FACTOR 1)` updates the config
  and returns, the controller materializes the replica within a tick. Same
  as user clusters today.
- **A zero-timeout commit `WAIT` becomes synchronous.** Today
  `WITH (WAIT FOR '0s')` writes a record the controller commits on its next
  tick. It now takes the direct path and returns with the cut-over already
  transacted. Same outcome, one tick sooner, and it works even when the
  controller does not.
- **`WITH (WAIT ...)` is accepted everywhere.** Part 4 removes its feature
  gate, so the escape hatch is reachable without flipping a flag first.
- **No more break-glass flag.** After Part 1, reverting to the legacy paths
  requires a binary rollback. The gate has been default-on since v26.29, so
  the controller paths have several releases of burn-in. The direct path is
  the remaining operational escape hatch for reshapes.

## Test and rollout surface

- `test/sqllogictest/system-cluster.slt`: the blocks asserting a synchronous
  replica count right after `ALTER CLUSTER mz_system SET (SIZE ...)` and
  after factor flips need rework, since convergence is now asynchronous and
  slt does not retry. Move them to testdrive (which retries) or assert on
  the config instead of the replica set.
- `test/testdrive/cluster-controller.td`: the gate-off sections go (the
  unmanaged-conversion refusal block and the `cc_handoff*` legacy-handoff
  scenarios). The file's freeze-the-controller technique (flipping the gate
  off to hold an in-flight state still) is replaced by cranking
  `cluster_controller_tick_interval` up.
- `test/pg-cdc/cluster-graceful-reconfiguration.td`: the explicit legacy
  foreground section goes, the controller section stays.
- Zero-timeout `WAIT` tests (`cluster-controller.td` and friends) now
  exercise the direct path, so assertions that expect a reconfiguration
  record for the `'0s'` case need adjusting.
- `test/sqllogictest/mz_cluster_schedules.slt`: the
  `cluster_check_scheduling_policies_interval` validation block goes with the
  var.
- `test/cluster/resources/resource-limits.td` exercises factor flips on
  `mz_analytics` with retrying queries, so it survives the switch to
  asynchronous convergence and pins the system-cluster limit exemption.
- Flag-name references: `misc/python/materialize/mzcompose/__init__.py` pins
  `enable_cluster_controller` per version for mixed-version runs and needs an
  upper version bound at the removal version, v26.38 (older binaries keep the
  flag, newer ones warn on the unknown default).
  `misc/python/materialize/parallel_workload/action.py` drops it from the
  do-not-flip list, and the launchdarkly-flag-consistency allowlist entries
  (listed twice) surface as prunable.
- Part 4 needs the same treatment for
  `enable_zero_downtime_cluster_reconfiguration`, and it is the load-bearing
  case: the graceful-reconfiguration platform check issues its
  `WITH (WAIT ...)` in a manipulate phase that an upgrade scenario runs
  against the previous release, which still enforces the gate. Unlike the
  controller gate this flag is a real LaunchDarkly flag, so its
  flag-consistency entry moves to the stale list until it is archived there.

## Alternatives

- **Delete the direct path entirely and let `WAIT FOR '0s'` ride the
  controller record.** Maximal deletion. Rejected: once the gate is gone,
  every reshape would depend on the controller ticking and applying, so a
  controller bug would leave no way to reshape any cluster, and no flag to
  fall back on. The kept branch is small, exercised by an explicit statement,
  and doubles as the cleanup tool for a wedged reconfiguration.
- **Keep the system-cluster exception, reject `WITH (WAIT ...)` on system
  clusters, delete only the scheduler and the staged machine.** This closes
  the staged machine's last door with a new error instead of ownership.
  Rejected: it must keep the full legacy direct create/drop path alive for
  system clusters forever, so the sequencer retains a second complete
  replica-materialization implementation, and it removes a currently-working
  statement while this design makes the same statement do the right thing.
  Ownership deletes more code and adds a capability instead of an error.
- **Accept and ignore `WAIT` on system clusters.** Rejected: an instant
  success that waited for nothing is the same silent lie we fixed for
  unmanaged clusters (see the v26 release notes entry on `WAIT` being
  silently ignored). Moot under this design, where the `WAIT` is honored.
