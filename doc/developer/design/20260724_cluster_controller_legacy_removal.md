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
   replicas itself because the controller will not. Part 3 deletes this path
   entirely.

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
  are deleted, not fenced off. The sequencer no longer creates or drops
  replicas for a managed-to-managed `ALTER CLUSTER`.
- `WITH (WAIT ...)` is accepted on every deployment, so graceful cluster
  reconfiguration no longer depends on a feature-acceptance flag (see Part 4).
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

Four parts. Parts 1 and 2 each widen `controller_owns` by dropping one
conjunct, Part 3 deletes everything that is dead once `controller_owns` is
constant-true, and Part 4 makes graceful reconfiguration generally available.
Parts 1 and 2 are separable and safe in either order. The controller-path
hardening can merge once both are in place, and Part 3 needs those
prerequisites. Part 4 landed independently as #38627 because accepting the
timeout surface does not depend on deleting the unreachable implementation.

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
config-shape changes reshape into a durable reconfiguration record. A factor
change stays a config-only write the controller converges on its next tick.

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

### Controller-path hardening before Part 3

These are fixes to the controller path already live in production. They are a
separate prerequisite PR, not behavior activated by deleting the legacy code:

- The sequencer must not predict a reshape's resource footprint. The concrete
  controller create transaction enforces resource limits against the complete
  strategy union. System clusters remain exempt from replica-count and credit
  accounting there. For a user cluster, the sequencer bounds only the target's
  intrinsic baseline replication factor against `max_replicas_per_cluster`
  before recording it. Cancellation back to the realized config bypasses this
  bound so a later limit reduction cannot trap an in-flight reconfiguration.
- Audit attribution: boot-time creates by the migration keep the `System`
  reason, runtime creates by the controller audit `Manual` (the tag for
  replicas the cluster's own config calls for), the same as user clusters.
- The controller's create path validates sizes via the cluster owner's
  allowed sizes. Builtin clusters owned by `mz_system` bypass
  `allowed_cluster_replica_sizes` (`get_role_allowed_cluster_sizes`), so no
  new failure mode there. `mz_support` / `mz_analytics` are owned by their
  own roles and see the same restriction their owners' ALTERs already see.

Every MANUAL managed-cluster shape change, including a zero-timeout commit,
writes or folds into a durable reconfiguration record. A shape change on a
non-MANUAL scheduled cluster updates the realized config directly, and
`WITH (WAIT ...)` is refused because there is no hydration overlap. A zero
timeout on the record path writes an already-elapsed deadline with
`ON TIMEOUT COMMIT`. The catalog write wakes the controller. A forced cut-over
first ensures that the complete target set exists, then advances the realized
config without waiting for hydration. This lets the concrete create transaction
enforce resource limits against the controller's complete strategy union. If
that transaction exceeds the budget, the controller leaves the realized set
untouched, marks the reconfiguration `resource-exhausted`, and
audits the outcome.

The two sets overlap only while the target hydrates. Past the deadline under
`ON TIMEOUT COMMIT` the reconfiguration has given up on hydration, so the
baseline stops contributing its realized replicas and the reshape becomes one
transaction that retires them and creates the target's. Only the net has to fit
the budget. Catalog implications queue the old replicas' drops before the
creates, so physical orchestration quotas observe the same replacement rather
than blocking the capacity-releasing drop behind a retrying create. That is what
lets a resize land with no room for both sets at once, and it is the only way to
shrink a cluster that already sits near its limit. The swap is rejected whole if
it still does not fit, so the record stays in progress and sheddable rather than
half-applied.

A hydration burst is transient too, but it is never shed. Nothing durable
records that a burst was unaffordable, so clearing it would let the unchanged
policy arm the same burst on the next tick, and each cycle would write a start
and a finish, allocate a replica id, and wake the next reconciliation. An
unaffordable burst retries its create instead, bounded by the tick interval, and
settles when the steady set hydrates.

The controller is the only component that decides the desired replica set. Its
decision unions every strategy, including the baseline, graceful
reconfiguration, ON REFRESH, and hydration burst. A sequencer-side cut-over
cannot safely reproduce that decision: some strategies depend on live signals
whose collection must complete off the serial coordinator loop. Using only the
baseline contribution drops replicas still desired by other strategies. In
particular, it cold-restarts an active hydration-burst replica and discards its
hydration progress. Deferring the whole decision to the controller removes the
second ownership implementation and preserves every active strategy.

Controller replica decisions are batched into one catalog transaction per
cluster. The coordinator's catalog transaction machinery inspects concrete
cluster and replica create ops after ids are allocated, derives their scoped
parameter contexts, and appends `UpdateScopedSystemParameters`. This is an
invariant of applying create ops rather than a responsibility of either the SQL
sequencer or cluster controller. Catalog implications therefore push a new
replica's scoped dyncfg layer before provisioning it, which render-frozen
settings require. The periodic parameter sync remains the full-state reconciler
rather than the first writer for these replicas.

The wake makes the normal latency one controller reconcile pass rather than the
periodic tick interval. The reconfiguration record is durable, so a restart
before that pass recovers on the first periodic tick. With background ALTER
enabled the statement returns after writing the record. With it disabled the
existing wait-shim blocks until the controller settles the record.

A replication-factor change remains refused while a reconfiguration record is
in progress because the record carries the factor it will write at cut-over.
Forcing a wedged resize through and changing its factor therefore takes two
statements: force the record with a zero-timeout commit, wait for it to settle,
then change the factor.

### Part 3: delete the staged machine and direct replica path

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
  converges, like user clusters today. The committed baseline is an exact
  count no strategy can shed, so that path keeps its own
  `max_replicas_per_cluster` check.

The removal also deletes `Op::UpdateClusterReplicaConfig`, whose last producer
was in the direct replica path.

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

### Part 4: accept `WITH (WAIT ...)` unconditionally (landed as #38627)

The planner gated every `WITH (WAIT ...)` clause on
`enable_zero_downtime_cluster_reconfiguration`, default off. Graceful
reconfiguration already used the durable controller path, so the flag only
controlled whether users could express its deadline and timeout behavior.

The feature flag and its planner gate are removed. The two rejections that
shared that code path stay: a `WAIT` without a replica-shape change, and a
`WAIT` on an unmanaged cluster.

This makes the graceful-reconfiguration surface generally available. The
private-preview badges on `ALTER CLUSTER`'s reference page came off with it.

`enable_cluster_schedule_refresh` is the near-identical sibling gate a dozen
lines below, and stays at its current default. It gates a separate SQL surface
under its own staged rollout (see Out of Scope).

## Behavior changes

- **System-cluster shape ALTERs become controller-driven.**
  `ALTER CLUSTER mz_system SET (SIZE ...)` changes from a synchronous
  whole-set recreation to a background graceful reconfiguration (default
  deadline 24h, `ROLLBACK` on timeout). This is strictly more capable: the
  graceful path is available when wanted (previously it did not exist for
  system clusters), and explicit `ON TIMEOUT COMMIT` requests a cut-over
  without waiting for hydration once its deadline passes.
- **Factor changes on system clusters converge asynchronously.**
  `ALTER CLUSTER mz_support SET (REPLICATION FACTOR 1)` updates the config
  and returns, the controller materializes the replica within a tick. Same
  as user clusters today.
- **A zero-timeout commit stays controller-driven.**
  `WITH (WAIT UNTIL READY (TIMEOUT '0s', ON TIMEOUT 'COMMIT'))` writes an
  already-expired record and wakes the controller. The controller provisions
  any missing target replicas and then commits the target without waiting for
  hydration. With background ALTER enabled, the statement can return before
  those passes complete. `WAIT FOR` instead rolls back if its deadline passes
  while the target remains unhydrated.
- **`WITH (WAIT ...)` is accepted on every deployment.** Part 4 removed its
  feature gate, so graceful reconfiguration is available without flipping a
  flag. Its intrinsic shape-change and managed-cluster checks still apply.
- **No more break-glass flag.** After Part 1, reverting to the legacy paths
  requires a binary rollback. The gate has been default-on since v26.29, so
  the controller paths have several releases of burn-in.

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
- Zero-timeout `WAIT` tests (`cluster-controller.td` and friends) exercise the
  same record and controller path as every other reshape. Testdrive pins that
  the controller is woken promptly, a forced cut-over reuses a matching target
  replica, and an active hydration-burst replica keeps its id. Resource-limit
  coverage verifies that a failed concrete create marks and audits the record
  while preserving the realized and burst replicas. Sqllogictest keeps
  acceptance coverage but cannot assert the asynchronous readback.
- `test/launchdarkly/mzcompose.py` runs with a one-hour parameter-sync interval
  and checks that a replacement created by the controller already has its
  replica-scoped override. The row can only come from the create transaction.
- `test/sqllogictest/mz_cluster_schedules.slt`: the
  `cluster_check_scheduling_policies_interval` validation block goes with the
  var.
- `test/cluster/resources/resource-limits.td` exercises factor flips on
  `mz_analytics` with retrying queries, so it survives the switch to
  asynchronous convergence and pins the system-cluster limit exemption.
- Flag-name references: `misc/python/materialize/mzcompose/__init__.py` pins
  `enable_cluster_controller` per version for mixed-version runs and needs an
  upper version bound at the removal version, v26.39 (older binaries keep the
  flag, newer ones warn on the unknown default).
  `misc/python/materialize/parallel_workload/action.py` drops it from the
  do-not-flip list, and the launchdarkly-flag-consistency allowlist entries
  (listed twice) surface as prunable.
- Part 4 applied the same treatment to
  `enable_zero_downtime_cluster_reconfiguration`. Mixed-version defaults pin
  it on for binaries before v26.41 because upgrade scenarios can issue
  `WITH (WAIT ...)` against a binary that still enforces the gate. Unlike the
  controller gate this flag is a real LaunchDarkly flag, so its
  flag-consistency entry moved to the stale list until it is archived there.

## Alternatives

- **Keep a synchronous sequencer cut-over for zero-timeout
  `ON TIMEOUT COMMIT`.** Rejected.
  A correct cut-over has to account for every controller strategy, but some
  strategy inputs can only be collected off the coordinator loop. A
  baseline-only approximation destroys replicas that remain desired, including
  an active hydration burst. Suspending and resuming the statement to collect
  those signals would recreate staged machinery this design removes. The
  durable record already wakes the controller and recovers across restart, so
  keeping a second replica owner is not worth the divergence.
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
