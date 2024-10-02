# Graceful Reconfiguration of Managed Clusters

Associated issues/prs:
[#20010](https://github.com/MaterializeInc/database-issues/issues/5976)
[#26401](https://github.com/MaterializeInc/materialize/pull/26401)

## The Problem
Reconfiguring a managed cluster via `ALTER CLUSTER...` leads to downtime. This
is due to our cluster sequencer first removing the old replicas and then adding new
replicas that match the new configuration. The duration of downtime can be seen
as extending through the full period it takes to rehydrate the new replicas.

## Success Criteria
A mechanism should be provided that allows users to alter a managed cluster,
delaying the deletion of old replicas while new replicas are spun up and
hydrated.

## Scope
### V1 Scope
 - Provide a timeout-based reconfiguration cleanup scheduling.
 - The DDL will block for the duration of the reconfiguration.
 - The action will be cancellable by canceling the query.

### V2 Scope
 - Provide a Hydration-based reconfiguration cleanup scheduling.
 - The DDL will be able to run in the background.
 - Backgrounded reconfigurations will be cancelable or be able to be
   preemptively finalized, by another connection issuing a SQL statement.

## Out of Scope
 - Interactions with proposed schedules, ex: auto-scaling/auto-quiescing

## Solution Proposal

### Summary:
This feature will introduce new SQL in `ALTER CLUSTER` which will create new
replicas matching the alter statement; however, unlike existing alter mechanisms,
it will delay the cleanup of old replicas until the provided `WAIT` condition is met.

The SQL will need to define a type of check to be performed, parameters for that
check, and whether or not to background the operation.

Suggested syntax:

```sql
ALTER CLUSTER c1 SET (SIZE 'small') WITH (
  WAIT = <condition>,
  BACKGROUND = {true|false},
```

The two types of checks, in agreement with the V1 and V2 scopes respectively, are:
 - `FOR`, which waits for a provided duration before proceeding with finalization.
    ```sql
    ALTER CLUSTER c1 SET (SIZE 'small') WITH (
      WAIT = FOR <interval>,
      BACKGROUND = {true|false},
    )
    ```
 - `UNTIL CAUGHT UP`, which will wait for a "catch up" mechanism to return true
   or for a timeout to be met. It will roll back or forward depending on the value
   provided to `ON TIMEOUT`.
    ```sql
    ALTER CLUSTER c1 SET (SIZE 'small') WITH (
      WAIT = UNTIL CAUGHT UP (
        TIMEOUT = <interval>,
        ON TIMEOUT = {CONTINUE | ABORT}
      )
      BACKGROUND = {true|false},
    )
    ```

All replicas will be billed during reconfiguration (except explicitly unbilled replicas).

__Cancelation__
For v1, all `ALTER CLUSTER` statements will block, and graceful reconfiguration
will only be cancelable by canceling the query from the connection that issued it.

For v2, we will introduce backgrounded reconfiguration which will need some
new semantics to cancel. Something like the following should suffice.
```sql
CANEL ALTER CLUSTER <cluster>
```

### Definitions:
 - Active Replica: These replicas should be actively serving results and are
   named r1, r2, r3, etc. If a reconfiguration is ongoing, they will match the
   prior version of the configuration. If a reconfiguration is not ongoing they
   should be the only replicas.
 - Reconfiguration Replica: These replicas only exist during a reconfiguration,
   their names will be suffixed with `-pending`. They will move to active
   once the reconfiguration is finalized. These will be stored in the
   `mz_pending_cluster_replicas` table.
 - Reconfiguring Cluster: any cluster with one or more replicas in the
   `mz_pending_cluster_replicas` table
 - Replica Promotion, during the finalization of an alter cluster, pending replicas will be "promoted".
   This entails changing the pending value of the replica to false, and removing the '-pending'
   suffix.

### Guard Rails
- Only one reconfiguration will occur at a time for a given cluster. Alter statements
  run against a reconfiguring cluster will fail.
- We must protect against creating a source/sink on a reconfiguring cluster and
  reconfiguring a cluster with sources/sinks. These clusters will need to use
  the non-scheduled mechanism (delete-before-create). This is due to a limitation
  where sources and sinks must only run on a single replica.
- We will use a launch darkly flag to hardcap the max timeout and duration.
- If we are not backgrounding the reconfiguration the expectation will be that
  if the sql query does not successfully return, due to a crash or connection drop, the alter
  will not finalize and all reconfiguration replicas will cleaned up.

### Limitations:
  - Cannot be used on clusters with sources or sinks, sources should not be created on
    clusters with reconfiguration replicas.
  - Must be run on managed clusters, and cannot be used to alter a cluster from
    managed to unmanaged.

### Visibility
Reconfiguration replicas will be identifiable for purposes of reporting and
visibility in the UI. They will both have a `-pending` suffix and will have a
`pending` column in `mz_cluster_replicas` will be true.

### Replica Naming
Active replicas will continue with the same naming scheme (r1, r2, ...)
Reconfiguration replicas will use the `-pending` suffix; ex `r1-pending`,
`r2-pending`, which will be removed once they are moved to active.


### Implementation Details
We want to accomplish the following
 - interpret SQL to identify when to use graceful vs disruptive alters
 - selectively choose which mechanism is used to alter the cluster
 - provide a mechanism to cancel the operation


#### Catalog Changes
A `pending: bool` field will be added to the `ClusterReplica` memory and durable
catalog structs. This will be set to true for reconfiguration replicas and will
help either recover or cleanup on environmentd crashes. `mz_cluster_replicas`
will show the value of this `pending` field.

#### Process
Below broken this process broken down into four components
1. Interpret the query to determine if we're doing graceful reconfiguration
2. run `sequence_alter_cluster_managed_to_managed_graceful`
3. wait and finalize
4. return to the user

*In v2, steps 3 and 4 may be swapped.

__Interpreting the Query__
This will use standard parsing/planning, the only notable
callout here is that we'll want `mz_sql::plan::AlterClusterOptions`
to contain an `alter_mechanaism` field which holds an optional
`AlterClusterMechanism`:

```rust
enum AlterClusterMechanism {
  Duration{
    timeout: Duration
    ...
  },
  UntilCaughtUp{
    timeout: Duration,
    continue_on_timeout: bool
    ...
  },
}
```

__Cluster Sequencer__

The following outlines the new logic for the cluster sequencer when
performing an alter cluster on a managed cluster.

Because this will need to be cancelable and will wait for a significant
duration before responding, the current `sequence_cluster_alter` process cannot
be used. This implementation will rely on migrating `sequence_cluster_alter` to
use the `sequence_staged` flow, allowing us to kick off a thread/stage which
performs waits/validations.

The new `sequence_alter_cluster` function will need to perform
the current validations, used in both `sequence_alter_cluster` and
`sequence_alter_cluster_managed_to_managed`, but will kick off a new
`StageResult::Handle` when an `alter_mechanism` is provided. The task in this
handle will perform waits/validations and pass off the finalization to
a `Finalize` stage.

Additionally, we'll need to perform some extra validations to ensure the
the `alter_mechanism` and `AlterClusterPlan` are compatible. These are:
1. validate the cluster is managed
2. validate that the cluster is not undergoing a reconfiguration
3. validate the cluster has no sources and now sinks
4. the cluster cannot have a refresh schedule (v1)

The `sequence_alter_cluster_managed_to_managed` method will need
to be updated to apply the correct name/pending status for new replicas
and only remove existing replicas when no alter_mechanism is provided.


For V2:
We will need to implement a message strategy that kicks the work off
off of the `sequence_staged` flow. In this scenario, we will still create new
replicas in the foreground, but the wait/checks will be performed in a separate
message handle by emitting a `ClusterReconfigureCheckReady` message.

The state for the reconfiguration will be stored in a `PendingClusterReconfiguration`
struct.


```rust
struct PendingClusterReconfiguration {
 ctx: ExecuteContext,
 // needed to determine timeouts
 start_time: Instant,
 // the plan will hold the cluster_id, config, mechanism, etc...
 plan: AlterClusterPlan,
 // idempotency key will need to be part of the  message and will need to be
 // validated to ensure we're looking at the correct plan, in case we
 // miss a cancel and a new entry was put in pending_cluster_reconfiguration.
 idempotency_key: Uuid,
}
```

In order to cancel we'll keep a map, `pending_cluster_reconfigurations`,  of
session `ClusterIds` to `PendingClusterReconfigurations`.

*Currently due to limitations in sources and sinks, we will need to maintain
the disruptive alter mechanism. Once multiple replicas are allowed for sources
and sinks we can remove the disruptive mechanism and default the wait to `WAIT=For 0 seconds`.


#### Handling PendingClusterReconfigure Messages

The responsibilities of this operation will be:
1. checking for cancellations
2. checking for the `WAIT` condition
3. finalizaing the reconfiguration
4. returning to the user

__Checking the `WAIT` Condition__
The message handler will perform all checks on every run. If the wait condition
has not yet been met, we will reschedule the message to be resent. For each
reschedule we'll spawn a task that waits some duration and sends a
`ClusterReconfigureCheckReady` message with the connection_id.
For `WAIT FOR` we can wait for the exact duration that would cause the next check to succeed.
For `WAIT UNTIL CAUGHT UP` we can just wait for three seconds.

__Finalize the Reconfiguration__
Finalizing the reconfiguration will occur once the `wait` condition has been
met. During this phase, the following steps will occur.
- Drop any non-pending replicas that do not have a billed_as value.
- Finalize the pending replicas:
  - Rename to remove the `-pending` suffix.
  - Alter config to set `pending` to true.
- Update the cluster catalog to the new desired config.


__Handling Dropped Connections__
Dropped connections will be handled by storing a `clean_cluster_alters: BTreeSet<GlobalId>` in the `ConnMeta`.
We will use this along with a `retire_cluster_alter` function which can be called during connection termination.
`retire_compute_sync_for_conn` provides an example of this.

__Handling Cancelation__
The mechanism used for handling dropped connections can be used for cancelation.
`cancel_compute_sync_for_conn` provides an example of this.

__v2 coniderations__
Reverting the reconfiguration
On failure or timeout of the reconfiguration, this task will revert to the state
prior to the `ALTER CLUSTER` DDL. It will remove reconfigure replicas, and ensure the
cluster config matches the original version.


#### Recover after Environmentd Restart
For v1, environmentd restarts will remove `pending` replicas from the Catalog
during Catalog initialization. This will cause the compute controller to remove
any pending physical replicas during its initialization (this is already an
existing feature).

For v2, we will need to store the AlterClusterPlan durably for all backgrounded reconfigurations.
We will then need the coordinator to attempt to continue an ongoing reconfiguration on restart.


#### Interactions with Cluster Schedules:

__V1__
We will choose to initially disallow any reconfiguration of a cluster
with a schedule.

__V2__
For refresh schedules, we will only allow graceful reconfiguration on clusters
whose refresh schedule is "ON", we would also need to prevent the clusters from being
turned off until the reconfiguration is finished.


__Future Schedules__
Some proposed schedules, auto-scaling, for instance, may want to use graceful
reconfiguration under the hood. These would need specific well-defined
interactions with user-invoked graceful reconfiguration.

### Related Tasks


## Alternatives

### Treating Reconfigure as a Schedule
The initial version of this doc detailed a potential solution where reconfiguration
was treated as a cluster schedule. In that plan, roughly, a new schedule would be added
to the cluster being gracefully reconfigured. Rather than having a task handle the reconfiguration
`HandleClusterSchedule` would have logic that looked for `ReconfigurationSchedule`s and would
send `ReconfigurationFinalization` decisions when the `wait` condition was met. This decision
would then promote pending replicas to active replicas, and remove the `ReconfigurationSchedule`
in `handle_schedule_decision`


### Syntax
The current proposal uses the `WITH (...)` syntax, it may be more straightforward to add this to the
cluster as a config attribute though:
```SQL
ALTER CLUSTER SET (CLEANUP TIMEOUT 5 minutes)
```

The above syntax would add a permanent reconfiguration timeout to the cluster that would
be applied on every reconfigure. For this to work we'd need to update
the cluster to know when a reconfigure event started, then we could compare the
event + the duration to now and send a `ReconfigureFinalization` decision.

It seems like the reconfiguration task exists only to interact with actions taken from
`ALTER CLUSTER` statements are not a property of the cluster, so I'm
not very bullish on this syntax. I could be convinced that this is more ergonomic.

## Open Questions

### How do we handle clusters with sinks and sources?
There is a current limitation around clusters with sources where the replication
factor must be 1 or 0 This being said we must disallow any reconfiguration
schedules for clusters with sources (and sinks?). This is a new annoying issue
as we now allow clusters to perform both compute and source roles. We should
consider some ways to handle this, one potential is to keep source activity to
a single replica in a multi-replica cluster. Alternatively, we just don't allow
this, and we add some big disclaimers to clusters with sources.

### Hydration-Based Scheduling
There is currently the beginning of the mechanism to look at hydration
based metrics for scheduling in `compute-client/src/controller/
instance:update_hydration_status`, this works very well for a single data flow,
but can't gauge the hydration status of a replica as a whole.

What we likely want is a mechanism [discussed here](https://github.com/MaterializeInc/database-issues/issues/5976#issuecomment-2058563052), where we
compare the data-flow frontiers between reconfiguration and active replicas,
and when the reconfiguration replicas have all data-flows within some threshold
of the active, we send a claim that the check condition has been met and proceed with
finalization of the reconfiguration.

__Potential challenges with using hydration status__:
It is difficult to know when "hydration" has finished because:
  - New data coming in may change the timestamp and lead to hydration status inaccuracies
  - New data-flows can be created ad-hock which will need to be "hydrated"
  - hydration_status errors related to Environmentd restart?
    (https://materializeinc.slack.com/archives/CTESPM7FU/p1712227236046369?thread_ts=1712224682.263469&cid=CTESPM7FU)
Some of these can be resolved by only looking at data flows that were installed
when the replica was created. It seems like comparing active and reconfiguration
replicas will likely work better than attempting to gauge when hydration has
finished for a given replica. It also may allow us to shorten the timeout if all
replicas for a cluster undergoing reconfiguration are restarted.

For hydration-based scheduling the `until caught up` syntax will be used.

There is also a question on the mechanism for tracking hydration or frontiers.
We could check at a regular interval if the frontiers between original
and reconfiguration replicas match. Another possible alternative is spinning up a thread that
subscribes to the frontiers and kicks off finalization once the chosen condition is met.

### Handling Sources / Sinks
Currently we are opting not to allow graceful reconfiguration of clusters with
sources and sinks, due to a single replica limit for these clusters. However,
it may be possible to allow some graceful reconfiguration on these clusters, if
we postpone installing the sources or sinks on the pending replicas until the
replica is promoted. This will ensure that it is the only replica running. We
would also have to ensure the new replication factor is greater than or equal to
one in the new config. Multi-replica sources and sinks will implicitly resolve
this issue, so, for now, it seems reasonable to wait for that.
