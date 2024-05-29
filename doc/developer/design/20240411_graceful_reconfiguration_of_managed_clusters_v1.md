# Graceful Reconfiguration of Managed Clusters

Associated issues/prs:
[#20010](https://github.com/MaterializeInc/materialize/issues/20010)
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
 - The action will be cancelable or able to be preemptively finalized.

## Out of Scope
 - Interactions with proposed schedules, ex: auto-scaling/auto-quiescing

## Solution Proposal

### Summary:
This feature will introduce new SQL in `ALTER CLUSTER` which will create new
replicas matching the alter statement; however, unlike existing alter mechanisms,
we will delay the cleanup of old replicas until the provided conditions are met.

The SQL will need to define a type of check to be performed, parameters for that
check, and whether or not to background the operation.

Suggested syntax:

```sql
ALTER CLUSTER c1 SET (SIZE 'small') WITH ( 
  WAIT = FOR <interval>,
  BACKGROUND = {true|false},
)
```

```sql
ALTER CLUSTER c1 SET (SIZE 'small') WITH ( 
  WAIT = UNTIL CAUGHT UP (
    TIMEOUT = <interval>,
    ON TIMEOUT = {CONTINUE | ABORT}
  )
  BACKGROUND = {true|false},
)
```

The two types of checks, in agreement with the V1 and V2 scopes respectively, are: 
 - `FOR` which waits for a provided duration before failing and aborting.
 - `UNTIL CAUGHT UP` which will wait for a "catch up" mechanism to return true
   or for a timeout to be met. It will roll back or forward depending on the value
   provided to `ON TIMEOUT`.

The SQL will kick off some action in `sequence_alter_cluster_managed_to_managed`
which will result in new reconfiguration replicas being created, new entries
placed `mz_internal.mz_pending_cluster_replicas` and a `ReconfigureCluster`
task being created which will ensure the reconfiguration finishes.

To keep track of ongoing reconfigurations we will introduce a new
`mz_internal.mz_pending_cluster_replicas` table which holds the state of
reconfiguration replicas. This table will include all configuration parameters
required to restart a reconfiguration task. It will be in charge of resuming or
cleaning up a reconfiguration.

All replicas will be billed during reconfiguration (except explicitly unbilled replicas).


### Definitions:
 - Active Replica: These replicas should be actively serving results and are
   named r1, r2, r3, etc. If a reconfiguration is ongoing, they will match the
   prior version of the configuration. If a reconfiguration is not ongoing they
   should be the only replicas.
 - Reconfiguration Replica: These replicas only exist during a reconfiguration,
   their names will be suffixed with `reconfig-`. They will move to active
   once the reconfiguration is finalized. These will be stored in the
   `mz_pending_cluster_replicas` table.
 - Reconfiguring Cluster: any cluster with one or more replicas in the
   `mz_pending_cluster_replicas` table

### Guard Rails
- Only one reconfiguration will occur at a time for a given cluster. Alter statements
  run against a reconfiguring cluster will fail.
- We must protect against creating a source/sink on a reconfiguring cluster and
  reconfiguring a cluster with sources/sinks. These clusters will need to use
  the non-scheduled mechanism (delete-before-create). This is due to a limitation
  where sources and sinks must only run on a single replica.
- We must store all state required to restart a `ReconfigureCluster` task in the event that environmentd
  crashes/restarts.
- We may want to consider limiting timeout duration.

### Visibility
Reconfiguration replicas should be identifiable for purposes of reporting and
visibility in the UI. We may be able to do this by creating a prefix `reconfig-`
for reconfiguration replicas, alternatively, we could set a bool status on the
replica resource in the catalog.

### Details

#### Limitations:
  - Cannot be used on clusters with sources, sources should not be created on
    clusters with reconfiguration replicas.
  - Must be run on managed clusters, and cannot be used to alter a cluster from
    managed to unmanaged.

#### Replica Naming
Active replicas will continue with the same naming scheme (r1, r2, ...)
Reconfiguration replicas will use the `reconfig` prefix; ex `reconfig-r3`,
`reconfig-r4`, which will be removed once they are moved to active.

#### Interactions with other schedules:

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


#### Catalog Changes
We will need to create a new table `mz_internal.mz_pending_cluster_replicas`

| column     |  type |
| ---------- | ----- |
| replica_id | str   |
| cluster_id | str   |
| timeout    | int   |
| wait_type  | str   |
| background | bool  |
| start_time | int   |
| finished   | bool  |
| continue_on_timeout | bool |

This table will be responsible for holding the state of the graceful replication task.
Each reconfiguration replica will have an entry in `mz_epnding_cluster_replicas`.


#### Reconfiguration Task

The reconfiguration task will perform the following actions:
1. Monitor the `wait` condition
The `wait` condition provided in the DDL will be polled by this task, once the
condition is met the replicas will be marked as finished and the task will move
on to finalization.

2. Finalize the reconfiguration
Finalizing the reconfiguration will occur once the `wait` condition has been
met. During this phase, the following steps will occur.
- reconfiguration replica promotion to active
- cluster configuration update to the new desired state
- replicas with the previous configuration will be removed
- replicas for the cluster will be removed from the `mz_pending_cluster_replicas` table

3. Reverting the reconfiguration
On failure or timeout of the reconfiguration, this task will revert to the state
prior to the `ALTER CLUSTER` DDL. It will remove reconfigure replicas, and ensure the
cluster config matches the original version.


#### Cluster Sequencer

The following roughly defines the new logic for the cluster sequencer when
performing an alter cluster on a managed cluster.

`mz_sql::plan::AlterClusterOptions` will have additional fields added to keep track of
the mechanism being used to perform the alter `AlterClusterMechanism::{Default|Duration|UntilCaughtUp}`.

If we see a non-default mechanism we'll ensure that the cluster is managed
and pass the mechanism to `sequence_alter_cluster_managed_to_managed`.
This function will need to be updated to either perform a
`sequence_alter_cluster_managed_to_managed_disruptive` or
`sequence_alter_cluster_managed_to_managed_graceful` operation.

The `disruptive` version will perform the current delete-before-create mechanism.
The `graceful` version will perform the following actions:
1. Validations
  1. validate that the cluster is not undergoing a reconfiguration
  2. validate the cluster is managed
  3. validate the cluster has no sources and now sinks
  4. the cluster cannot have a refresh schedule (v1)
2. Creating the reconfiguration replicas
3. Transactionally creating reconfiguration replicas and inserting the replicas into
   the `mz_pending_cluster_replicas` table.
4. Starting a blocking (or backgrounded) reconfiguration task 


#### Recoviner from Environmentd Crash
Initially, we will recover from an environmentd crash by removing all
reconfiguration replicas and clearing the `mz_pending_cluster_replicas` table,
to ensure no active reconfigures are occurring. This is because
we will treat any disruption in connection as a failed DDL which will need to
be aborted.

For V2, for each cluster, we will want to inspect whether an active
reconfiguration is ongoing and continue it. To do this we'll likely need calls
to `sequence_alter_cluster_managed_to_managed` to be idempotent w.r.t pending
cluster replicas. 


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

What we likely want is a mechanism [discussed here](https://github.com/MaterializeInc/materialize/issues/20010#issuecomment-2058563052), where we
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


### Reconfiguration replica naming:
Is it still useful to have reconfiguration replica naming differences if we also have a table tracking those
replicas? It seems like we could get the list just as easily with a join and skip a step in the reconfiguration task.

### Should the reconfiguration task create the replicas
Currently, I have the reconfiguration task set to be in charge of the wait check and the finalization, but not
the reconfiguration replica creation nor the insertion into `mz_cluster_pending_replicas`. This seems like a reasonable
separation, as if we cannot do these two things we should fail and not background a task.


### Do we want a more generic jobs table?
The proposed mechanism is for a sort of one-off very specific job engine. Assuming we
want a more robust job engine in the future it may make sense to take baby steps towards that here.
For instance, we could introduce an `mz_job` table that stores all information about a ReconfiguratTask. This
could be used on environmentd restart to re-initialize the task or perform cleanup for the task if the envd restart
would've caused the task to fail. I haven't thought this through much.
