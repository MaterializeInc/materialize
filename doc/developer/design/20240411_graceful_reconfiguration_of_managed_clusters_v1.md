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
A mechanism should be provided that allows users to delay the deletion of old
replicas while new replicas are spun up

## Scope
### V1 Scope
 - Timeout-based reconfiguration cleanup scheduling

### V2 Scope
 - Hydration-based reconfiguration cleanup scheduling

### Out of scope
 - Issues arising from multi-subscriber message handling are out of scope.

## Solution Proposal

### Summary:
This feature will introduce new SQL in `ALTER CLUSTER` which will delay the
cleanup of replicas until a specified timeout, or until a hydration check
is triggered. 

Ex:
```sql
ALTER CLUSTER c1 SET (SIZE 'small') WITH ( CLEANUP TIMEOUT 5 minutes );
```

Additionally, it will be possible to preempt the cleanup by specifying the
statement with a TIMEOUT of 0, as long as no other changes are made to the
cluster.

Ex:
```sql
ALTER CLUSTER c1 SET (SIZE 'small') WITH ( CLEANUP TIMEOUT 0 minutes );
```


The new syntax and mechanisms will be built to work with auto-cleanup mechanisms
built around cluster hydration but the syntax for that and the mechanism are not
in scope for this document.

All replicas will be billed during reconfiguration (except explicitly unbilled replicas).


### Definitions:
 -  Active Replica: These replicas should be actively serving results and are
    named r1, r2, r3, etc. If a reconfiguration is ongoing, they will match the
    prior version of the configuration. If a reconfiguration is not ongoing they
    should be the only replicas.
 - Reconfiguration Replica: These replicas only exist during a reconfiguration,
   their names will be suffixed with `reconfig-`. They will move to active once
   the reconfiguration is finalized.


### Guard Rails
- Only one reconfiguration will occur at a time for a given cluster If a
  cluster undergoing reconfiguration is modified we should drop reconfiguration
  replicas and overwrite the reconfigure value. If the alter changes no fields
  we should just modify the reconfigure value without dropping the replicas. We
  will issue a notice when overwriting an existing reconfigure.
- We must protect against creating a source/sink on a reconfiguring cluster and
  reconfiguring a cluster with sources/sinks. These clusters will need to use
  the non-scheduled mechanism (delete-before-create). This is due to a limitation
  where sources and sinks must only run on a single replica.
- Optionally, we may want to consider limiting timeout duration.

### Visibility
Reconfiguration replicas should be identifiable for purposes of reporting and
visibility in the UI. We may be able to do this by creating a prefix `reconfig-`
for reconfiguration replicas, alternatively, we could set a bool status on the
replica resource in the catalog.

### Details

#### Invariants:
  - Only one reconfiguration will be allowed at a time for a cluster. 
  - Unbilled replicas do not count towards reconfiguration and are not affected
    by reconfiguration.

#### Limitations:
  - Cannot be used on clusters with sources, sources should not be created on
    clusters with reconfigure replicas.

#### Replica Naming
Active replicas will continue with the same naming scheme (r1, r2, ...)
Reconfiguration replicas will use the `reconfig` prefix; ex `reconfig-r3`,
`reconfig-r4`, which will be removed once they are moved to active.

#### SQL
Introduce new SQL to define a delay duration for alter cluster.

`ALTER CLUSTER c1 SET (SIZE 'small') WITH ( CLEANUP TIMEOUT  5 minutes );`

This will alter the cluster and create new cluster replicas, but will not
remove existing replicas. Instead, it will set the schedule for the cluster
to `ClusterSchedule::Reconfigure` and set the deadline to `now + the provided
duration`. When CLEANUP TIMEOUT is not provided the cluster will behave as it
previously did, immediately tearing down and then creating the new replicas.

Initially, all reconfigurations will wait for the full timeout; however, when we
move to a hydration/parity cleanup detection mechanism, the reconfiguration may
finish before the timeout. The same SQL can be used in both cases.

#### Interactions with other schedules:

In introducing a second `ClusterScehdule` variant, we now have to account for
the interaction between these variants. To do so, `ClusterSchedule::Manual`
will be special-cased. We will allow multiple `ClusterSchedules` to be set
concurrently on a given cluster, however, when `ClusterSchedule::Reconfigure`
is one of them, it will be the only schedule that will cause a decision to be
emitted.

Benefits of this approach:
 - No other `ClusterSchedule` could interact with a cluster being reconfigured,
   which may reduce the complexity of behavior between interacting schedules.
 - Interactions with refresh. A resize on an active refresh to a smaller cluster
   size would complete (or timeout) rather than be quiesced by the refresh
   schedule decision. This should give a better indication of whether the new
   size would work on subsequent refreshes.

Downsides of this approach:
 - Replicas may be alive for the entire delay period rather than just the period
   it takes to perform the refresh, this could lead to additional/unexpected
   billing.

#### Catalog Changes
__Cluster__
Add a new ClusterSchedule Enum along with a `Reconfigure`  variant which would
be applied temporarily during reconfigure.
```rust
ClusterSchedule::Reconfigure {
  // timestamp for user-provided timeout
  timeout: Option<u64>,
  // V2 hydration config may end up here when we determine how it should be
  // used to determine reconfiguration is complete This could also want to be
  // a detection_mechanism: enum if we want different detection mechanisms that
  // the user can specify 
  // auto_detect: bool
}
```

#### Scheduling
`cluster_scheduling`'s `check_schedule_policies`, will be updated
to additionally check `check_reconfiguration_policy`, which sends
`ScheduleDecisions` with the decision `FinalizeReconfigure`, when the reconfigure timeout deadline has passed.

`check_reconfiguration_policy`, for V1, will trigger a `FinalizeReconfiguration` `ScheduingDecision` for clusters that
have a `Reconfigure` `ClusterSchedule` with a timeout value greater than the current time. In V2, we will add logic to this to also check for parity/hydration. At this point, it may need to be backgrounded as it's likely a much more costly check.

`Message::SchedulingDecisions` will be adjusted from 
`SchedulingDecisions(Vec<(&'static str, Vec<(ClusterId, bool)>)>)` to be
`SchedulingDecisions(Vec<SchedulingPolicyDecision>)`
```rust
enum SchedulingPolicyDecision {
  Refresh(Vec<(ClusterId, bool)>),
  FinalizeReconfigure(Vec<(ClusterId)>),
}
```

The logic for `handle_schedule_decision` will be directed to the correct
function based on the  variant of decision `handle_refresh_decision` or
`handle_finalize_reconfigure_decision`.

For each cluster `handle_finalize_reconfigure_decisions` will check the current
catalog to avoid conflicts with catalog updates since the `FinalizeDecision`
was sent. If there's a collision that should prevent the finalization, such as a
new size adjustment, then break; Otherwise, remove the active replicas and move
the reconfiguration replicas to be active. Finally, remove the `Reconfigure`
schedule from the cluster.


#### Cluster Sequencer
The following roughly defines the new logic for the cluster sequencer when
performing an alter cluster on a managed cluster.

There are two states to look at
1. the newly provided config
2. the reconfigue config (what is in the catalog)

First scenario, the cluster being altered has no `Reconfigure` `cluster_schedule`:
If cleanup_timeout is None:
 - use the old mechanism ( drop and replace )
If the new alter statement provides a `cleanup_timeout`: 
 - check for sources/sinks,
 - update the catalog with the changes and add a `Reconfigure` cluster schedule
 - deploy new reconfigure replicas

Second scenario, the cluster being altered is undergoing a reconfigure:
If cleanup_timeout is None,
 - Use the old mechanism - Drop and replace including all reconfigure replicas and remove the schedule.
If the new alter statement provides a `cleanup_timeout`:
 - If the new config matches the current config
   - bump the timeout
 - If the new config doesn't match the current config
   - check for sources/sinks
   - update cluster catalog with new schedule and config
   - drop reconfigure replicas
   - launch new reconfigure replicas

Note:
An `ALTER CLUSTER` statement that is run twice (double shot), either needs to
push out the timeout of an ongoing reconfiguration or needs to entirely replace
the existing reconfigure schedule dropping and recreating any reconfigure
replicas. I believe the former is the more useful behavior.

## Alternatives
### Multiple Schedule Interactions
We may choose to not special-case the `Reonconfigure` `ClusterSchedule`. In
doing so both Schedules can create `ScheduleDecision` messages in their handle
scheduling calls, the handler for each decision may happen in any order one
after the other with no delay. Importantly, handling decisions is a serial
process so no two decision handlers will be taking action at the same time. This
may lead to a replica  that was recently spinning up from an `ALTER CLUSTER`
statement being immediately shut down due to a refresh finalizing. Notably, if
the cluster was sized down the following refresh may fail due to OOD or OOM as
it never succeeded with the new size.

Benefits
  - Should be relatively easy to write this, as the expectation for any schedule
    adjustment should be idempotent, ensure the cluster/replicas are in the
    correct state after the decision action has finished.
Downsides
  - The interaction with `REFRESH EVERY` here is still weird. One might downsize
    during the active period of `REFRESH EVERY`, but refresh finishes before the
    reconfiguration finishing and the user will not know that the smaller size
    will OOM or OOD(out of disk) on the next run because the reconfigure size
    never had to finish.

### Allow cancelation?
We could additionally add a prior config field in `ClusterSchedule::Reconfigure`
that stores the `prior_config`. This would be the pre-reconfigure configuration
and could be used to cancel an upgrade. This would be done by running an
`ALTER CLUSTER` statement that does not provide a `CLEANUP TIMEOUT`, and whose
configuration matches the `prior_config`. This seems somewhat convoluted and
I don't know that allowing for cancellation is necessary as long as we allow for
adjustment of the current reconfigure timeout, and we allow for overwriting the
current reconfiguration entirely.


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

To me the reconfigure schedule exists only to interact with actions taken from
`ALTER CLUSTER` statements and is not really a property of the cluster, so I'm
not very bullish on this syntax. I could be convinced that this is more ergonimic.

## Open Questions

### How do we handle multiple schedules?
If this feature is introducing multiple schedules we'll need some rules
governing the interaction between schedules. There's a world where each schedule
needs to consider its interaction with all other schedules and that seems kind
of scary.

### How do we handle clusters with sinks and sources?
There is a current limitation around clusters with sources where the replication
factor must be 1 or 0 This being said we must disallow any reconfiguration
schedules for clusters with sources (and sinks?). This is a new annoying issue
as we now allow clusters to perform both compute and source roles. We should
consider some ways to handle this, one potential is to keep source activity to
a single replica in a multi-replica cluster. Alternatively, we just don't allow
this, and we add some big disclaimers to clusters with sources.

### How does a multi-subscriber coord work with schedules?
We don't want two schedule decisions to be acting at the same time. Currently,
this isn't an issue, but it seems a bit scary. Is this a concern?

### How will this impact coord delays?
Particularly when we look at hydration-based, this may add a reasonable
complexity/time to the `coord` loop, which could lead to stalls. We may need to
consider spinning off cluster scheduling decisions to a task.

### Should ClusterSchedule::Reconfigure be a schedule?
Since reconfiguration is only a temporary action it's somewhat reasonable that
we wouldn't want to count it as a "schedule" option. If we're not doing this,
we'd still have to define and persist some field to alert us of the timeout
deadline, which likely means adding `reconfiugration_deadline: option<u64>`
and `reconfiguration_mechanism` to the durable cluster object. I don't think
this plays well with other types of scheduling and doesn't properly encapsulate
the reconfiguration behavior, it's also less extensible when we think about
different configurations for reconfiguration we may want in the future.

### Hydration-Based Scheduling
There is currently the beginning of the mechanism to look at hydration
based metrics for scheduling in `compute-client/src/controller/
instance:update_hydration_status`, this works very well for a single data flow,
but can't gauge the hydration status of a replica as a whole.

What we likely want is a mechanism [discussed here](https://github.com/MaterializeInc/materialize/issues/20010#issuecomment-2058563052), where we
compare the data-flow frontiers between reconfiguration and active replicas,
and when the reconfiguration replicas have all data flows within some threshold
of the active, we send a message which invokes a `RecofigurationFinalize`
scheduling decision. This decision may have to be a different decision policy
then the standard `ReconfigurationFinalize` as we may want to perform some
additional checks of the hydration status to ensure new data flows between when
the message got sent and when the action is being taken have hydrated.

__Potential challenges with using hydration status__:
It is difficult to know when "hydration" has finished because:
  - New data coming in may change the timestamp and lead to hydration status inaccuracies
  - New data-flows can be created ad-hock which will need to be "hydrated"
  - hydration_status errors related to `evnironmentd` restart?
    (https://materializeinc.slack.com/archives/CTESPM7FU/p1712227236046369?thread_ts=1712224682.263469&cid=CTESPM7FU)
Some of these can be resolved by only looking at data flows that were installed
when the replica was created. It seems like comparing active and reconfiguration
replicas will likely work better than attempting to gauge when hydration has
finished for a given replica. It also may allow us to shorten the timeout if all
replicas for a cluster undergoing reconfiguration are restarted.

Ideally, no extra syntax is needed for hydration. We can implement it directly using the
current syntax and it will just finalize the reconfiguration before we hit the timeout.
