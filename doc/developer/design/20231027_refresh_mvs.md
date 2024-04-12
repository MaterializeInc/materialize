# Tuning Freshness on Materialized Views

- Associated:
  - Big tracking issue: [#26010 `REFRESH` options](https://github.com/MaterializeInc/materialize/issues/26010)
  - Main epics:
    - [#22878 [Epic] Refresh options for materialized views](https://github.com/MaterializeInc/materialize/issues/22878)
    - [#25712 [Epic] Automatic cluster scheduling for REFRESH EVERY matviews](https://github.com/MaterializeInc/materialize/issues/25712)
  - Older issues:
    - [#13762: CREATE MATERIALIZED VIEW could support REFRESH modifiers](https://github.com/MaterializeInc/materialize/issues/13762)
    - [#6745: Consider WITH FREQUENCY option for sources, views, sinks.](https://github.com/MaterializeInc/materialize/issues/6745)
    - [#21479: Support for static data collections](https://github.com/MaterializeInc/materialize/issues/21479)
  - Slack:
    - [channel #wg-tuning-freshness](https://materializeinc.slack.com/archives/C06535JL58R/p1699395646085619)
    - [big design thread in #epd-sql-council](https://materializeinc.slack.com/archives/C063H5S7NKE/p1699543250405409)
    - [`REHYDRATION TIME ESTIMATE` thread in #epd-sql-council](https://materializeinc.slack.com/archives/C063H5S7NKE/p1712165305916299)
  - Notion:
    - [Tuning REFRESH on MVs: UX](https://www.notion.so/materialize/Tuning-REFRESH-on-MVs-UX-1abbf85683364a1d997d77d7022ccd4f)
    - [Compute meeting on automatic cluster scheduling](https://www.notion.so/materialize/Compute-meeting-on-automatic-cluster-scheduling-ce353b8af52e449d8784241c4a1c0585)

## TLDR

Users should be able to configure materialized views to compute result changes less frequently, but cheaper. It turns out that we can do this by
- turning on a replica only when we want a result refresh, and
- rounding up the timestamps of data and frontiers that go into the Persist sink to the time of the next result refresh.

## The Problem

Materialize keeps materialized views fresh by running a dataflow continuously. These dataflows consume significant resources, but the resource usage is worth it for a large class of use cases. Therefore, the use cases that are the main focus of Materialize are the ones that can derive value from always fresh computation results, termed Operational Data Warehouse (ODW) use cases.

ODW use cases typically focus on the most recent input data (hot data). However, there is often a significantly larger amount of older data lying around (cold data). The cold data is often of the same kind as the hot data, and therefore we postulate that users will often want to perform the same or similar computations on these. In this case, it would be convenient for users to simply use Materialize for both hot and cold computations, and not complicate their architectures by implementing the same computations in both Materialize and a different, traditional data warehouse.

The problem is that Materialize is currently not so cost-effective for running a materialized view on cold data, because
- there is significantly more cold data than hot data, so a much larger compute instance is needed to keep compute state in memory, and
- keeping results always fresh is not so valuable for cold data.

This currently prevents us from capturing these cold use cases, even though they feel very close to our hot, core ODW use cases.

## Success Criteria

We should give users a way to trade off freshness vs. cost.

Note that other systems also expose similar knobs. Making this trade-off tunable by users is an important point also in the [Napa paper](http://www.vldb.org/pvldb/vol14/p2986-sankaranarayanan.pdf). Snowflake's dynamic tables allow the user to configure a ["target lag"](https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh#understanding-target-lag).

Criteria:

1. Importantly, our solution shouldn't complicate the user's architecture. For example, the user shouldn't need to externally schedule jobs that perform an array of non-trivial tasks (and manage their possible failures), such as managing multistep table updates (e.g., first delete old results than insert new results) and turning replicas on and off. This is a crucial criterion, because if we can only propose a complicated solution, then users won't be better off with our solution than with using a different system for the cold data.

2. Another critical requirement is that our solution should achieve a significant cost reduction. This is because the cold data is often significantly larger than the hot data. For example, at one important user, the hot data is from the last 14 days, while the cold data is the same data but from the last 5 years.

3. The slowly updated results should be queryable together with other, normal objects. This is important because sometimes we might want to join the cold results with other objects that have normal refresh rates. Also, one important user needs a unified view of the hot and cold results, so we should be able to union them.

4. We shouldn't hold back compaction of inputs (indexes, most importantly, but also Persist) to the time of the last refresh. This is because a significant amount of time can pass between two consecutive refreshes, and therefore holding back compaction would lead to the buildup of significant amounts of in-memory state in input indexes.

## Scope and Limitations

We are targeting the new refresh options only to materialized views, but not indexes for now. Note that one can create an index that is refreshed at a specified interval simply by creating a normal index on top of a `REFRESH EVERY <interval>` materialized view.

We are not aiming to cover the entire range of possible freshness-cost trade-off settings. In particular:
- We are not aiming here to make results more fresh at a higher cost than what Materialize can provide by default. For efforts in that direction, see [#19322: Re-enable TIMESTAMP INTERVAL source option](https://github.com/MaterializeInc/materialize/issues/19322).
- A more subtle scoping consideration is that we are also not targeting the range of settings that would require incremental computation with on-disk computation state. Instead, we now focus only on the easy case that is the range of freshness where redoing the entire computation periodically yields a lower cost than having a continuously running dataflow. This means that setting a refresh interval of less than 1 hour will not work well in most cases. It might be that even a few-hour refresh interval won't make using this feature worthwhile. Note that for one important user, a 1-day refresh interval is ok. Later, we might be able to cover this missing intermediate range of freshness by entirely different approaches:
   - When we have robust [out-of-core in Compute](https://materializeinc.slack.com/archives/C04UK7BNVL7/p1700084826815849) (probably with auto-scaling), we might be able to trade off freshness vs. cost by simply using a much smaller replica than the cluster's total memory needs, but supplying input data at a much larger tick interval than the default 1 sec. For example, we might supply new input data at every 10 mins, and hope that even with a lot of swapping, we'll be able to process the update in 10 mins.
  - We could suspend replica processes and save their entire state to S3 and a local disk cache, and restore it when a refresh is needed. [There are existing techniques for saving process state, e.g., CRIU](https://faun.pub/kubernetes-checkpointing-a-definitive-guide-33dd1a0310f6).

We are not aiming to provide hard guarantees that we'll refresh a materialized view at the specified time. This is because the solution approaches considered here are based on spinning up replicas near the refresh times, which is not always possible due to AWS capacity issues. Therefore, timely completion of refreshes are only on a best-effort basis, with possibly tunable knobs by power users that control the trade-off of cost vs. chances that refreshes will complete on time.

The below proposal only considers timelines that have totally ordered timestamps, and whose timestamps relate to wall clock time (as opposed to, e.g., transaction sequence ids of some system).

## Solution Proposal

### `REFRESH` options syntax

We propose providing users various `REFRESH` options on materialized views:
```
CREATE MATERIALIZED VIEW ... [WITH (
  [REFRESH [=] {
    ON COMMIT
    | EVERY <every_interval> [ALIGNED TO <aligned_to_timestamp>]
    | ON SCHEDULE '<cron>'
    | AT {CREATION | <timestamp>}
  }]+
)]
```

For example:
```SQL
CREATE MATERIALIZED VIEW mv1 WITH (
    REFRESH AT CREATION,
    REFRESH EVERY '1 day' ALIGNED TO '2023-11-17 03:00'
) AS SELECT ...
```

### `REFRESH` options semantics

Some of the `REFRESH` options can be combined and/or supplied multiple times. (If two refreshes would fall on the exact same moment, then only one refresh is performed at that moment.) The meaning of the various refresh options are as follows.

#### Refresh `ON COMMIT`

This is the default when not supplying any options, i.e., our current behavior of refreshing the materialized view when any changes to our inputs are committed. (The term `REFRESH ON COMMIT` is present in various other database systems, so should be familiar to users.) It is an error to specify this together with any of the other `REFRESH` options.

#### Refresh `EVERY <every_interval> [ALIGNED TO <aligned_to_timestamp>]`

We'll refresh the materialized view periodically, with the specified interval, e.g., `1 day`.

The purpose of `<aligned_to_timestamp>` is to specify the "phase". For example, users might want daily refreshes to happen outside business hours. `<aligned_to_timestamp>` is an absolute time, e.g., if today is `2023-11-16`, then I might want to create a daily refresh materialized view where the first of the periodic refreshes happen at `2023-11-17 03:00`, which will cause all later refreshes to also happen at `03:00`.

The default `<aligned_to_timestamp>` is `mz_now()` at the moment of creating the materialized view.

It is allowed to specify `REFRESH EVERY` more than once. These can have different intervals and/or `ALIGNED TO` timestamps.

Conceptually, refreshes extend infinitely to the past and future from the `<aligned_to_timestamp>`, but in practice the `least_valid_read` of the MV's input ID bundle constrains how far to the past they go. More formally, when considering the TVC of the MV, `REFRESH EVERY p ALIGNED TO a` means we have refreshes at `a + i*p`, where `i = -∞ .. ∞`. In practice, we have to project this to a pTVC, so `i` won’t actually start from `-∞`, but from some `s` such that `a + s*p` can actually be computed based on the input pTVCs. When `ALIGNED TO` is not given, `s` should usually be 0 (or smaller in case of custom compaction windows on inputs). This is because `a` defaults to the creation time of the MV, so for `s=0`, `a + s*p` will be simply the creation time of the MV, and it makes sense that any input pTVCs include the creation time of the MV at the moment of the creation of the MV. In the code, this is made sure by the combination of purification choosing `mz_now()` (the default of `ALIGNED TO`) to be the oracle read timestamp, and immediately grabbing read holds already in purification.

We also need to carefully consider what happens with a `<aligned_to_timestamp>` that is in the past. This can be useful in two situations. First, the user shouldn't need to update her `<aligned_to_timestamp>` every time when redeploying the materialized view (e.g., through DBT). For example, let's say that on `2023-10-16` the user created an MV that is refreshed daily at 03:00, by specifying `ALIGNED TO 2023-10-17 03:00`. Let's say that now a month later on `2023-11-16` the user is making a small tweak in the definition of the MV, but wants to keep the same refresh schedule. If we didn't allow a `<aligned_to_timestamp>` in the past, the user would have to update her `<aligned_to_timestamp>` to be also a month later than before, i.e., `ALIGNED TO 2023-11-17 03:00`. However, instead I propose to simply accept the original `<aligned_to_timestamp>`, and ignore those refreshes that would happen before our `least_valid_read`.

Another consideration for `<aligned_to_timestamp>`s in the past is that we'd like to be able to actually perform some refreshes in the past when the compaction window of the materialized view reaches to the past. For example, if a table has a compaction window of 30 days, and on this table the user creates a materialized view with a daily refresh, we should actually perform all the refreshes for these last 30 days.

(We also [considered](https://materializeinc.slack.com/archives/C063H5S7NKE/p1699883928733719?thread_ts=1699543250.405409&cid=C063H5S7NKE) an alternative design, where instead of `ALIGNED TO` we would have had `STARTING AT`, where refreshes wouldn't have extended to the past. This would have made it possible to precisely control the time of the first refresh, e.g., the user could say that the MV should refresh every day, starting on Friday next week. But no one had a realistic use case for this, and having the theoretically more elegant ALIGNED TO had various minor advantages, so we consciously opted out of having this precise control for the time of the first refresh. But if someone comes up with a compelling use case for this at some point, I could still add STARTING AT with maybe a day of work.)

#### Refresh `ON SCHEDULE '<cron>'`

To support more complex refresh schedules, we'll also support [cron schedule expressions](https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules). For example, the user whose needs prompted this work would like to have daily refreshes, but not on the weekends. While this is possible to express using `REFRESH EVERY <every_interval> [ALIGNED TO <aligned_to_timestamp>]`, it is quite cumbersome: one would need to specify a `REFRESH EVERY '7 days'` 5 times: for each of the weekdays when a refresh should happen.

The implementation for refreshes scheduled by cron expressions will be similar to `REFRESH EVERY`, except for the determination of when the next refresh should happen.

#### Refresh `AT {CREATION | <timestamp>}`

We perform just one refresh of the MV at the specified time. (`REFRESH AT` can be given multiple times.) Note that if the only given refresh options are all `AT` options, then there will be a last refresh, after which the MV will never be refreshed again. This will allow for certain performance optimizations in dataflows that consume the MV as input ([#23179](https://github.com/MaterializeInc/materialize/issues/23179), 
[26571](https://github.com/MaterializeInc/materialize/issues/26571)).

_Before the first refresh is performed, MVs are not queryable_ (queries will block until the first refresh completes), because their `since` will be initially set to the time of the first refresh. The first refresh can be a significant time away from the time of creating the MV. For example, the first refresh of an MV that is updated daily could be at night, while the MV is probably created during the day. In such cases, the user would have to wait a long time to actually start using the MV. To prevent this annoying situation, users might often want to add an extra refresh at the time of creation by specifying `REFRESH AT mz_now()`. A syntactic sugar for this is `REFRESH AT CREATION`. For example,
```SQL
CREATE MATERIALIZED VIEW mv1 WITH (
    REFRESH EVERY '1 day' ALIGNED TO '2023-11-19 03:00',
    REFRESH AT CREATION,
) AS SELECT ...`
```

#### Refresh `NEVER`

Originally, we were planning to have a `REFRESH NEVER` option, which would perform only one refresh, at the creation time of the materialized view, and then never again. However, this is superseded by the `REFRESH AT CREATION` option. I'd say this is a better syntax than saying "never", because it's actually not "never": we do perform one refresh (at the creation time).

### Implementation Overview

The two main components of the implementation is that we

_A._ automatically turn on a replica at times when we want to perform a refresh, but otherwise keep the MV's cluster at 0 replicas, and

_B._ round up the timestamps of data and frontiers that go into the Persist sink that is writing the results of the materialized view.

#### _A._ Automatically Turning on a Replica For Each Refresh

_A._ alone can already achieve [success criteria](#success-criteria) 1. and 2. How this works is that the MV is put on a cluster that has 0 replicas most of the time, and Materialize automatically adds a replica briefly when it is time to perform a refresh. This replica rehydrates, and computes result updates for the MV. After we observe that the MV's upper frontier has passed the time of the needed refresh, we automatically drop the replica.

We could implement this, for example, in `ActiveComputeController::process`: We'd periodically check whether we need to spin a replica up or down: We'd check the progress of the materialized view, and
- if we need to be performing a refresh, but we don't have a replica, then create a replica;
- if we don't need to be performing a refresh currently, but we have a replica, then drop the replica.

However, _A._ alone falls short of the rest of the success criteria: The problem is that the upper frontier of the materialized view would be stuck at the time when the replica was dropped. This would mean that it wouldn't satisfy 3., i.e, the slowly updated materialized view being queryable together with other, normal objects. This is because the sinces (and uppers) of other objects would be far past the stuck upper, making timestamp selection impossible for such queries that involve both a rarely refreshed materialized view and normal objects (even in `SERIALIZABLE` mode). It would also not satisfy criterion 4., because compaction of input objects would be held back
by clusters without replicas, because we expect that the dataflows on these clusters will be spun up again and continue processing data from where they left off, so we can't compact that data away.

#### _B._ Rounding Up Timestamps Going Into the Persist Sink

We can solve the shortcomings of _A._ by rounding up timestamps that go into the Persist sink that is writing the results of the materialized view. The modified timestamps should fall to the time of the next desired refresh.

Importantly, we have to round up both the timestamps in the data and also the frontiers. This is because we want the MV's upper frontier to jump ahead to the time of the next refresh (i.e., to the end of the current refresh interval), indicating to the rest of the system that the MV won't receive data whose timestamp is before the time of the next refresh. This is needed because for normal MVs a replica sending us `FrontierUpper`s would keep ticking the MV's upper forward, but in our case we want to have 0 replicas for most of the time. So, what we do instead is to make the upper jump forward all the way to the next refresh while we have the replica, and then we don't need to manipulate the upper later while we have 0 replicas.

Contrast the above behavior of upper to what _A._ alone would get us: With only _A._, the upper would be mostly stuck near the _beginning_ of the current refresh interval (the interval between two consecutive refreshes). With _B._, the upper will be mostly standing at the _end_ of the current refresh interval. This behavior of upper gets us halfway to achieving success criterion 3.: The materialized view's upper won't impede timestamp selection of queries that also involve other objects, because the MV's upper will be far ahead of the sinces of other objects that are refreshed normally.

We still need to consider how the since frontier of the materialized view moves. Crucially, it shouldn't jump forward to near the time of the next refresh, because that would again preclude timestamp selection when querying the MV together with other objects, violating success criterion 3., because the since of the MV would be far ahead of the uppers of normal objects. When initially designing this feature, we mistakenly thought that without some further code modifications the since would jump forward to the next refresh because that is the since that the Compute controller would request on the MV: tailing the upper by 1 second (or more generally, by the compaction window of the MV). However, it turns out that the Adapter is keeping the sinces of all objects from jumping into the future in any case: The Coordinator is keeping read holds on all objects, and ticks these read holds forward every second. ([See `Coordinator::advance_timelines`](https://github.com/MaterializeInc/materialize/blob/b89d4961f8a8ccd172be2dc46f7b83bb00b9b871/src/adapter/src/coord/timeline.rs#L645).)

A code modularity argument could be made for not relying on the Adapter to hold the sinces near the current time: One could say that the Compute Controller should be keeping sinces reasonable without relying on other components (the Adapter). However, I would counter this argument by saying that it is the Adapter's responsibility to keep sinces from moving into the future, because it's the Adapter that will be issuing the queries that need the sinces to be near the current time. Therefore, I'd say it's ok to rely on Adapter holding back the sinces from jumping far into the future.

Note that the compaction of downstream objects, e.g. an index on the materialized view, will be similarly controlled by the Adapter, even if their uppers jump forward together with the materialized view.

## Further Requirements and Refinements (Now or Soon)

### Starting the Refresh Early

TODO: update this section with the latest discussion on `REHYDRATION TIME ESTIMATE`.

Notice that if we turn on the replica at exactly the moment when a refresh should happen, then the MV's upper will be stuck for the time that the refresh takes. This is because the upper would still be at the logical time of the refresh during most of the time of the replica's rehydration. (More specifically, until the replica is finished processing the input data whose timestamps are before the time of the refresh, which are rounded up to the time of the refresh.) This would violate success criterion 3. for the time of performing the refresh, because other objects' sinces would keep ticking forward, and would therefore have no overlap with our special materialized view's since-upper interval.

There is a workaround for this until we properly fix it: If there is a specific query involving a `REFRESH EVERY` MV and some other object that one knows will need to be run during a refresh, then creating another (normal) MV (or view + index) on that query would ensure that the sinces of other involved objects are held back until the refresh completes, so the data would be queryable under serializable isolation.

A proper fix would be to start up the replica a bit before the exact moment of the refresh, so that it can rehydrate already. For example, let's say we have an MV that is to be updated at every midnight. If we know that a refresh will take approximately 1 hour, then we can start up the replica at, say, 10:50 PM, so that it will be rehydrated by about 11:50 PM. At this point, most of the Compute processing that is needed for the refresh has already happened. Now the replica just needs to process the last 10 minutes of input data until midnight at a normal pace. We let the replica run until the MV's upper passes midnight (and jumps to the next midnight), which should happen within a few seconds after midnight. Note that before midnight, queries against the MV will still read the old state (as they should), because the new data is written at timestamps rounded up to midnight.

How do we know how much earlier than the refresh time should we turn on the replica, that is, how much time the refresh will take? In the first version of this feature, we can let the user set this explicitly by something like `REFRESH EVERY <interval> EARLY <interval>`. Later, we should record the times the refreshes take, and infer the time requirement of the next refresh based on earlier ones. Note that this will be complicated by the fact that we have different instance types [that have wildly differing CPU performance](https://materializeinc.slack.com/archives/CM7ATT65S/p1697816482502819).

### Logical Times vs. Wall Clock Times

Note that generally we can't guarantee that refreshes will happen at exactly the wall clock times specified by the user. For example, the system might be down due to scheduled maintenance at the time when a refresh should happen. However, we should still perform the refresh at the logical time specified by the user. In other words, each refresh should consider exactly the data whose timestamps were at most the refresh timestamp, even if the refresh happens a bit later in wall clock time. The proposed solution naturally satisfies this requirement, as the rounding up is performed on logical timestamps.

### Delta Join Tweaks

TODO

## Possible Future Work

- REFRESH ON DEMAND (and ALTER ...)
- When we have custom compaction windows, make sure stuff works. E.g., `<aligned_to_timestamp>` in the past should actually start in the past.

## Minimal Viable Prototype

A prototype is available [here](https://github.com/ggevay/materialize/commit/f1f279cad786f4b3cb96ac4e376fa1bd90e160ea) (written together with Jan). It simply inserts an operator in front of the Persist sink, which rounds up the timestamps as proposed above. The prototype does it for all MVs (with a fix interval), so there is no SQL syntax in the prototype. Automated replica management is not implemented yet, but one can manually drop and create replicas.

## Console

We need to present information as both "current through " and "queryable at " to reduce the confusion

...

## Docs / Gotchas

- Without `REFRESH AT CREATION`, an MV is not queryable before the first refresh. To avoid beginner users being surprised by this fact, our docs should include `REFRESH AT CREATION` in the example commands (in addition to e.g., `REFRESH EVERY ...`).
- consistency guarantees are different: changes of the input not immediately reflected, even if we query the MV at a logical time that's later than the input change.
- no guarantee for completing the refresh on time
- When a version upgrade changes the output of an MV's computation (e.g., because of a bugfix or an evolution of an unstable feature), an MV will reflect the change not immediately after the version upgrade, but only after the next refresh. Note that if the MV already had its last refresh (e.g., `REFRESH AT CREATION` with no other refresh options), then the output will never actually change.

## Alternatives

### Tables Managed by Externally Scheduled Jobs

Even without any new features, users could keep the data in a table instead of an MV, and schedule jobs on their own to run refreshes. The problem with this approach is that it would place a considerable burden on users: they would have to schedule external jobs, deal with possible failure scenarios of these jobs, etc. When we suggested this approach as a workaround to a big user, they told us that an important advantage of Materialize in their eyes is simplicity (compared to their existing architecture), which would be lost with this approach, and they are willing to pay for the simplicity.

### `REFRESH NEVER` MVs Periodically Recreated by Externally Scheduled Jobs

`REFRESH NEVER` MVs would probably be also easy to implement. These would be sealed forever after their initial snapshot is done. With these, a user could manually simulate `REFRESH EVERY` by dropping and recreating the MV at every refresh. This would require users to manually create and drop also replicas around the time of the refresh, so this approach has the same drawback of the above, tables approach: it is too complex for users, as it requires complex external job scheduling. Another downside is that users would have to drop and recreate all dependants on every refresh.

### `REFRESH ON DEMAND` MVs

We could add a `REFRESH ON DEMAND` option to MVs, and a `REFRESH MATERIALIZED VIEW` command that would trigger a refresh. Users could then schedule an external job that would give the `REFRESH MATERIALIZE VIEW` command periodically. Replicas could be automatically managed by us, so this would be a less complex external job scheduling burden for users as the two above alternatives.

However, the problem with this is that then we wouldn't know where to move the upper of the MV after a `REFRESH MATERIALIZED VIEW` command completes a refresh, because we couldn't know when the next refresh will happen. This would mean that we'd have to either give up on success criteria 3. and 4., or we'd have to do some more implementation:
- We'd need a task that periodically ticks forward the uppers and sinces of the MV even when there is no replica for the MV.
- We'd still need to manipulate the timestamps that go into the Persist source, but this time we wouldn't know at dataflow creation where to round up the times, but would have to instead tell this to the roundup operator at every refresh.

Considering that the implementation wouldn't be simpler for us, and it would be more complicated for users (externally scheduled job), we reject this alternative for now. (In the future it might happen that some other use case will require `REFRESH ON DEMAND` MVs, and then we can implement also this, in addition to `REFRESH EVERY`.)

### Just Creating a Replica When a Refresh Should Happen

As mentioned before, simply keeping the cluster of an MV at 0 replicas most of the time and creating a replica when a refresh should happen would satisfy success criteria 1. and 2., but not 3. and 4. We initially suggested this as a workaround [to a big user](https://materializeinc.slack.com/archives/C053EPHMU05/p1698173967255099), but it turned out that it's not suitable for them, because they would like to consume a unified view of the cold and hot results through PowerBI, which seems unable to perform this unioning. Therefore, they'd need to `UNION` the cold and hot results inside Materialize, for which they need success criterion 3. Also, not satisfying success criterion 4., i.e. holding back Persist compaction of inputs to the time of the previous refresh, would be "low-to-medium scary".

### Implementation Alternative: Creating a New Dataflow at Each Refresh

(The above alternatives are alternatives to `REFRESH EVERY` MVs, but this alternative is just about the implementation details of `REFRESH EVERY` MVs.)

This approach would need similar replica management as the proposed solution, but instead of rehydrating an existing dataflow, we would always create a new dataflow to run the refresh. An advantage of this approach is that these dataflows could be single-time dataflows, and thus rely on one-shot SELECT optimizations. However, this approach would be more complicated, as we would have to separate the lifecycle of dataflows from the lifecycle of MVs. These new dataflows would be a kind of hybrid between Peek dataflows and MV dataflows, and would require new code in various parts of the Compute Controller. As we are under some time pressure to implement this feature, we opted for the simpler solution proposed in this document.

### Implementation Alternative: Suspending the Replica Process to Disk

As mentioned in the [scoping section](#out-of-scope), an alternative implementation that would cover also intermediate refresh intervals would be to suspend the replica process and save its entire state to S3 and a local disk cache, and restore it when a refresh is needed. [There are existing techniques for saving process state, e.g., CRIU.](https://faun.pub/kubernetes-checkpointing-a-definitive-guide-33dd1a0310f6) However, this would be a much bigger project than the proposed solution.

## Automated Cluster Scheduling for `REFRESH EVERY` MVs

TODO: updated discussion

## Introspection / Observability

For showing the `REFRESH` options of each materialized view, I'm planning to create a new table in `mz_internal` called `mz_materialized_view_refresh_options`, which will have a row for each non-trivial refresh option of each MV. The columns will be `(materialized_view_id text, refresh_interval interval, refresh_interval_aligned_to timestamptz, refresh_at timestamptz)`, and only those columns will have a non-null value that are relevant for the refresh option that the row is about (e.g., a `REFRESH EVERY` would be non-null in interval and interval_aligned_to). `REFRESH ON COMMIT` is considered a trivial refresh option, and won't show up in the table. (See Nikhil's [msg](https://materializeinc.slack.com/archives/C06535JL58R/p1701228087745239?thread_ts=1700595291.424189&cid=C06535JL58R) on an old design thread.)

For showing the previous and next refresh of each MV, I'm planning to create a new table in `mz_internal` called `mz_materialized_view_refreshes (materialized_view_id text, last_completed_refresh timestamptz, next_refresh timestamptz)`. I'm thinking to update it in `Controller::record_frontiers` (where `mz_frontiers` is also updated). The values will be calculated as follows:
- `next_refresh` will usually be the write frontier of the MV's storage collection, unless:
  - The write frontier is 0. This happens when the first refresh hasn't completed yet, in which case we need to figure out the first refresh: we'll call `RefreshSchedule::round_up_timestamp` on the MV's `as_of`.
  - The write frontier is `[]`. This happens when the MV is after its last refresh, and will never be refreshed again. We could represent this simply with a `NULL`.
- `last_completed_refresh` is roughly `RefreshSchedule::round_down_timestamp(next_refresh - 1)`, plus special handling for the `[]` write frontier case. (`round_down_timestamp` will be a new function, which will be the same as `round_up_timestamp`, but backwards.)

For seeing whether the last refresh is being late, the user can run `EXPLAIN TIMESTAMP` in `STRICT SERIALIZABLE` mode, and look at can respond immediately. If it's false, then the last refresh's completion is overdue. Another way to get the same information would be to check if `mz_materialized_view_refreshes.next_refresh < now()`.

For showing cluster schedules, I'm thinking of doing a similar thing as in `mz_materialized_view_refresh_options`, because eventually the `SCHEDULE =` option will allow for specifying multiple schedules. So I'd create a new table in `mz_internal` called `mz_cluster_schedules`, and it would have a row for each non-trivial (non-`MANUAL`) schedule option of each cluster. This would currently be only `SCHEDULE = ON REFRESH`. Columns would be `(cluster_id text, refresh_rehydration_time_estimate interval)`. (Eventually, we'll probably also want a `next_scheduled_turn_on`, but this doesn't seem so urgent. It will get more important when we'll be choosing the warmup time automatically.)

For seeing whether a cluster is currently turned on, the user can simply look at `mz_cluster_replicas`, because we currently turn clusters On/Off by just creating/dropping replicas. We might also add a builtin view for showing this information in a more focused way.

For the automatic cluster scheduling history, the user can look at `mz_audit_events`. This has a `details` column, which is a JSON blob, where I'm planning to add the `reason` for turning on a cluster, i.e., which materialized views were in need of a refresh. (See Nikhil's comment [here](https://github.com/MaterializeInc/materialize/pull/26401#pullrequestreview-1981986544).) There is also the `mz_cluster_replica_history` view, which takes its info from `mz_audit_events`, and presents the info in a nicer form. I could add a new reason column to this view.

We'll also want to show rehydration times from the last several refreshes, to help users set the `REHYDRATION TIME ESTIMATE` of clusters. I'm thinking to create a new table `mz_internal.mz_compute_hydration_history (replica_id text, rehydration_time interval)`, which would have one row for each replica creation, and it would show the time it took to rehydrate the replica when it was created. (The user can join this with `mz_cluster_replica_history` to know which cluster the replica belonged to, replica size, etc.) Btw. this doesn't need to be constrained to clusters involving `REFRESH` MVs; this info seems generally useful for any compute cluster. If we want to make it even more useful generally, we might want to add one row not just for each replica creation, but also each replica restart, so that we'll show the rehydrations that happen at system upgrade restarts. In this case, we'll probably need also a `time` column, and then `(replica_id, time)` would be a composite key. For this general version, we might have to truncate the relation to keep it from growing too big.

We might want to also track the time it takes to actually perform a refresh, assuming that the replica is already hydrated. This will often take <1 sec, but if the MV's storage is big and/or there are many changes, then it might take more.

## Rollout

I plan to first implement the feature without automatically turning replicas on and off, and release the feature in this half-finished state behind a feature flag. At this point, we can already show the feature to the customer whose needs prompted this work, and they can validate it to some degree. At this point, the user will need to manually manage the replicas. Update: this is done, the user is using it in their prototype, managing replicas with GitHub Actions at hardcoded times.
