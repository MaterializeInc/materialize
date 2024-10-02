# Compute Operator Hydration Status Tracking

- Associated: https://github.com/MaterializeInc/database-issues/issues/7164
- Associated: https://github.com/MaterializeInc/database-issues/issues/6518

## The Problem

In Compute today, progress is tracked at the level of dataflows.
Each dataflow has a write frontier that, ideally, advances continually and lets us know up to which point a dataflow has processed its input data and how far it lags behind its inputs.
However, if a dataflow's frontier does not advance, our means of observing its progress are very limited.
The dataflow will appear to be stuck and discovering where it is stuck requires significant knowledge about Materialize's introspection sources and Timely's reachability tracking, which we can't expect from users.

This issue becomes particularly apparent during dataflow hydration, i.e., the time during which a dataflow processes the snapshot data from its sources.
Since source snapshots can be large, dataflow hydration can take significant time, in the order of minutes or even hours.
During this time users don't receive any progress updates about the dataflow.
This is bad UX and also prevents users from finding out about hydration bottlenecks that they might be able to improve upon by changing their queries.

## Success Criteria

* Users have access, in SQL, to information about intra-dataflow hydration progress.
* Users can use this information to gain some confidence about dataflow hydration progressing as expected.
* Users can use this information to identify parts of their query plans that are bottlenecks during hydration.

## Out of Scope

* Showing intra-dataflow hydration progress information in the Console.
* Providing estimates for remaining dataflow hydration time.
* Tracking intra-dataflow progress after completed hydration.

## Solution Proposal

Dataflows consist of connected operators through which data flows.
Each of these operators has a write frontier communicating up to which times it has finished producing outputs.
Like the dataflow frontier, operator frontiers can be used to detect completed hydration, but at a finer granularity than dataflow granularity.
This design proposes that Compute reports the frontier-derived hydration status of a useful subset of all dataflow operators and exposes these status in a SQL relation.
Users can then query this relation directly, and we can build further abstractions based on it, like visualizations in the Console.

While we could report hydration status for all operators, we prefer to not do so for several reasons:

* **Efficiency:** Given the number of operators in a typical dataflow, the cost of tracking all their frontiers would be significant.
  While we can't avoid some overhead from adding this feature, we would like to keep it as small as possible.
* **Understandability:** Many of Compute's dataflow operators are implementation details that don't relate to the computed query (e.g. logging operators).
  Showing information about these operators to users would at best introduce needless noise and at worst be actively confusing.
* **Technical Feasibility:** Attaching hydration monitoring to every dataflow operator is difficult given the current code structure.
  A good solution would likely require us to implement support into the Timely Dataflow runtime.

Instead, this design proposes to only report hydration status of dataflow regions that correspond to LIR nodes.
LIR is the low-level intermediate representation used by Materialize's optimizer.
Users can show their queries as LIR plans using the `EXPLAIN PHYSICAL PLAN` syntax.
LIR plans are the lowest level of query representation we document and that (advanced) users can therefore be expected to understand.
LIR nodes map directly to groups of operators in a dataflow, so it is possible for us to collect hydration status information at this level.

While it would be desirable to also provide hydration progress information at a higher level, we consider that outside the scope of this design.

Reporting LIR-level hydration status requires a number of pieces that don't exist yet in our code:

1. [Assigning IDs to LIR nodes during query planning](#lir-node-ids)
2. [Collecting LIR-level hydration status during dataflow processing](#hydration-status-logging)
3. [Reporting LIR-level hydration status to the controller](#reporting-introspection-data)

While there is some amount of lifting required to implement all these pieces, our hope is that they will prove generally useful.
(1) allows mapping low-level dataflow information back to `EXPLAIN` plans and will be useful for making our internal memory visualizer user-facing.
It is also a precondition for surfacing runtime information, like memory usage, in `EXPLAIN` plans.
(3) gives us a reliable way to expose dataflow introspection information in SQL, which will be useful in alleviating some of the pain currently caused by the bad ergonomics of our per-replica introspection mechanism.

### LIR Node IDs

To make LIR nodes identifiable, we assign IDs to them (referred to as _LIDs_ in the following).
LIDs need to be unique only within a dataflow, as they can be namespaced by the dataflow ID, or by the `GlobalId` of the SQL object that created the dataflow.

This design proposes that LIDs should be increasing integers.
This is the simplest ID format that introduces the least visual noise when exposed in, e.g., `EXPLAIN` plans, and there is currently no need for anything more complex.
Since both the `EXPLAIN` output and the introspection tables that will expose operator hydration information are part of Materialize's unstable API, we are free to change the ID format later, should the need arise.

LIDs are assigned during MIR-to-LIR lowering upon creation of each LIR node.
The lowering context keeps track of assigned IDs and ensures that they are unique within a dataflow.

As indicated above, we need to adjust the explain code to show LIDs in the output of `EXPLAIN PHYSICAL PLAN`.
To reduce the noise level, inclusion of LIDs will be controlled by an output modifier, and disabled by default.

### Hydration Status Logging

Timely logging does not directly expose dataflow operator frontiers.
While it does expose per-operator reachability information that can be used to derive frontiers, doing so requires accurate knowledge about the connectivity of the dataflow graph and complicated reasoning logic.
To reduce complexity in the Compute code, we choose not to attempt this approach.

Instead, we propose adding a hydration status logging operator at the end of each group of operators rendered for an LIR node.
These operators will observe frontier progress in their inputs and report hydration events via loggers connected to the `ComputeState`.
There is already precedence for such operators, namely the ones that log import frontiers and arrangement sizes.

### Reporting Introspection Data

Existing replica introspection data is exposed through per-replica logging dataflows that export introspection arrangements.
There are two issues with this approach:

1. In multi-replica clusters, different replicas usually have different data in these arrangements.
   As a result, introspection arrangements can only be queried by special replica-targeted queries.
   To collect introspection data from all replicas, clients have to enumerate the existing replicas and issue a separate query for each, yielding bad UX.
2. When a replica is slow to respond to queries, as if often the case during hydration, it will also be slow to respond to introspection queries.
   This is particularly annoying when one wants to use introspection queries to monitor hydration progress.

To avoid these issues, we propose reporting operator hydration information to the compute controller through `ComputeResponse`s instead.
The compute controller can then write this information into storage-managed collections and expose it to SQL clients that way.
This solves issue (1) as introspection information from all replicas ends up in the same collection and can be retrieved with a single query.
Issue (2) is mostly solved because the time logging events spend on the replica is reduced.
It is still the case that when a replica is completely unresponsive, it likely won't schedule the hydration logging operators, so no new introspection data will make it to the compute controller either.
However, even in these (hopefully rare) cases introspection data previously written can still be queried.

### SQL-level Changes

This design proposes adding two new relations in which operator hydration status information is exposed:

```
mz_internal.mz_compute_operator_hydration_statuses_per_worker {
    object_id: text,
    plan_node_id: uint8,
    replica_id: text,
    worker_id: uint8,
    hydrated: bool,
}

mz_internal.mz_compute_operator_hydration_statuses {
    object_id: text,
    plan_node_id: uint8,
    replica_id: text,
    hydrated: bool,
}
```

... where the former is a storage-managed collection (aka. `BuiltinSource`) while the latter is an aggregating view over the former.

Furthermore, we propose adding a new `EXPLAIN` output modifier, `node_ids`, that toggles rendering of LIDs in physical plan outputs.
For example:

```
materialize=> EXPLAIN PHYSICAL PLAN WITH (node_ids) FOR MATERIALIZED VIEW mv;
                 Physical Plan
------------------------------------------------------------------
 materialize.public.mv:                                          +
   TopK::Basic limit=10 // { node_id: 1 }                        +
     Get::PassArrangements materialize.public.t // { node_id: 0 }+
       raw=true                                                  +

(1 row)
```


## Minimal Viable Prototype

A prototype (though not really "viable" in terms of code quality) was implemented in #24636.

Performance tests using TPC-H suggest that the prototype increases slow-path query processing times by 3.7% on average.
This number should be taken with a grain of salt as the conducted benchmark showed a high variability between different TPC-H queries but also different runs of the same query.
Nevertheless, we can conclude that implementing the feature as designed will have a non-negligible impact on CPU-bound workloads.

There are three sources of computation slowdown we can expect from this design:

* The increased number of operators (LIR regions and logging operators) requires more time spent in dataflow scheduling.
* The increased number of operators and channels produces more Timely logging data, requiring more time spent for logging dataflows and arrangements.
* Some time is spent executing the new hydration logging operators.

## Alternatives

### Progress Tracking Using Fine-grained Timestamps

Another approach to measuring progress through the initial snapshot processing is to make dataflow inputs provide the snapshot data at multiple distinct timestamps.
For example, an input could, rather than inserting 1GB of data at time `T`, insert 100MB each at times `(T, 0)`, `(T, 1)`, ..., `(T, 9)`.
These fine-grained input timestamps would be reflected at the dataflow outputs as well and allow tracking progress at finer granularity than regular time advancements.

We don't choose this approach for two reasons:

* Implementation effort: Changing the main timestamp type used in Compute and accounting for all the new edge cases introduced by this change seems like a lot of effort.
  That said, no prototype implementation was attempted, so we cannot say for sure.
* This approach of reporting progress does not help in finding bottlenecks in dataflow plans.
