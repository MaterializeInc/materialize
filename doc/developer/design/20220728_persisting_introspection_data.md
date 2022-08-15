# Persisting introspection data

This document discusses design choices and an implementation overview regarding persisting introspection information.

## Introspection and what it does

The introspection mechanism surfaces the timely logging dataflows to users. For example the [mz_catalog.mz_scheduling_histogram](https://materialize.com/docs/sql/system-catalog/#mz_scheduling_histogram) allows user to inspect how long certain timely operators were running. This can be useful for understanding and debugging Materialize’s performance.

The contents of introspection tables are inherently different per timely instance and thus per replica, unlike with other dataflows, it is not expected that different replicas produce the same data.

The usual logic of `ActiveReplication` that forwards Peek’s to all replicas and uses the first one does not make sense, as there is no way for the user to tell from which replica the obtained numbers are.

Thus we have to offer some other mechanism.

## Scenarios to consider

Debugging the performance of materialize is especially important if a replica does not behave as expected. It is important, that users can query and interpret the introspection data of replicas in the following scenarios:

- Replica that has been removed

In this case the user actively deleted the replica (for example via `DROP CLUSTER REPLICA`). The user might still be interested in the introspection data, for example as reference in a comparison to another (faster/slower) replica. On the other hand, since the user actively requested the replica to be dropped, it would have also been possible for the user to save relevant data before dropping the replica. (See the design question “Behavior after replica drop“.)

- Slow replica

If a replica gets saturated and does not perform timely updates anymore, the user is probably interested in which dataflows cause a lot of computation. We should ensure that 1) the logging works reliably even in the case of an otherwise saturated replica and 2) it does not influence the logging of other replicas.

- Crashing replica

If a replica crashes (for example due to being out-of-memory), the introspection data can’t reliably be saved by the user before the crash happens, as it happened unexpectedly. It is important for the user to see the introspection data for a postmortem analysis.

- Restarting replica

Related to the crashed replicas, we have to consider what happens when a replica restarts, since Kubernetes (potentially with some back-off) does this automatically. If we want to analyze the fault scenario it is that the user can distinguish from which lifetime the data stems. On the other hand, keeping around a lot of data from a long defunct replica might distract the user.

- In future versions: Using introspection for autoscaling

At some point in the distant future, we might use this data to automatically detect an overloaded replica and migrate the instance to a bigger instance automatically.

## Problems
### Difficulties in offering an aggregated source

From a pure SQL perspective, the obvious schema to present the cross-replica introspection information is to extend the introspection tables with a replica id column.

For example `mz_scheduling_histograms` would get an additional column `replica_id`.

```rust
worker    slept_for    requested    count
-----------------------------------------
...
```

Would become

```rust
replica_id     worker    slept_for    requested    count
---------------------------------------------------------
...
```

However, if done naively the way queries work in materialize requires a fixed timestamp at which time the results of the queries are reported. But if one replica is lagging or otherwise unable to provide logging data, the query would either report out of date data (when settling for the least timestamp where all replicas have provided data) or block for a long time (when choosing a timestamp closer to the actual time). See the design question “Expose an aggregate sources or source per replica” as well as the [discussion in the Github](https://github.com/MaterializeInc/materialize/pull/13340#issuecomment-1180628374) PR for more technical details.

### UX gap with per replica sources

To replace the arranged introspection with the persisted sources, we should consider the following scenario: A user has two replicas. First the user tries to debug a performance issue by writing a complex SQL statement targeting the replica (using `_1` as table postfix). Now the user wants to send the same query to another replica. The user has to rewrite the whole query to include the new postfix (`_2`).

We could offer an aggregate *view* over all replicas, but this view has to be changed in the catalog on each replica create or drop, thus any view defined upon this needs to be removed, defeating the purpose of having the view in the first place. Also, a `TAIL` running against such an aggregated source would not contain newly appearing replicas.

In addition, this aggregated view would also suffer from the timestamp selection problem described in "Difficulties in offering an aggregated source".

Alternatively, to stay backwards compatible with the replica targeted queries, we could offer a view that refers to a postfixed variant, and recreate this view with a `SET CLUSTER_REPLICA` command. (I.e. the view would be defined as  `CREATE VIEW mz_arrangement_sharing AS SELECT * FROM mz_arrangement_sharing_1` and the postfix changes with each `set cluster_replica`). This approach suffers from the same limitations as above, because the view has to be recreated. It is not possible for the user to define views upon that view and then change the `cluster_replica` variable.

### The zombie replica problem

We have to consider the possibility of a half alive computed that has lost all connections to other workers and environmentd, but still continues writing its introspection sources. Because it has lost all connections, it is indistinguishable from dead to the rest of the system. At some point, Kubernetes will restart that instance, but this might not happen immediately and we should consider the option of having two computeds - with the same replica id - running at the same time.

If it goes wrong, the user could see duplicate data in the introspection sources.

Depending on which design settle, this problem has different solutions. It boils down to either delegating this to the storage layer (using renditions or similar) or actively checking for this situation (for example by reading back the introspection and checking no one else wrote with that id).

## Current and new proposed solution

### M1: Replica directed queries

In M1, we implemented [replica targeted](https://github.com/MaterializeInc/materialize/pull/12568) queries. They use the special variable `cluster` and `cluster_replica` to direct a future `SELECT` to a specific replica. Using this technique, it is possible to query the introspection information of a given replica.

```rust
CREATE CLUSTER test
  REPLICA replica_a (SIZE '1'),
  REPLICA replica_b (SIZE '2');
SET cluster = test;
SET cluster_replica = replica_a;
SELECT * FROM mz_materializations;
```

Setting this variable will have an effect on all queries, not only those that target introspection sources.

### M2: Using persist for introspection

A better alternative is having the replicas write their logging information into persist and making these persist shards available in the catalog. This way it is possible to query and analyze introspection data from another (non-production) cluster, even in the case of a replica that died or became otherwise unresponsive

# Design decisions

Because of the problems outlined before, we offer per-replica introspection sources and views. But since these are not a complete replacement (see the UX gap above) we keep the arranged logs and the replica directed queries around.

### Replica crash/restart

On replica restart, we reuse the persist shard. When a replica starts, it obtains a snapshot from persist and removes the entries. This works fine if the previous replica has terminated completely, but does not solve the zombie replica problem. However, the precise mechanism on how we write a timely dataflow to persist is currently under revision.

### Views defined on introspection sources

We do have some views that are defined on top of introspection sources to present the data in a more user friendly way. These views are duplicated for the persisted introspection sources, also with a postfix.

### Namespace

For now we keep the persisted introspection data as well as the introspection views in `mz_catalog` . Also for now we keep the catalog item type as “source”, thus they show up
for example with `SELECT * FROM mz_catalog.mz_sources`

### Expose an aggregate sources or per-replica source

For the reasons described above, it is not trivial to realize a unionized view over all replicas. In the future, source reclocking might be a mechanism that is applicable to this problem, but it is fairly low priority. There are a couple of ideas how to get the benefits with mechanisms available now:

- Construct one source per replica

The idea is to expose to the user one source per replica-introspection type pair. For example after `CREATE CLUSTER c1 REPLICAS (r (SIZE '1'));` the following tables can be found in `mz_catalog`:

`mz_catalog.dataflow_operators_1` (for the default cluster)

`mz_catalog.dataflow_operators_2` (for the cluster c1)

Each table name has a cluster id suffix, users can determine the cluster id using the table `mz_catalog.mz_cluster_replicas_base`.

**Benefits**: It allows the user to query up-to-date data from a single replica. There is nothing that blocks a computed of writing into the logging sources as fast as persist allows, thus the queried data should be fresh. This approach synergizes well with storage renditions, if a computed restarts, it could start a new rendition, elegantly solving the problem that a “zombie” computed writes to the same shard.

**Drawbacks**: It is not idiomatic SQL, which makes it hard to construct aggregates over the replicas (”what is the average response time over all replicas?” type of queries). Also, the user can still create an aggregate view over these sources (as shown below), which then would expose the same lag problem as if we provide a naively unionized view. The user has to understand the implications of doing this somehow.

```sql
CREATE VIEW mz_replica_cpu_usage AS
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_1
  UNION ALL
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_2
  UNION ALL
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_3
```

- Let all computeds write into one aggregated shard

Using an atomic operation on the persist shard, multiple writers can write into one persist shard, which could then directly contain the data in the format with the replica column added. ([see Frank’s comment](https://github.com/MaterializeInc/materialize/pull/13340#issuecomment-1181844708) ).

**Benefits**: It exposes a single source per introspection type, thus we can match the ideal schema presented above. Updates from fast replica appear together with old updates from lagging replicas.

**Drawbacks**: Multiple reader have to contend for the persist shard, thus limiting the update frequency of the data which requires some compromise in data freshness.
There is another reason for limiting the update frequency: We have to determine an artificial timestamp (because we can only append data with an increasing timestamp) and have to ensure that we are not running arbitrarily far into the future.

To determine an update frequency, we need to know: How many replicas we expect to exist at one time and how fast persist’s  `compare_and_append` is in a scenario with contention. As an educated guess: For example assuming a maximum of 100 replicas and an update latency of 10ms I’d suggest a update frequency of 10s or more (accounting for potential retries).

- Pipe updates through environmentd

**Benefits**: Using environmentd as serialization point can potentially lead to better freshness than if every writer contends on the persist shard itself.

**Drawbacks**: High load on environmentd. A pretty big break with our dataplane separation in platform.

**Decision:** We implement the per replica source for now.

### Behavior after replica drop

Should the persisted introspection data remain after a drop cluster? The user actively requested the `DROP CLUSTER REPLICA` so the user could always save the data before dropping.

- If the source remains, what should be the content of these sources?
If we do one source per replica, we also have to consider what are the contents? We can for example have the source stay around but provide empty data.
- If no, do we need cascade (as in `DROP CLUSTER ... CASCADE`) to clean up the introspection shards? Or is introspection data “volatile” that should be dropped even without cascade?
- Even if we do remove the entries without cascade, we need to handle the case when there are dependencies on those entries, such as a view created on top of the introspection sources.

Right now, `DROP CLUSTER REPLICA` does not have a cascade option (because there was nothing to cascade into).

**Decision**: We drop the introspection sources and views on replica drop. We add a `CASCADE` option, that will also drop any dependents on the views. The same logic

The same logic propagates to `DROP CLUSTER` if a cluster is dropped, and there is a (transitive) dependency on one of the cluster’s replica introspection sources, the command will fail. Unless `CASCADE` is specified, in which case, the cluster, the replica, the introspection sources and any views using these introspection sources is used.

# Implementation details

This section describes some of the implementation choices of introspection sources.

Right now we have two different introspection locations *arranged* and *persisted*. Both contain the same data, but are either stored in a in-memory arrangement or in a persist shard.

The computeds write compact introspection data. To present this data in a more user-friendly way, we define views on top of the introspection sources.

Note that each replica (even within the same cluster) generates *different* introspection data. This is unlike production data, where we expect each replica to produce exactly the same data (and thus it does not matter which replica writes first to persists or replies to a `Peek` ).

|  | Arranged | Persisted |
| --- | --- | --- |
| Source (compact representation) | Often postfixed with internal (For example: `mz_dataflow_operator_reachability_internal` ) | `mz_dataflow_operator_reachability_internal_1` |
| Views (human readable representation) | Normal, builtin view (For example: `mz_dataflow_operator_reachability` ) | `mz_dataflow_operator_reachability_1` |

The user can request on a per cluster basis whether to enable introspection and with which frequency the introspection data is updated. This option applies to both, arranged and persisted introspection.

The types `LogVariant` and `LogView` enumerate the persisted introspection sources and views.

### Catalog perspective

Arranged *views* are global, independent of the number of clusters or replicas. Arranged sources get a different `GlobalId` per cluster. Persisted sources and views on the other hand are per-replica. Thus environmentd creates or removes catalog entries related to introspection sources in roughly the following cases:

- The system is started/bootstrapped
    - This is when the arranged ID’s are allocated and views are built (since the views are static, using the BUILTINS mechanism).
    - For persisted introspection sources, nothing happens apart from a potential catalog migration (See Catalog Migration).
- A cluster is created
    - `GlobalId` for the *arranged sources* are allocated. They are not visible in the introspection tables (i.e. `select * from mz_catalog.mz_sources;` will always display the `GlobalId` of the default cluster). But queries are rewritten correctly: If the non-default cluster is selected, selecting from a *arranged source* will resolve the name to the per cluster id.
- A replica is created
    - Nothing happens for arranged introspection
    - A `GlobalId` and an introspection shard are allocated for each introspection source, after inserting the corresponding catalog entries (e.g. `mz_catalog.mz_dataflow_operator_reachability_internal_1` ), the introspection
    views that use these postfixed sources are parsed from a SQL string and inserted into the catalog. The `LogView` enum can produce a SQL `CREATE VIEW ...` string that refers to the postfixed sources. Thus it’s important that the sources are inserted in the catalog before parsing and inserting the views in the catalog too.
- A replica is dropped
    - Nothing happens for arranged introspection....
    - Arranged introspection views and sources are removed. `CASCADE` is necessary if any other catalog item depends upon the sources or views. However at runtime, views are a normal catalog item that has a dependency on the source. Whenever we determine if there are dependents, we have to ensure we don’t accidentally include the *persisted views*.

Note that the replica created case can be triggered with a `CREATE CLUSTER REPLICA` as well as an inline replica specification with `CREATE CLUSTER` . Analogous, drop replica can happen with `DROP CLUSTER REPLICA` as well as `DROP CLUSTER ... CASCADE`

All of these mechanisms become a no-op if a cluster is created with introspection turned off.

The `GlobalId` to CollectionMetadata mapping is stored in the `METADATA_COLLECTION` stash, as any other id backed by a storage collection.

### Catalog Storage

At runtime, the catalog is populated with the normal entries for sources and views. However, when the catalog is stored all the persisted sources and views are stored alongside the replica object. This ensures we can retrieve the connection between a replica and the introspection ids.

### Catalog Migration

The serialized `SerializedComputeInstanceReplicaLogging` has three variants:

- Default: No introspection sources created, but create the default ones when possible.
- Concrete: Introspection sources are allocated, but not views. Create default views when possible.
- ConcreteViews: Introspection sources and views have been allocated.

In `catalog::open` the cases Default and Concrete are replaced with `ConcreteViews` . If the cluster has logging enabled, it will become ConcreteViews with non empty vectors. If the cluster has logging disabled, it will be replaced with `ConcreteViews([],[])`.

### Compute controller

The compute controller that manages the replica has to ensure that the `CreateInstance` command is specialized for each replica. It does so using the data stored in the replica struct.

### Computed perspective

The computeds do not know about the views at all, they know and worry only about the compact representation. When a user queries an introspection view, it is handled like a normal view.

The `CreateInstance`  command is the first command a computed receives and contains a `LoggingConfig` . This struct describes which introspection sources to maintain in memory, as arrangement and which to write to persist.

If there are multiple replicas, the arranged logs use the same `GlobalId` for the same introspection source across replicas. This means that queries that use arranged introspection sources need to be directed to a replica, otherwise the user can’t tell which replica replied.
