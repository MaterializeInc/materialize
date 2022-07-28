# Persisting Introspection Data

## Summary

The introspection mechanism surfaces the timely logging dataflows to users. For example the
[mz_catalog.mz_scheduling_histogram](https://materialize.com/docs/sql/system-catalog/#mz_scheduling_histogram)
allows user to inspect how long certain timely operators were running. This can be useful for understanding and debugging Materialize’s performance.

The contents of introspection tables are inherently different per timely instance and thus per replica. Unlike
with other dataflows, it is not expected that different replicas produce the same data. The usual logic of `ActiveReplication`
that forwards Peek’s to all replicas and uses the first one does
not make sense, as there is no way for the user to tell from which replica the obtained numbers are.




## Goals

On a high level, we want to provide users with a convenient way to query each replicas introspection sources. These
data sources are of particular interest in the scenarios where the performance does not match expectations.

### Scenarios to consider

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

## Current state

### M1: Replica directed queries

In M1, we implemented [replica targeted queries](https://github.com/MaterializeInc/materialize/pull/12568) queries. They use the special variable `cluster` and `cluster_replica` to direct a future `SELECT` to a specific replica. Using this technique, it is possible to query the introspection information of a given replica.

```rust
CREATE CLUSTER test
  REPLICA replica_a (SIZE '1'),
  REPLICA replica_b (SIZE '2');
SET cluster = test;
SET cluster_replica = replica_a;
SELECT * from mz_materializations;rmissions that must be reviewed by an App Manager before you can inst
```

Setting this variable will have an effect on all queries, not only those that target introspection sources.


## Difficulties in offering an aggregated source

From a pure SQL perspective, the obvious schema to present the cross-replica introspection information is to extend the introspection tables with a replica id column.

For example `mz_scheduling_histograms` would get an additional column `replica_id`.

```
worker    slept_for    requested    count
-----------------------------------------
...
```

Would become

```
replica_id     worker    slept_for    requested    count
---------------------------------------------------------
...
```

However, if done naively the way queries work in materialize requires a fixed timestamp at which time the results of the queries are reported. But if one replica is lagging or otherwise unable to provide logging data, the query would either report out of date data (when settling for the least timestamp where all replicas have provided data) or block for a long time (when choosing a timestamp closer to the actual time). See the design question “Expose an aggregate sources or source per replica” as well as the [discussion in the Github](https://github.com/MaterializeInc/materialize/pull/13340#issuecomment-1180628374) PR for more technical details.

## The zombie replica problem

We have to consider the possibility of a half alive computed that has lost all connections to other workers and environmentd, but still continues writing its introspection sources. Because it has lost all connections, it is indistinguishable from dead to the rest of the system. At some point, Kubernetes will restart that instance, but this might not happen immediately and we should consider the option of having two computeds - with the same replica id - running at the same time.

If it goes wrong, the user could see duplicate data in the sources.

Depending on which design settle, this problem has different solutions. It boils down to either delegating this to the storage layer (using renditions or similar) or actively checking for this situation (for example by reading back the introspection and checking no one else wrote with that id).



## Non-Goals

- Offer a unified view over the introspection sources. The clean way of doing this is by solving the the re-clocking problem (#12867).
- Implement a post-mortem analysis option for crashed replicas. The solution should enable this in future, but we do not plan to implement it now.


## Description

Given the difficulty in offering a compelling aggregate source, we opt to create a different source for each replica's introspection shards.
These persist shards will appear in the catalog. This way it is possible to query and analyze introspection data from another (non-production)
cluster, even in the case of a replica that died or became otherwise unresponsive.

he idea is to expose to the user one source per replica-introspection type pair. For example after `CREATE CLUSTER c1 REPLICAS (r (SIZE '1'));` the following tables can be found in `mz_catalog`:

`mz_catalog.dataflow_operators_1` (for the default cluster)

`mz_catalog.dataflow_operators_2` (for the cluster c1)

Each table name has a cluster id suffix, users can determine the cluster id using the table `mz_catalog.mz_cluster_replicas_base`.
This allows the user to conveniently query up-to-date data from a single replica. There is nothing that blocks a computed
of writing into the logging sources as fast as persist allows, thus the queried data should be fresh. This approach synergizes well with storage renditions, if a computed restarts, it could start a new rendition, elegantly solving the problem that a “zombie” computed writes to the same shard.

The big drawback of this approach is that it is not idiomatic SQL, which makes it hard to construct aggregates over the replicas (”what is the average response time over all replicas?” type of queries). Also, the user can still create an aggregate view over these sources (as shown below), which then would expose the same lag problem as if we provide a naively unionized view. The user has to understand the implications of doing this somehow.

```sql
CREATE VIEW mz_replica_cpu_usage AS
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_cluster_1_replica_1
  UNION ALL
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_cluster_1_replica_2
  UNION ALL
  SELECT * FROM mz_catalog.mz_replica_cpu_usage_cluster_2_replica_1
```

In general, we drop these sources with the replica. The behavior is that the sources
of a replica are dropped without a `CASCADE`, but if there are dependencies on these sources,
`CASCADE` is necessary.

`DROP CLUSTER` already has a `CASCADE` option, which will drop the replicas. This will be extended
to cascade into the introspection sources.

To be consistent we add a `CASCADE` option to `DROP CLUSTER REPLICA`, which deletes any items
are depended upon the introspection sources.

## Alternatives

The alternatives considered would offer the aggregate source to users, while not being reliant on the reclocking being fully implemented.
Comments to these approaches can be found in the [PR #13340](https://github.com/MaterializeInc/materialize/pull/13340):

- Let all computeds write into one aggregated shard

Using an atomic operation on the persist shard, multiple writers can write into one persist shard, which could then directly contain the data in the format with the replica column added. ([see Frank’s comment](https://github.com/MaterializeInc/materialize/pull/13340#issuecomment-1181844708) ).

**Benefits**: It exposes a single source per introspection type, thus we can match the ideal schema presented above. Updates from fast replica appear together with old updates from lagging replicas.

**Drawbacks**: Multiple reader have to contend for the persist shard, thus limiting the update frequency of the data which requires some compromise in data freshness.
There is another reason for limiting the update frequency: We have to determine an artificial timestamp (because we can only append data with an increasing timestamp) and have to ensure that we are not running arbitrarily far into the future.

To determine an update frequency, we need to know: How many replicas we expect to exist at one time and how fast persist’s  `compare_and_append` is in a scenario with contention. As an educated guess: For example assuming a maximum of 100 replicas and an update latency of 10ms I’d suggest a update frequency of 10s or more (accounting for potential retries).

**My take**: I think this is a great option! From my perspective the reduce update frequency is acceptable. Implementation wise, it does not go well together with persist renditions, thus some extra care has to be taken for implementing clean up and handling the zombie problem.

- Pipe updates through environmentd

**Benefits**: Using environmentd as serialization point can potentially lead to better freshness than if every writer contends on the persist shard itself.

**Drawbacks**: High load on environmentd. A pretty big break with our dataplane separation in platform.

**My take**: I don’t like piping data the user has not queried through environmentd.


## Open questions

### Do we need a dedicated compaction window for introspection?

Introspection data is quite different than user data, does it make sense to provide a dedicated compaction window for introspection sources? This would allow users to go further back in the introspection than the data.

Assume for example a customer with a lot of data, and thus has to chose a small compaction window (say minutes). Over the course of a day, the frequency of new data arrivals varies, for example during commute time a ride tracking application would process much more data than at night. Storing the introspection data for a day would allow the user to read back in time to compare commute times with night times.

If we decide to do this, we have to figure out how it can be configured. In case we go for one-source-per replica, it could be a SQL parameter when we create the replica. If we go for a one-source for all approach, it could be a global variable that can be set.

**My take**: I think the use case is legit and we should do it. Given we can find a user friendly way to configure it.

### What to do with introspection views defined on intro sources?

We do have some (TODO: which ones?) views that are defined on top of introspection sources. As of now, they still use the replica directed introspection sources. To use only the persisted sources, we have to change those views:

- Keep the replica targeted options around
- For each replica, create a postfixed version of the view too
- Disable the views until we have figured how to offer an aggregated source

### How to best present the introspection “sources” to the user?

Right now, they show up with “show sources”, which is possibly a bit surprising to the user (which would only expect

Options are:

- Keep it as it is: Introspection sources show up with `show sources`
- Create a new category type log, the user has to do a `show log` . The output could include log specific information, like which replica corresponds to the source.

### Behavior after replica crash / restart

What happens to the introspection data after a replica has crashed? The data can be valuable for debugging, so it should not disappear. On the other hand, the coordinator/kubernetes will probably restart the compute instance immediately, which could lead the newly started computed to wipe out any previous information.

Options are:

- Let the restarted computed wipe the old data

Since kubernetes is backing-off the restart, there is still a small window where the deceased replica data can be seen. Also, the query can use an explicit `AS OF` to get the data of that moment.

**My take**: It’s probably good enough.

- On restart generate a new replica id, to distinguish the old from the new instance

And allow the old data to stay around.

Assigning a new replica id seems to be the most comfortable for that specific use case. However, an ever changing replica id might also be confusing for users (and cause all kinds of implementation problems).

An additional mechanism is needed for purging the data of the deceased replica.

**My take**: It would give the user a bit of more slack when viewing the introspection data, but creates more complexity for the users (now having to deal with changing ids + needs to remember to clean up stale introspection data). I don’t think the additional complexity justifies this. On the plus side, this it would solve the zombie replica problem directly (because there is no confusion between “runs” anymore).

- Delay replica restart

The replica is not restarted automatically, but requires user invention. This allows the user to inspect the data before the replica comes up again.

Does kubernetes actually stop trying to re-spin crashed nodes?

**My take**: I like the automatic restart, as it can keep a production workload running for smaller hiccups so we should not change that as default. But maybe it makes sense to offer a debugging option that changes this behavior.

### Namespace

This is especially relevant if we go for the one source per replica-introspection source pair: Having many cluster/replicas can clog the `mz_catalog` namespace. Is it better to use a new namespace (for example `mz_introspection` ) ?

**My take**: If we do one source per replica, we should do an extra namespace, otherwise it can stay in `mz_catalog`.

### Do we keep replica directed queries?

The primary use cases for replica directed queries are the introspection sources, but it can be also a useful tool for debugging. For example a user can compare the latency of two replicas of different size by sending a `SELECT` to a specific replica.

**My take**: No strong opinions here, I don’t think keeping it has any drawbacks.
