# Source Metrics for Users, v2

- Associated Epic: https://github.com/MaterializeInc/materialize/issues/23154


## The Problem

The [existing source statistics](https://materialize.com/docs/sql/system-catalog/mz_internal/#mz_source_statistics_per_worker)
we expose to users for sources answer _some important questions_, like:
- _Generally_, how fast we are reading data from upstream?
- Have we committed the source's snapshot?

but fail to answer **two** critical questions users regularly ask about their sources:

1. How much of its snapshot has this source processed?
2. Is this source keeping up with _upstream_? (This is usually related to: can I resize this source?)


This design document proposes metrics that answer these questions, and goes into detail about their
implementation.

## Success Criteria

This design has succeeded when we are able to present these metrics to users in the console,
and they can use them to answer questions about their source's snapshotting and sizing.

## Out of Scope

- Expose new metrics for sinks.
- Solve the _hydration backpressure problem_ (more on this later)
- Expose detailed, _granular_ metrics that are 100% accurate.

## Solution Proposal

### Background: _Source Frontier Differences_

We define a _source frontier difference_ as the _difference_ between two source frontiers,
as a single, numerical value. The unit of this value depends on the source frontier type.

Each source implementation (critically, Kafka, Postgres, and MySQL) expose different _source timestamp_
types. These types are partially ordered, and have an inconsistent shape. A _source frontier difference_
is a way to capture _how different_ two frontiers are in, in a way that is consistent between source types.

Source frontier differences are used in some form in the metrics proposed in [Snapshot Progress](#snapshot-progress) and
[Steady-state](#steady-state).

#### Kafka frontier differences

For example, consider the following 2 kafka source frontiers:
```
# Partition Id: Offset
f1: {0: 100, 1: 150}
f2: {0: 150, 1: 170}
```

In this case, the source frontier difference is `f2 - f1 = 70 offsets`. Similarly:

```
{0: 100, 1: 100} - {0: 0} = 200 offsets
```

#### Postgres frontier differences

Postgres has two separate definitions:

For the number of rows in each table:
```
{table1: 110 rows, table2: 15 rows} - {table1: 100 rows, table2: 10 rows} = 15 rows
```

And for lsn's during replication:
```
100100 lsn - 100050 lsn = 50 bytes
```

#### MySQL frontier differences

Similar to Postgres, mysql has a rows-per-table difference, as well as a replication difference.

However, during replication, MySQL's source timestamp is partially ordered, instead of just a single
lsn number:

```
# source id: transaction id
f1: {0: 1000th txid, 1: 0th txid}
f2: {0: 1010th txid, 1: 10th txid}
```
In this case, `f2 - f1 = 20 transactions`

#### Comparisons

As you can see, source frontier differences depend on the source, and are primarily useful for _calculating rates_ and when
_comparing values_. For example:

- If a source's frontier has moved from `{0: 10}` to `{0: 70}` in 1 minute, Materialize can be said to have processed 1 offset/second.
- If a source's frontier snapshot frontier is `{table1: 100 rows}` and its current frontier is `{table1: 50}`, Materialize can be said
to have processed about 50% of the snapshot.

Additionally, differences are designed to be _lower-bounded estimates_. For example, `{0: 100} - {0: 0, 1: 50}` is 100, as we
choose the lower bound where we assume no offsets past 50 for partition 1 exist.


## Snapshot Progress

The first set of metrics this design document proposes involve _snapshot progress_, which is a _lower bound_ on the _percentage_ of the source's snapshot Materialize
has _read_. These metrics are designed to answer #1 in [the problem statement](#the-problem).

We will introduce 2 new columns in `mz_source_statistics_per_worker`:

```
| `snapshot_total`     | [`uint8`] | The total number of upstream values that are part of the snapshot. |
| `snapshot_progress`  | [`uint8`] | The number of upstream values Materialize has read so far.         |
```

The unit of _values_ depends on the source type, and will be _rows_ for MySQL and Postgres, and _offsets_ for kafka.

These values can be summed across workers and compared (`snapshot_progress / snapshot_total`) to produce
a _lower-bound_ estimate on the progress we have made reading the source's snapshot.

### Source frontier difference

Note that this percentage is calculated using _source frontier difference_, between the `snapshot_total` and `snapshot_progress`.
For consistency within schema of the `mz_source_statistics_per_worker` table, each worker in the source pipeline is required to
pre-aggregate these values from the possibly complex frontier that represents the total and progress of a snapshot.
This primarily means summing offsets/rows across partitions/tables.

### Source specifics

The `SourceReader` implementation for each source will be required to produce a `snapshot_total`, as well as
a continuously updated `snapshot_progress` frontier on each worker.

#### Kafka

`snapshot_total` can be be trivially exposed by exposing the snapshot frontier already tracked within its source operator,
and summing across partitions. Similarly, `snapshot_progress` can be derived from the operator's output frontier.

#### Postgres and MySQL

`snapshot_total` will need to be calculated, in the unit of rows by performing `SELECT count(*)` on the tables that participate in the snapshot.
Both the Postgres and MySQL implementations will be required to perform this query, per-table, during snapshotting. For RDBMSes, this
should be cheap.

`snapshot_progress` will need to be exposed by the source operators themselves, as the data itself during snapshotting all has
the same frontier. This means the operators will need to track and periodically report a frontier describing the progress they
have made reading the snapshot. In practice, this is the number of rows they have read, per-table.

#### Example Scenarios and Hydration Backpressure

Below is a set of charts for that these new metrics and the derived percentage would look like in 2 difference scenarios.

![snapshot-progress](./static/source_metrics_2/snapshot_progress.png)


These charts make it clear that if we can _read_ from upstream faster than we can process that data, we may present a `snapshot_progress`
that is artificially inflated. This is considered reasonable for this design because:

- We will present the metrics as the _percentage of the snapshot Materialize has read_.
- The console will present the user will the memory usage of the replica doing snapshotting, which makes it clear when this scenario is occurring
(This is why memory is included on the above charts).
- In general, once we have read the entire snapshot from upstream, we process the remaining data quite quickly. This is particularly true for non-UPSERT sources.
- Categorically fixing this means implementing _Hydration Backpressure_, which effectively locks Materialize's _read-rate_ to its _process-rate_.
This is considered [out of scope](#out-of-scope) for this design, but, _critically_: if and when _Hydration backpressure_ is implemented, these proposed
metrics _become even more useful_, as opposed to becoming irrelevant.


## Steady-state

The second set of metrics this design document proposes describe the _rate_ at which upstream and Materialize are processing data.
These metrics are designed to answer #2 in [the problem statement](#the-problem).

```
| `upstream_rate`      | [`uint8`] | The rate at which data is appearing in the upstream service.       |
| `process_rate`       | [`uint8`] | The rate at which Materialize is processing upstream data.         |
```

This rate has units that depend on the source type (replication bytes for Postgres, offsets for Kafka, gtid's for MySQL) and are
designed to be _compared_. The following charts show what these metrics will look like, depending on whether Materialize is keeping up
or falling behind.

![steady-state](./static/source_metrics_2/steady_state.png)


Note that when our _process rate_ is consistently below the `upstream_rate`, the user knows their source isn't keeping up with upstream.

### Implementation

These rates are _source frontier differences_. Each source's `SourceReader` implementation will
be required to expose a continuous stream of _upstream source frontiers_. This means:

- The Kafka source will periodically fetch metadata and expose per-partition offset high-watermarks.
- The MySQL source will periodically expose the result of `SELECT @gtid_executed`, which is the set of gtids representing the latest transactions committed.
- The Postgres source will periodically expose the result of `SELECT pg_current_wal_lsn()`, which is the lsn of the latest transaction.

Additionally, the source pipeline will periodically invert the latest frontier we have committed for the source, from the Materialize timestamp
domain to the source-timestamp domain. For MySQL and Postgres sources, this frontier will the `meet` of the subsource frontiers (as in,
calculate their minimum).

`upstream_rate` will be derived from the two latest _upstream source frontiers_, by calculating _source frontier difference_ between them.
Similarly `process_rate` will be derived from the two latest committed frontiers.


### Existing metrics

Existing metrics such as `messages_received` and `bytes_received` continue to be useful for understanding the general performance of a given source.
If users find some of them confusing or not useful, we can remove them in the future.

## Minimal Viable Prototype

N/A for now. This design document is primarily designed to capture the two core metrics we should add to sources, and how to
implement them in a feasible way. The attached example charts capture the desired output.


## Alternatives

- Expose 100% accurate information about how much of the snapshot we have processed, as opposed to how much we have read.
    - Rejected as it's considered as difficult as implementing Hydration Backpressure.
- Expose progress information in a different format per source type.
    - Rejected as it adds undue complexity in the system catalog, console, and for the user to interpret.
        - In the future, if customers want per-partition information for Kafka sources, we can do so easily using this same general design.
- Consolidate the two metric sets proposed in this design document into one.
    - Rejected due to its complexity.
- Expose more information to users about their sources.
    - This design document is intentionally constrained, to help users answer 2 well-known questions about their sources.
- Replace the 2nd metric set with [Relocking the upstream frontier](https://github.com/MaterializeInc/materialize/issues/23345).
    - Rejected to decouple this design with a reclocking policy decision.
    - Note that the schema and semantics of this design document are forward-compatible with that policy change.


## Open questions

- Are there any edge cases around _source frontier differences_ that can't be reconciled into a relatively meaningful number for the purpose of metrics?
- Are there other scenarios than the ones predicted in the above charts that we aren't capturing?
    - One case might be: people's sources that fall behind upstream just use more memory, while Materialize continues to read as fast as possible.
    The metrics added by this proposal would allow us to easily distinguish between those cases.
- Should `upstream_rate` and `process_rate` be exposed as rates (as described in this design document), or should we instead expose the maximum value, and
leaving calculating the rate to the client?
