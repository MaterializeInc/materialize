# Reclocking implementation

## Summary

This document proposes a design for implementing reclocking and exposing the
reclocked mappings to end users.

## Goals

* Describe the physical representation of the reclocking data.
* Describe the policy for reclocking each type of source.
* Describe how users interact with the reclocking data.

## Non-Goals

* Motivating reclocking. See the product spec in [#11218].
* Describing reclocking formally. See the
  [original reclocking design doc](20210714_reclocking.md).

## Description

### Reclockable source types

We define a **reclockable** source type as one where the upstream system
provides a useful [gauge of progress](20210714_reclocking.md#description).
At the time of writing, all source types are reclockable:

* Kafka
* Kinesis
* PostgreSQL
* PubNub
* S3

### Storage layer implementation

A reclockable source will consist of two definite collections in the storage
layer: the [`remap`](20210714_reclocking.md#remap) collection, which contains
the reclocking metadata, and the reclocked data collection.

Nonreclockable sources will consist of only a single definite collection
containing the source's data. For consistency with reclockable sources, we will
refer to this collection as the "reclocked" collection, even though there is no
upstream clock.

The type of the `remap` collection depends on the source type:

| Source type | `remap` data         | `remap` diff                  |
|-------------|----------------------|-------------------------------|
| Kafka       | `partition: i32`     | `offset_increment: i64`       |
| Kinesis     | `shard: i64`         | `sequence_increment: numeric` |
| PostgreSQL  | `ignored: ()`        | `lsn_increment: i64`          |
| PubNub      | `region: u32`        | `timetoken_ns_increment: i64` |
| S3          | `object_key: text`   | `byte_offset_increment: i64`  |

The `remap` collection exists in the Materialize time domain, where a `u64`
indicates milliseconds since the Unix epoch. In the future, if Materialize
learns to support additional time domains, the `remap` collection will exist in
the "target" time domain, which may be any one of the available time domains.

Let's take a concrete example. Suppose the `remap` collection for a two
partition Kafka source contains the following entries:

```
# (data,  time,           diff)
  (1,     1649686076392,  +42)   # (1)
  (2,     1649686076392,  +40)   # (2)
  (1,     1649686079487,  +3)    # (3)
  (2,     1649686079487,  +4)    # (4)
```

Entry (1) indicates that offsets 0-42 (inclusive) in partition 1 are assigned a
Materialize timestamp of Monday, April 11, 2022 14:07:56.392 PM UTC. Entry (2)
indicates that offsets 0-40 in partition 2 are assigned the same timestamp.
Entry (3) indicates that offsets 43-45 in partition 1 are assigned a Materialize
timestamp of 14:07:59.487 UTC on the same day. Entry (4) indicates that offsets
41-44 in partition 2 are assigned the same timestamp.

Notice how the offsets are described by their *increments*, not their absolute
value. This allows the collection to be consolidated by summing the diffs of
events with the same data.

#### `remap` policy

The remapping must be monotonic. Informally, the mapping must produce an order
of events that does not conflict with order of events in the upstream source.
See [`remap`](20210714_reclocking.md#remap) for a more formal statement of the
property.

We will impose an additional policy constraint on our implementation: if
transactional boundaries are present in the upstream source, events in the same
transaction must be mapped to the same timestamp.

Note that there is explicitly no commitment to the granularity of the mapping.
Any number of upstream events may be mapped to the same Materialize timestamp,
provided the above properties are upheld.

Concretely:

  * The mapping for a Kafka source depends on whether Debezium's transactional
    metadata topic is in use:

      * If Debezium's transactional metadata topic is not in use, the mapping
        must maintain the offset order of the Kafka topic within each partition.
        It does not need to maintain order across partitions.

      * If Debezium's transactional metadata topic is in use, the mapping must
        maintain the order of transactions as determined by the transactional
        metadata topic.

  * The mapping for a Kinesis source must maintain order by sequence number
    within each shard. It does not need to maintain order across shards.

  * The mapping for a PostgreSQL source must maintain order of the commit LSN
    for each transaction. It must present all changes in a single transaction at
    the same timestamp. TODO: what is the difference between "end LSN" and
    "commit LSN" in the PostgreSQL logical replication protocol?

  * The mapping for an S3 source must maintain the byte order within each
    object. It does not need to maintain order across objects.

#### Mechanics

The existing [`create_source`](https://github.com/MaterializeInc/materialize/blob/9af714e667a04677a14a038ba80c5614c9beb70b/src/storage/src/source/mod.rs#L869)
function will be modified to write bindings directly to a single persist `Shard`,
rather than sending to the bindings to the storage controller and waiting to
hear that they have been durably persisted.

The details are murky. There is a draft PR ([#11883](https://github.com/MaterializeInc/materialize/pull/11883))
with more specifics.

TODO: describe how to handle the Debezium transaction metadata topic.

### Storage API changes

Each source in the system is identified by a [`GlobalId`].  The global ID will
refer to the reclocked collection in the following commands:

  * [`StorageCommand::CreateSources`](https://dev.materialize.com/api/rust/mz_dataflow_types/client/enum.StorageCommand.html#variant.CreateSources)
  * [`StorageCommand::AllowCompaction`](https://dev.materialize.com/api/rust/mz_dataflow_types/client/enum.StorageCommand.html#variant.AllowCompaction)
  * [`StorageCommand::Insert`](https://dev.materialize.com/api/rust/mz_dataflow_types/client/enum.StorageCommand.html#variant.Insert)
  * [`StorageComand::DurabilityFrontierUpdates`](https://dev.materialize.com/api/rust/mz_dataflow_types/client/enum.StorageCommand.html#variant.DurabilityFrontierUpdates)

[`StorageCommand::RenderSources`](https://dev.materialize.com/api/rust/mz_dataflow_types/client/enum.StorageCommand.html#variant.RenderSources) will evolve to allow clients to specify *which* of the collections associated with the source to render:

```rust
/// Identifies a collection in the storage layer.
struct StorageCollectionId {
    /// The ID of the source with which the collection is associated.
    source_id: GlobalId
    /// Which collection associated with the source to choose.
    collection_id: CollectionType,
}

/// Specifies the type of a collection associated with a source.
enum CollectionType {
    /// The collection containing the reclocked upstream data.
    Reclocked,
    /// The collection containing the reclocking mappings.
    Remapping,
}
```

### SQL changes

The `CREATE SOURCE` statement will be extended with an `EXPOSE PROGRESS
AS <name>` clause. For example:

```sql
CREATE SOURCE foo
FROM KAFKA BROKER '...'
EXPOSE PROGRESS AS my_foo_progress
```

Materialize will make the contents of the `remap` collection for the source
visible under the specified name via a relation whose structure depends on the
type of the source:

* Kafka sources

  | mz_timestamp | partition | offset |
  |--------------|-----------|--------|
  | ...          | ...       | ...    |

* Kinesis sources

  | mz_timestamp | shard_id | sequence_number |
  |--------------|----------|-----------------|
  | ...          | ...      | ...             |

* PostgreSQL sources

  | mz_timestamp | lsn |
  |--------------|-----|
  | ...          | ... |

* PubNub sources

  | mz_timestamp | region | timetoken |
  |--------------|--------|-----------|
  | ...          | ...    | ...       |

* S3 sources

  | mz_timestamp | key | byte_offset |
  |--------------|-----|-------------|
  | ...          | ... | ...         |


If `EXPOSE PROGRESS` is not specified, and the source type is reclockable,
Materialize will generate a name of the form `<source>_progress`. If that name
already exists, Materialize will try names of the form `<source>_progress1`,
`<source>_progress2`, ... in sequence until it finds an available name. This
matches the logic used for generatic the name of a primary index for a table.

The reclocked relation will be a new catalog item of type `SourceProgress`:

```rust
// catalog.rs
/// An item representing a source progress relation.
struct SourceProgress {
    /// The ID of the source to which this progress applies.
    source_id: GlobalId,
    /// The type of the progress relation.
    ///
    /// This must be derived the type of the `source_id` source. It is
    /// duplicated here, rather than being derived from the source on the fly,
    /// due to limitations in the design of the catalog API. (The type of the
    /// item must be derivable from the item itself, i.e., without access to the
    /// rest of catalog).
    desc: RelationDesc,
}
```

`SourceProgress` items will necessarily have a `GlobalId` in order to be
included in the catalog; however this `GlobalId` will not be meaningful to the
storage layer.

Each reclocking relation will consist of one row per partition per time, where
"partition" is used to mean whatever the source type uses as its partition key.
End users can query historical timestamp bindings via `SELECT ... AS OF`, and
view the complete set of historical timestamp bindings via `TAIL ... AS OF`.
We may wish to introduce additional verbs for `TAIL`, to support e.g.

```sql
TAIL progress AS OF EARLIEST UNTIL LATEST
```

but formally designing those features is out of scope for this document.

### System catalog changes

A new system table `mz_source_progresses` will be added with the following schema:

| id | name | source_id |
|----|------|-----------|
| .. | ...  | ...       |

This table relates the name of a reclocking collection to a source. There is no
need to describe the database or schema of the reclocking collection; like
indexes, they always belong to the same schema as the relation with which they
are associated.

The `mz_objects` view will include items from `mz_source_progresses` with a type
of `source_progress`.

## Unresolved questions

* Is our `numeric` type large enough to hold a Kinesis sequence number?

[`CreateSourceCommand`]: https://dev.materialize.com/api/rust/mz_dataflow_types/client/struct.CreateSourceCommand.html
[`GlobalId`]: https://dev.materialize.com/api/rust/mz_expr/enum.GlobalId.html
[#11218]: https://github.com/MaterializeInc/materialize/issues/11218#issuecomment-1087849617
