# Unified Compute Introspection

- Associated:
  - https://github.com/MaterializeInc/database-issues/issues/7898
  - https://github.com/MaterializeInc/database-issues/issues/7886
  - https://github.com/MaterializeInc/database-issues/issues/6551

## The Problem

Today compute has an infrastructure of per-replica logging dataflows that collect various kinds of introspection information (frontiers, scheduling durations, arrangement sizes, ...) and expose them in arrangements.
The contents of these arrangements naturally differ between replicas, so querying them requires the use of replica-targeted queries.
This introduces two major pain points:

- Collecting introspection information for all replicas requires first enumerating the live replicas, then sending a replica-targeted query for each of them, then merging the received results.
  In a lot of contexts (e.g. monitoring tools) this workflow is hard or impossible to implement.
- If a replica is unresponsive, targeted queries against it will hang and/or timeout, removing the ability to collect any, even stale, introspection data.

These issues limit the usability of compute introspection for, e.g., use in the console and diagnosis of performance issues.

## Success Criteria

Compute provides a way to expose a subset of replica introspection data that:

- ... can be retrieved through a single query whose definition is independent of the specifics of the exporting replicas.
- ... can be retrieved successfully regardless of the health of any of the exporting replicas.

## Out of Scope

- Providing freshness of unified introspection data.
  If a replica is unresponsive, its retrievable introspection data might be stale.

## Solution Proposal

This design describes an MVP implementation for Unified Compute Introspection that can provide value quickly.

### Overview

- The compute controller defines a set of _introspection subscribes_ that continuously read from introspection indexes, optionally transforming (filter/aggregate/join/...) the read data.
- The catalog defines a set of `BuiltinSource`s and associated `IntrospectionType`s, one for each introspection subscribe.
- Whenever a replica connection is established, the compute controller installs all defined introspection subscribes on the replica, as replica-targeted subscribes.
- Whenever a batch of updates arrives for an introspection subscribe, the compute controller (a) prepends the replica ID to all updates and (b) sends the thus enriched updates to the storage controller, tagged with the corresponding `IntrospectionType`.
- The storage controller's `CollectionManager` appends the introspection updates to the collection associated with the given `IntrospectionType`.
- Whenever a replica disconnects, the compute controller cancels its introspection subscribes and instructs the storage controller to delete all introspection data previously emitted for this replica.
- The storage controller's `CollectionManager` reads back, for each relevant `IntrospectionType`, the current contents of the storage collection, filters out updates belonging to the disconnected replica, and appends retractions for those to the storage collection.

The `BuiltinSource`s fed by `IntrospectionType`s associated with introspection subscribes are unified introspection relations.
They contain introspection updates for all live replicas and can be queried from any cluster.

### Definition of a Unified Introspection Relation

To create a new unified introspection relation, one has to define three parts:

1. In the compute controller, a `DataflowDescription` that defines the introspection subscribe.
2. In the storage controller, an `IntrospectionType` that defines the storage-managed collection into which updates from the introspection subscribe are written.
3. In the catalog, a `BuiltinSource` that makes the storage-managed collection available to SQL queries.

(2) and (3) are required for any `BuiltinSource` defined in the system. Only (1) is specific to this design.

The `DataflowDescription`, since it is defined in the compute controller without access to the catalog or the optimizer, must be written using LIR constructs and can only reference introspection indexes, not builtin sources or views that may be defined in the catalog.
These are limitations that seem reasonable for an MVP.
We can consider removing them in the future, for example by moving the definition and/or management of the introspection subscribe queries into the coordinator.

### Management of Introspection Subscribes

The compute controller contains two methods, `Instance::add_replica` and `Instance::remove_replica`, that are invoked every time a replica connection is established or dropped, respectively.
This applies also to replica rehydration, which consists simply of removing and then re-adding a failed replica.
We propose to extend `add_replica` to install all defined introspection subscribes on the new replica.
`remove_replica` is already set up for canceling introspection subscribes, as it needs to cancel any subscribes targeting the removed replica for correctness reasons.

Installing introspection subscribes requires `add_replica` to be able to generate `GlobalIds` to identify these subscribes.
The compute controller currently does not have this capability.
We propose to give the compute controller access to the coordinator's `transient_id_gen` by converting it from an `u64` to an `Arc<AtomicU64>` and passing a reference to the compute controller during creation.

The compute controller needs to handle responses for introspection subscribes specially:
Rather than propagating them to its clients, it needs to translate them into updates of the associated storage-managed collections:

- `Row` updates (both insertions and retractions) are translated into the schema of the storage-managed collection by prepending the replica ID as the first column.
  The modified `Row`s are then sent to the storage controller for recording under the respective `IntrospectionType`.
- Subscribe errors and cancellations are translated into deletion requests to the storage controller.
  A deletion request targets the storage-managed collection identified by the respective `IntrospectionType` and contains a filter on the first column of each contained `Row` that selects the ID of the replica whose introspection subscribe was interrupted.

### Deletion from Storage-managed Collections

The storage controller delegates the writing of storage-managed collections to a background task called the `CollectionManager`.
The `CollectionManager` accepts `GlobalId`s identifying storage collections and corresponding `(Row, Diff)` updates to be appended to these collections, and appends them at the current system time.
It does not yet expose a mechanism to delete previously written updates from a collection by reading back the collection contents, determining the necessary retractions, and appending them.

To remove introspection data for dropped/disconnected replicas from the unified introspection relations, we require such a deletion mechanism.
Note that the compute controller is not itself able to determine the set of necessary retractions.
While it can be taught to read the contents of storage-managed collections, it doesn't have any way of knowing the time as of which all previously emitted introspection data has been fully written to its target collection.
The compute controller would risk reading back the target collection's contents too soon and retract only part of the introspection data previously emitted for the disconnected replica.

We propose extending the `CollectionManager`'s API to also accept deletion requests.
A deletion request contains a collection ID, as well as a `filter` closure `Box<dyn Fn(&Row) -> bool>`.
The `CollectionManager` handles a deletion request by reading back the target collection at its latest readable time, applying the `filter` closure to each read `Row`, and appending retractions for each `Row` the `filter` returned `true` for.
Append and deletion requests must be applied by the `CollectionManager` in the same order they have been issued by the client, to guarantee that deletions will retract all previously sent appends.

## Minimal Viable Prototype

Prototype implementation: https://github.com/MaterializeInc/materialize/pull/27491

## Alternatives

### Defining Introspection Subscribes in the Coordinator

It is possible to implement the unified introspection mechanism outside of the compute controller.
The compute controller's API should be sufficient for this:
It allows defining replica-targeted subscribes, returns subscribe responses, and interrupts replica-targeted subscribes on replica disconnections.
Thus, an alternative design could be to let the coordinator install the introspection subscribes and act on their responses by writing them to storage collections.

This alternative has the desirable properties that introspection subscribes could be defined in SQL, rather than LIR, and that they could read from arbitrary builtin relations, not just introspection indexes.

The main drawback of this alternative appears to be complexity.
During an (unfinished) attempt to develop a prototype of this approach, the amount of changes and new edge cases required in the coordinator code was significantly higher than with the design proposed above.
The coordinator makes a number of assumptions about subscribe handling (e.g., every active subscribe is associated with a client connection) that don't hold for introspection subscribes and would need to be adjusted.

In the interest of moving fast we opt to not follow this approach for the MVP implementation.
If we find that the limitations of the controller-based approach are too great, we can revisit this decision.

### Controller `CollectionId`s

As an alternative to sharing the `transient_id_gen` between the coordinator and the compute controller, we could change the compute (and perhaps storage) layer to identify collections not by `GlobalId` but by a separate `CollectionId` type:

```rust
enum CollectionId {
    External(GlobalId),
    Internal(u64),
}
```

This would allow the compute controller to mint its own collection identifiers without needing to synchronize with the coordinator.

While this would be a desirable change, also for a possible `ALTER MATERIALIZED VIEW ... SET CLUSTER` feature, the required refactoring work would be significant.
Again in the interest of moving fast, we opt for the simpler approach of sharing the `transient_id_gen` instead.
