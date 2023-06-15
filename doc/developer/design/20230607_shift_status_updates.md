# Shift Storage Status Updates

- [Shift Storage Status Updates](#shift-storage-status-updates)
  - [Associated](#associated)
  - [Context](#context)
  - [Goals](#goals)
  - [Overview](#overview)
  - [Detailed description](#detailed-description)
    - [Rollout strategy](#rollout-strategy)
  - [Alternatives](#alternatives)
  - [Open questions](#open-questions)


## Associated

- [#18795](https://github.com/MaterializeInc/materialize/issues/18795)

<!--
Note: Feel free to add or remove sections as needed. However, most design
docs should at least keep the suggested sections.
-->

## Context

<!--
Bring the reader up to speed enough, such that they can understand the
following goals and descriptions.

An important reason for this is helping future readers understand the
assumptions that went into the design, and in turn the goals and design itself.

Be sure to capture the customer impact/customer problem, which should be the
motivation for the proposed design!
-->

Presently, status updates for source ingestion and sink dataflows are written to the dataflow's designated persist status shard. These writes happen periodically by a single worker per dataflow running in `clusterd`.

Due to the fact that there is currently no way for these dataflows to gracefully shutdown, dropping their backing replicas does not result in status updates indicating that work has been paused.

A concrete example of this can be witnessed by creating in a source in some cluster, dropping the only replica in that cluster, and inspecting it status via `mz_internal.mz_source_statuses`. The current behavior is that the source is incorrectly described as `running` even though it is not possible for ingestion to occur.

## Goals

<!--
Enumerate the concrete goals that are in scope for the project.
-->

- Describe how to shift the writing of status updates for sources/sinks from storage workers to the storage controller
- How to evolve status relations to support storage clusters with multiple replicas

<!-- ## Non-Goals -->

<!--
Enumerate potential goals that are explicitly _out_ of scope for the project
ie. what could we do or what do we want to do in the future - but are not doing
now.
-->

## Overview

<!--
Brief, high-level overview. A few sentences long, at most a couple of smaller
paragraphs.
-->

The primary motivation for enforcing status updates flow through the storage controller is that it allows us to correctly handle instances in which a replica is dropped, in which case any late status updates can simply be ignored and the status of any dataflows managed by the dropped replica can have their status set to `paused`. As mentioned in the [Alternatives](#alternatives) section, relying on graceful shutdown instead would be complex and error prone.

The health operator for ingestion dataflows will continue to exist as a terminal operator in the dataflow graph. However, rather than writing status updates directly to the persist shard corresponding to the collection's status history, the health operator will update a local mapping from `GlobalId` to `HealthStatus` that exists as part of the storage worker's state. Updates of a similar fashion will be done during sink production as well.

Upon some defined frequency, storage workers will emit a new `StorageResponse::StatusUpdates` to the storage controller. When receiving these events, the storage controller will write the status updates to the shard associated with `mz_source_status_history` / `mz_sink_status_history`.

## Detailed description

<!--
Describe the approach in detail. If there is no clear frontrunner, feel free to
list all approaches in alternatives. If applicable, be sure to call out any new
testing/validation that will be required.

For some features it can be helpful to sketch an implementation. If you're
working on things that are crossing team boundaries it will be helpful to spell
out any new interfaces/traits/interactions.

For most new features, you should think about testing, rollout/lifecycle, and
observability. These things can warrant their own sections.
-->

We will introduce a couple new pieces of state to `mz_storage::storage_state::StorageState` in order to keep track of the statuses of sink and source ingestion dataflows:

```rust
pub source_statuses: Rc<RefCell<BTreeMap<GlobalId, Option<HealthStatus>>>>,
pub sink_statuses: Rc<RefCell<BTreeMap<GlobalId, Option<HealthStatus>>>>,
```

For source ingestion dataflows, the health operator will evolve to update the status corresponding to the source's `GlobalId` in `source_statuses`. For sinks, `Healthchecker::update_status` will evolve to do the same for `sink_statuses`.

The storage worker will be updated to periodically send `StorageResponse::StatusUpdates` to the storage controller, which will be defined as:

```rust
pub enum StorageResponse {
    ...,
    StatusUpdates(Vec<ObjectStatusUpdate>),
}

pub enum ObjectStatusUpdate {
    Sink(SinkStatusUpdate),
    Source(SourceStatusUpdate),
}

// Same for `SinkStatusUpdate`
pub struct SourceStatusUpdate {
    id: GlobalId,
    status: String,
    error: String,
    hint: Option<String>,
}
```

To avoid sending this information on every tick of the storage worker's main processing loop, we will only send updates per some defined frequency (e.g. every 5 seconds). This is similar to how statistics reporting is currently implemented. Upon receiving a `StorageResponse::StatusUpdates` message, the storage controller will append the status updates to the appropriate managed collection. Writes to the backing persist shard will take place in a dedicated task, namely the one spawned by `mz_storage_client::controller::collection_mgmt::CollectionManager`.

When dropping a replica, the storage controller will need to retrieve all source/sink dataflows that have been observed to be associated with the given storage instance. While this state is currently maintained in `mz_storage_client::controller::rehydration::RehydrationTask`, we can lift it up into `RehydrationStorageClient` in order for the storage controller to access it without duplicating the data. Upon dropping an instance, the storage controller can be sure that it won't receive any more `StorageResponse`'s from that instance. As a result, late status updates after an instance is dropped are guaranteed not to occur.

Immediately after the coordinator announces initialization is complete, the storage controller will be updated to look for any ingestion/export dataflows that belong to an instance with 0 replicas and set their status to `paused`. This should allow `environmentd` to correctly set the status for sources/sinks in scenarios where it crashes before it was able to write the `paused` status to persist.

In order to support the future in which storage clusters may have multiple replicas, we will need to update the schemas for the `mz_{source|sink}_status_history` relations to include a `replica_id`. This is necessary for the reason that dropping a replica in a storage cluster with multiple replicas shouldn't set the status of a source, for example, to `paused`. Isolating status updates for a source/sink to a specific replica allows any derived relations such as `mz_{source|sink}_statuses` to make determinations based on all the replicas in the cluster.

### Rollout strategy

There is opportunity for the above work to be broken down into a couple of smaller chunks. The PRs involved may look like the following:

1. Shift status writes to the storage controller
2. Update `mz_{source|sink}_status_history` and any derived relations to account for a `replica_id`, which is necessary for multi-replica storage clusters

## Alternatives

<!--
Similar to the Description section. List of alternative approaches considered,
pros/cons or why they were not chosen.
-->

1. Add a graceful shutdown mechanism for storage workers
    - Rejected due to the fact that graceful shutdown is not guaranteed to occur

## Open questions

<!--
Anything currently unanswered that needs specific focus. This section may be
expanded during the doc meeting as other unknowns are pointed out. These
questions may be technical, product, or anything in-between.
-->

- Should we control the rate of `StatusUpdates` from the worker or batch writes at the controller?
