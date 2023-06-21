# Shift Storage Status Updates

- [Shift Storage Status Updates](#shift-storage-status-updates)
  - [Associated](#associated)
  - [Context](#context)
  - [Goals](#goals)
  - [Overview](#overview)
  - [Detailed description](#detailed-description)
    - [Testing](#testing)
    - [Rollout strategy](#rollout-strategy)
  - [Alternatives](#alternatives)
  - [Open questions](#open-questions)


## Associated

- [#20036](https://github.com/MaterializeInc/materialize/issues/20036)

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

The health operator for ingestion dataflows will continue to exist as a terminal operator in the dataflow graph. However, rather than writing status updates directly to the persist shard corresponding to the collection's status history, the health operator will send status updates across a channel maintained by the storage worker. Updates will be sent in a similar fashion during sink production as well.

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
pub struct SourceStatusUpdate<T = mz_repr::Timestamp> {
    id: GlobalId,
    status: String,
    error: String,
    hint: Option<String>,
    timestamp: T,
}
```

We will introduce a new channel in which operators can communicate up status updates. This channel should be unbounded so that the timely workers never block and can always make progress. Operators can use the new public piece of state that will be added to `mz_storage::storage_state::StorageState`:

```rust
/// Send handle for source/sink statuses. Updates can be sent from dataflow operators
/// and they will be flushed to the storage controller during the worker's
/// main processing loop. Senders should take care to only emit updates if the status of
/// the ingestion/export in question has changed.
pub object_status_tx: crossbeam_channel::Sender<ObjectStatusUpdate>,
```

Ingestion/export dataflows will evolve to send status updates across this channel, rather than directly writing statuses to persist. The current behavior of only emitting _new_ status updates will be preserved in order to avoid unnecessary load. Every so often, any pending status updates will be flushed to the controller where writes to the collection's associated persist shard will take place. This frequency may be configurable and is primarily meant to be a safeguard for storage dataflows that experience frequent status updates (e.g. a Kafka source in a crash loop).

In the storage controller, writes to persist will take place upon receival of a `StorageResponse::StatusUpdates`. Given that timestamps are captured in the storage worker and packaged in status updates, we can simply utilize the current machinery in `mz_storage_client::controller::collection_mgmt` to perform monotonic appends. That is, the timestamps of individual status updates are not necessarily correlated to the time at which they are written to persist.

Additionally, there will be a couple of new statuses introduced, namely `paused` and `unknown`. A status of `paused` indicates that a source/sink cannot perform any computation because it has no available resources. A status of `unknown` indicates the channel in which responses are received from the storage replica is broken, therefore the controller has no way of receiving status updates.

In order to set a status of `paused` for a source/sink, the storage controller will need to know the dataflow's backing replica at the time the replica is dropped. The mapping between observed sources/sinks currently exists in `mz_storage_client::controller::rehydration::RehydrationTask`, however we can lift it up into `RehydrationStorageClient` in order for the storage controller to access it without duplicating the data. Furthermore, to combat cases in which `environmentd` crashes before it can successfully write a `paused` status, the storage controller should write a status of `paused` for any dataflows that belong to an instance with 0 replicas immediately following coordinator initialization.

Statuses of `unknown` can be written upon the response channel from the storage worker -> controller breaking. We can avoid continuously writing updates in scenarios where the worker is unhealthy for an extended period of time by remembering the last status for each source/sink in the storage controller and only writing to persist if the status has changed.

In order to support the future in which storage clusters may have multiple replicas, we will need to update the schemas for the `mz_{source|sink}_status_history` relations to include a `replica_id`. This is necessary for the reason that dropping a replica in a storage cluster with multiple replicas shouldn't set the status of a source, for example, to `paused`. Isolating status updates for a source/sink to a specific replica allows any derived relations such as `mz_{source|sink}_statuses` to make determinations based on all the replicas in the cluster.

### Testing

The current test suite for verifying source/sink statuses should carry over, given that there are no breaking changes to the status entries themselves. However, we should add new tests for the new `paused` status described above.

### Rollout strategy

There is opportunity for the above work to be broken down into a couple of smaller chunks. The PRs involved may look like the following:

1. Shift status writes to the storage controller and add functionality for observing new statuses
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
