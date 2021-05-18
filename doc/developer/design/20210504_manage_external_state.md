# Managing Materialize's External State

## Summary

Aside from reading and writing streaming data, Materialize may create or destroy external state for particular sources
and sinks. The goal of this design doc is to align on a single way to create and destroy that external state.

## Goals

- Decide if sources and sinks should maintain external state in the same way.
- Decide where external state should be maintained.
- Decide which crate is responsible for creating and destroying external state (ex: `coord`, `dataflow`).

## Non-Goals

- Any source or sink-specific implementations of this, if possible. While those might be used as examples
to motivate our thinking, they should be separately designed and reviewed.

## Description

### Motivation

Some Materialize sources and sinks need to create, maintain, and destroy state external to Materialize. Examples of
this include:
- Kafka sinks [create downstream Kafka topics] based on a user-provided topic prefix. Data is then sent to
the created topic.
- Postgres sources [create an upstream replication slot] in order to read logical replication logs from a Postgres
instance. Data is read from the created slot.

In the future, the following will also require the ability to create, maintain, and destroy external state:
- Any sinks where we create a new topic, partition, table, bucket, or other object in an external system.
- Other direct replication sources (such as MySQL).

Futhermore, external state is not necessarily associated 1-1 with a source.
Materialize will currently spawn as many source *instances* as needed from a
given source, typically one per downstream materialized view. Since it's the
source instances that do the actual communication with external systems there
might be instance-specific state that needs to be maintained. For example,
postgres source instances each need a separate replication slot to read data
from.

[create downstream Kafka topics]: https://materialize.com/docs/sql/create-sink/#kafka-connector
[create an upstream replication slot]: https://www.postgresql.org/docs/13/warm-standby.html#STREAMING-REPLICATION-SLOTS

### Why does this matter?

As we add more sources and sinks, the more uniformly we implement them the better. Even if we don't move forward with
this design, it would be great to get on the same page about how we should manage external state as we all implement new
features.

Kafka sinks, which are the only completed objects that manage external state at the moment, do so in `coord` (via its
[`sink_builder`]). This proposal would not require any changes there.

Postgres sources, on the other hand, create upstream replication slots in the `dataflow` crate. If we wanted to move
forward with this proposal, that detail of Postgres sources would need to be rewritten.

[`sink_builder`]: https://github.com/MaterializeInc/materialize/commit/c4b96211ba3e6bfeb739bf5f288a42190d119157

### Proposal

Currently, there are two crates where we could handle external state: [`coord`] and [`dataflow`]. I propose that all logic that
manages external state should exist in [`coord`]. (dataflow is explored as an alternative later on.) As per it's documentation,
the `coord` crate is responsible for coordinating client requests with the dataflow layer. This is primarily handled via the
Coordinator, which is currently responsible for:
```
//!   * Launching the dataflow workers.
//!   * Periodically allowing the dataflow workers to compact existing data.
//!   * Executing SQL queries from clients by parsing and planning them, sending
//!     the plans to the dataflow layer, and then streaming the results back to
//!     the client.
//!   * Assigning timestamps to incoming source data.
```

This proposal would add the additional responsibility:
```
//!   * Create, track, and destroy external state that must be handled by Materialize.
```

`coord` and the Coordinator are uniquely positioned to create, track, and destroy external state because it is the only entity
that has access to Materialize's inner Catalog. For this reason, if we want to persist information about managed external state,
that information actually must exist in `coord`.

To make this work, I propose adding a function like the following to `CatalogItem` (which is similar to [Petros' propsal
here]):
```
/// Returns an operator that indicates any external state to be created before
/// we create an item in the catalog.
pub async fn create_state_op(&self) -> Option<ExternalStateOp> {}

/// Returns an operator that indicates any external state to be destroyed once
/// catatlog_transact succeeds.
pub async fn destroy_state_op(&self) -> Option<ExternalStateOp> {}
```

The `ExternalStateOp` enum will be similar to our Catalog `Op`, but it will be responsible for holding the details necessary
to create or destroy external state. `ExternalStateOp` would reside in the `dataflow-types` crate to access the connector-specific
details required to create or destroy that state. But the Coordinator, who will fetch the `ExternalStateOp`s on `CREATE`ing or
`DROP`ping objects, will be responsible for executing them.

I propose that `ExternalStateOp` should have a function like the following:
```
/// Creates or destroys external state as indicated by the ExternalStateOp
pub async fn execute(&self) -> Result<(), anyhow::Error> {
    match &self {
        op1 => { do something external },
        ...
    }
}
```

That `execute` function will by called by the Coordinator in `catalog_transact` once the object in question has been
successfully created or dropped. For a sketch of what this would look like, check out this [WIP PR].

And, as we look to the (potentially distant) future when Materialize will be running a single Coordinator and multiple `dataflow`s,
each `dataflow` not knowing about the existence of the others, I think that this proposal becomes more compelling. We avoid the
distributed systems problem of figuring out which `dataflow` should create/destroy things and how they should communicate that information.
Instead, the Coordinator, which is aware of the fact that there are multiple `dataflow`s running the same thing, can simply handle
that responsibility.

[`coord`]: https://github.com/MaterializeInc/materialize/blob/main/src/coord/src/lib.rs
[`dataflow`]: https://github.com/MaterializeInc/materialize/blob/main/src/dataflow/src/lib.rs
[Petros' proposal here]: https://github.com/MaterializeInc/materialize/pull/6607#discussion_r623746055
[WIP PR]: https://github.com/MaterializeInc/materialize/pull/6607/commits/a1659385bc1f813607fbfce93bbdc374808cf0e1

## Alternatives

An alternative approach is to let the `dataflow` crate be responsible for creating and destroying external state. We could
do this by introducing new `Create` and `Drop` Traits that sources and sinks would have to implement. For sources and sinks
that don't require external state, their implementations of `Create` and `Drop` would simply be no-ops. For other sources and
sinks, the logic required to create and destroy external state would be implemented in these Traits.

A bonus of this approach is that the logic to create or destroy external state related to a database object would be located
near that object's definition.

A challenge of this approach is that if the `dataflow` crate fails to create or destroy external state, we will be stuck with
an error-ing dataflow. The user will have to manually `DROP` this object and recreate it. This is not a challenge if the external
state management is handled in `coord`, where it will error sooner.

## Open questions

* Is there any external state that *needs* to be created in the `dataflow` layer? (Where it's not possible to create it
earlier?)
* Same question as above, but for destroying state.

## Issues raised during discussion

This design document was discussing on 5/5/2021, in the External Integrations weekly team sync. The following thoughts and
issues were raised.

### Source instance state

The Coordinator may spin up a source than in turn spins up multiple source instances. If each of those source instances
requires its own state, and we want to store external state information in `coord`, we will need to enable one of two
things:
1. Have a way for source instances to communicate back to the Coordinator, which could cause race conditions.
1. Pre-create any state needed by a source instance in the Coordinator, and then pass that information to each source
instance via some `Connector` information.

The team agreed that the second approach was preferable, even if it updated the interface of some `dataflow` functions.
However, storing source instance state introduces a bit of persistence complexity. Whatever instance-specific state we
store in the Catalog must be stable across versions or versioned.

Note: the same may be true for sink instances of some type, but was not discussed.

### Source instance error paths

It was brought up that if there were an issue with a source or sink at the `dataflow` layer, the dataflow operators would
not be empowered to clean up state themselves or communicate this back to the Coordinator. While this is a valid objection,
it matches current behavior. If there is an issue with a dataflow, the user will have to manually `DROP` it. Moving forward,
as we provide more useful errors, we may want to revisit this. There are two ways that we can approach doing this:
1. We can let the dataflow layer eagerly clean up its state if an error is encountered, and teach the coordinator not
   to complain if e.g. it goes to clean up a replication slot and that slot is already gone.
1. The dataflow layer could notify the coordinator if it encounters an error, and the coordinator could put the source
   instance into a "cleaned up but not dropped" state.

### pg_replication_slots table in Materialize

It was proposed that we create a new `pg_replication_slots` table in Materialize's Catalog in order to track the external
state of Postgres sources. (This suggestion is Postgres source specific, breaking a bit with the goal of this design doc, but
helping us think of more concrete solutions.) `pg_replication_slots` will be like Materialize's timestamps table.

Alternatively, we could create a new table like `CREATE TABLE source_instance_data (source_id, data)` to achieve the same
goal, without making the table bespoke.

### Add a new CatalogItem variant

It was proposed that, instead of the proposal in this document, that we create a new `CatalogItem` type to track and store
information relevant to external state. Unfortunately, `CatalogItem` is a specific type of resolveable object, and information
about external state does not match that pattern. We will not be moving forward with this alternate proposal.

### Difference between this approach and desired Timestamper refactors

It was brought up that we want to move the Timestamper from `coord` and into `dataflow`, which is the opposite of what this
document proposes for external state. The reason being that each BYO source must share the same Timestamper, and it would
be preferable for each source to get its own thread. This was a valid proposal for the Timestamper, but had two challenges:
1. Kafka's API makes this painful
1. The Timestamper thread wasn't designed in a scalable way

### Handling external state will block

It was raised that the async work to create or destroy state might be slow -- will we block? The answer to this is yes!

## Outcome

After the discussion with the External Integrations team, we agreed that managing external state belongs in `coord` for the
reasons listed above. In order to implement this, we've agreed that we will add a new struct that tracks external state
that will sit beside our current `Connector` information. It is essential that this new state information be directly tied
to individual instances of sources and sinks.

When creating objects in Materialize that create external state, the Coordinator will be responsible for instantiating this
state and passing the required information to the `dataflow` layer. When dropping objects, the Coordinator will be responsible
for using this state information to actually destroy the external state.

A first attempt at managing external state this way was made in [#6714], but has not yet been merged.

[#6714]: https://github.com/MaterializeInc/materialize/pull/6714
