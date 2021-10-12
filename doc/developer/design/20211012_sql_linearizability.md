# SQL Linearizability

## Summary

A description of the **coordination layer**'s [strict serializability](https://jepsen.io/consistency/models/strict-serializable) guarantees over the SQL interface, and how it provides them.

## Goals

The main goal is to describe the correctness guarantees that are provided and not provided when users access Materialize over a SQL connection.
This includes detailed descriptions of how these are enforced.

## Non-Goals

It is a non-goal to be concerned about performance until we can measure it (however we discuss ways it might be addressed).

## Description

The **coordination layer** (`coord`) provides a SQL interface to Materialize.
For *definite* inputs, this interface is strictly serializable.
For other inputs, the interface is strictly serializable except across restarts.
The use of `AS OF` in any query removes the linearizability guarantee and tracking described below.

For a single startup of Materialize, serializability is already provided as a property of dataflow.
To provide linearizability for a single startup (and thus strict serializability), all SQL transactions (which can be a single statement) must generate correct timestamps for their timeline (identified by `TimelineId`).
For write transactions (`INSERT`/`UPDATE`/`DELETE`), the transaction timestamp must be > any previous read timestamp in their timeline.
For read transactions (`SELECT`/`TAIL`), the transaction timestamp must be >= any previous write timestamp in their timeline.

For definite inputs, the timestamp must have these same properties across restarts.

### Design

Add a `Timeline` type to enforce the read/write relationship described above (writes must increase the timestamp following reads).
It will have methods `get_read_ts` and `get_write_ts` to fetch the current read or write timestamp, and `ensure_at_least` if `coord` needs to advance the timestamp itself.
To `coord`, add a `HashMap<TimelineId, Timeline>` to allow each timeline its own linearizability tracking.
At all places where timestamps are generated (`sequence_peek`, `sequence_tail`, and `sequence_end_transaction`), compare the generated timestamp to the `Timeline`'s read or write timestamp.
If the generated timestamp is less than the `Timeline`'s timestamp, advance generated to `Timeline`'s (forcing this query to be pending more than it would have otherwise).
If the generated timestamp is greater than the `Timeline`'s timestamp, advance `Timeline`'s to generated (using `ensure_at_least`).

Tables will remove their current linearizability tracking and instead use the `EpochMilliseconds` (system time aka realtime) `Timeline`.

When a `Timeline` changes its state and before any response is returned to the user, its state will be persisted to durable storage (sqlite today).
The `HashMap` above will be initialized on each startup with the persisted `Timeline` states.

## Alternatives

When writing `Timeline` state to disk, could we round up to the next second, allowing us to have a maximum of one write per second?
It's unclear what to do for non-realtime timelines here.

When inputs have their `upper` advance, should that instead be used to advance the `Timeline` timestamps?
This would cause even more unnecessary delays than the current proposal.

## Open questions

Catalog changes today are not in dataflow and have a single global state, so they are already linearizable, but not serializable.
Should we improve them to be serializable?
They currently would prevent describing the SQL layer as being strictly serializable.

What is the performance cost of writing to disk for each transaction that increases `Timeline` timestamps?
We do not necessarily have to do one write per transaction (multiple read or write transactions could all use identical timestamps, for example).
If users do not require linearizability across restarts, persisting `Timeline` state to disk could be scheduled and batched, or disabled all together.

What is the query delay cost of enforcing linearizability?
Some queries will have increased delay times if they must wait for their timestamps to close due to some other query bumping the linearizability timestamp.
If users do not require linearizability at all, it (meaning the use of `Timeline`) could be disabled globally (some future CLI flag), per session (some future session `SET` setting), or per statement (with existing `AS OF`)

In the future, should we change the guarantee to be for all persisted inputs (instead of all definite inputs)?
This may allow us to use the persist seal times to initialize the `HashMap` and not depend on sqlite at all.
