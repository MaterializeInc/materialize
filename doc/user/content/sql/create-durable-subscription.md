---
title: "CREATE DURABLE SUBSCRIPTION"
description: "`CREATE DURABLE SUBSCRIPTION` creates a named, resumable subscription whose progress Materialize tracks for you."
menu:
  main:
    parent: 'commands'
---

`CREATE DURABLE SUBSCRIPTION` creates a named subscription that Materialize can
resume. Materialize tracks how far you have processed, and retains exactly the
history you still need.

## Conceptual framework

A plain [`SUBSCRIBE`](/sql/subscribe/) has no memory. If the connection drops,
the next `SUBSCRIBE` starts over, and unless you configured a [history retention
period](/transform-data/patterns/durable-subscriptions/) and recorded your own
timestamps, it starts over with a full snapshot.

A durable subscription moves both of those responsibilities into Materialize:

*   **Materialize remembers your position.** You acknowledge what you have
    processed with [`ACKNOWLEDGE`](/sql/acknowledge/), and Materialize stores
    that position durably. Your application does not need to persist a
    timestamp, which matters for clients that have nowhere durable to put one,
    such as a browser or an edge function.

*   **Materialize retains exactly the history you need.** The acknowledged
    position, not a fixed duration, determines how much history is kept. You do
    not have to guess a history retention period that is long enough to cover
    an outage but short enough to afford.

The trade for that convenience is delivery semantics. A durable subscription is
**at-least-once**: after you reconnect, you may see updates you already
processed. See [Delivery semantics](#delivery-semantics).

## Syntax

```mzsql
CREATE DURABLE SUBSCRIPTION <name> ON <object_name>
WITH (TTL = <interval>)
;

CREATE DURABLE SUBSCRIPTION <name>
WITH (TTL = <interval>) AS
SELECT <columns> FROM <object_name> [WHERE <predicate>]
;
```

| Field | Use |
| --- | --- |
| `<name>` | A name for the subscription. Used by [`SUBSCRIBE`](/sql/subscribe/) and [`ACKNOWLEDGE`](/sql/acknowledge/). |
| `<object_name>` | The source, table, or materialized view to subscribe to. |
| `TTL` | **Required.** How long you have to acknowledge. A positive [interval](/sql/types/interval/) value, for example `'1m'`. See [Time to live](#time-to-live). |
| `AS SELECT ...` | An optional projection and filter over a single object. See [Supported objects and queries](#supported-objects-and-queries). |

## Details

### Supported objects and queries

A durable subscription can target a source, a table, or a materialized view.
Views and indexes are not supported, because Materialize cannot durably retain
history for them.

The `AS SELECT` form accepts a projection and a filter over a **single** object,
which Materialize evaluates while reading it. This covers selecting a subset of
columns, computing derived columns, and filtering rows. It does not create a
dataflow and adds no compute cost, which is why it does not change the resume
behavior described below.

Anything more than that is rejected, including joins, aggregations, `DISTINCT`,
subqueries, and more than one object in the `FROM` clause. Temporal filters,
meaning predicates over [`mz_now()`](/sql/functions/now_and_mz_now/), are also
rejected. To subscribe durably to a query of that kind, create a [materialized
view](/sql/create-materialized-view/) for it and target the view. That is also
the faster option at resume time: resuming against a stored collection reads
recent data from storage, whereas subscribing to a query builds a new dataflow
that must rehydrate first.

A projection can cause distinct rows to become identical, in which case their
changes combine. The result is still a correct stream of changes to the
projected relation.

If the projection or filter produces an error for some row, for example a
division by zero, the subscription returns that error. Your position is
unchanged, because only [`ACKNOWLEDGE`](/sql/acknowledge/) moves it, so
reconnecting returns the same error until the offending data changes.

### Time to live

`TTL` is the maximum time you may go without acknowledging. It bounds both your
exposure and Materialize's storage:

*   If you acknowledge within the `TTL`, Materialize guarantees that the
    unacknowledged history is still available when you reconnect.

*   If you do not, the subscription **expires**. Materialize stops retaining
    history for it, and the next attempt to use it fails with an error rather
    than silently skipping the gap. See [Expiry](#expiry).

Choose a `TTL` that covers the outages you expect to recover from, plus the time
your application needs to restart. A minute is a reasonable starting point for
an interactive client. There is a system-wide maximum; ask your administrator if
you need a longer one.

A durable subscription retains history from the moment you create it, whether or
not anything is reading from it. Creating one and never using it retains history
until the `TTL` expires it.

History is retained for the object according to the **furthest behind** of its
readers. If several durable subscriptions target one object, one reader that
stops acknowledging holds history for all of them until its `TTL` expires it.
Keep the `TTL` no longer than you need for this reason, not only for your own
storage.

### Intended use

Create a durable subscription per logical consumer, once, and reuse it across
reconnections. It is a provisioned resource, like a table or a view: it is
recorded durably, it appears in the catalog, and creating or dropping one is a
data definition statement.

Do not create one per page load or per session. Creating and dropping thousands
of short-lived subscriptions is far more expensive than reusing a few long-lived
ones, and every subscription that exists adds to startup work. Give each
consumer a stable name and reconnect to it.

### Expiry

When a subscription expires, it is not dropped. It remains visible in
[`mz_internal.mz_durable_subscriptions`](#monitoring), showing the position it
expired at and how long it had gone without acknowledging, so you can tell an
expiry apart from a subscription that never existed.

Attempting to subscribe using an expired subscription returns an error. To start
using it again, run [`ALTER DURABLE SUBSCRIPTION ... RESET`](/sql/alter-durable-subscription/),
which re-arms it at the current time. Resetting acknowledges that you are
accepting a gap in the data and will need a new snapshot.

### Progress messages are required

Subscribe `WITH (PROGRESS)` whenever you intend to acknowledge. A progress
message is the only thing that tells you a timestamp is complete, and therefore
the only safe thing to acknowledge. Not every timestamp produces a progress
message, and receiving a row at time `t` does not mean that time `t` is
finished, so acknowledging a row's timestamp can skip updates you have not seen.

### Where reading starts

By default, omit `AS OF` and Materialize resumes from the position you last
acknowledged, computing the boundary so that you receive updates at and after
that position. This requires nothing from you and is the recommended path.

You may pass `AS OF` to start somewhere else inside the retained history, which
is useful if you track your own position and are further along than your last
acknowledgement. It behaves exactly as it does for a plain
[`SUBSCRIBE`](/sql/subscribe/#as-of).

{{< warning >}}

`AS OF` is an **exclusive** lower bound under `SNAPSHOT false`: `SUBSCRIBE` emits
updates at times *strictly greater* than the timestamp you pass. To receive
updates at time `T`, pass `AS OF T - 1`. Passing `T` silently skips every update
at `T`.

If you would rather not carry that arithmetic, omit `AS OF` and filter instead.
Every update carries `mz_timestamp`, so a consumer that has committed through `T`
can drop everything below `T` on arrival. Receiving data you already have costs
bandwidth; asking for the wrong starting timestamp costs data.

{{</ warning >}}

`UP TO` is also accepted, and is useful for draining a bounded batch: read up to
a timestamp, commit, acknowledge that same timestamp, and disconnect.

### Snapshots

`SNAPSHOT` defaults to **`false`** here, which is the opposite of a plain
[`SUBSCRIBE`](/sql/subscribe/#snapshot). Resuming without re-snapshotting is the
purpose of a durable subscription, so it is the default.

A requested snapshot is taken **at your acknowledged position**, not at the
current time, so that the state you receive corresponds to a position you can
name. That gives three ways to reconnect:

*   **Continue**, with `SNAPSHOT false`. You hold state at or past your
    acknowledged position and want the changes since. This is the default and
    the common case.

*   **Reconcile**, with `SNAPSHOT true`. You know your position but suspect your
    local state has drifted. You receive the authoritative state as of your
    acknowledged position, which you can compare against what you hold, followed
    by subsequent changes.

*   **Start over**, with [`ALTER DURABLE SUBSCRIPTION ...
    RESET`](/sql/alter-durable-subscription/) and then subscribing. You have lost
    your local state and want current data. Reconciling at an old position would
    hand you historical state plus every change since, which is more work than
    starting fresh.

### Delivery semantics

A durable subscription delivers **at least once**. You process a batch, commit
it, and then acknowledge. If your application fails between the commit and the
acknowledgement, Materialize still has the older position, so it re-sends
updates you already processed.

Your consumer must therefore be idempotent. Deduplicate on the combination of
`mz_timestamp` and the row, and do not treat the stream as an event log: after a
resume, the same logical change may arrive consolidated differently than it did
the first time.

{{< important >}}

If you need **exactly-once** processing, keep recording your own progress
timestamp and writing it in the same transaction as the data it covers, as
described in [Durable
subscriptions](/transform-data/patterns/durable-subscriptions/#note-about-idempotency).

You can still use a durable subscription for this. Acknowledge as normal to
control how much history is retained, and on reconnection discard every update
below your own committed timestamp. Materialize supplies the guarantee that the
history is still there; your recorded timestamp supplies the deduplication.

{{</ important >}}

### Privileges

Creating a durable subscription requires `SELECT` on the target object.
`SELECT` is checked again each time you subscribe, so revoking it stops an
existing subscription from being used.

Dropping the target object fails while a durable subscription exists on it,
unless you use `CASCADE`.

Changing the target's columns, for example with `ALTER TABLE ... ADD COLUMN`,
expires every durable subscription on it. The shape of the stream cannot change
underneath a reader.

### Monitoring

`mz_internal.mz_durable_subscriptions` reports each subscription's acknowledged
position, how long it has gone without acknowledging, how far behind the object
it is, and whether it has expired. Use it to find subscriptions that are holding
history:

```mzsql
SELECT name, target, acknowledged_at, time_since_ack, lag, state
FROM mz_internal.mz_durable_subscriptions
ORDER BY time_since_ack DESC;
```

`time_since_ack` is what the `TTL` is compared against, so it is the column that
predicts an expiry. `lag` is the distance to the object's current time, which is
what predicts how much data a reconnection will replay.

This relation is distinct from
[`mz_internal.mz_subscriptions`](/reference/system-catalog/mz_internal/#mz_subscriptions),
which lists `SUBSCRIBE` statements that are running right now. A durable
subscription appears in `mz_durable_subscriptions` whether or not anything is
currently reading from it.

## Examples

### Create a subscription

```mzsql
CREATE DURABLE SUBSCRIPTION winning_bids_feed
ON winning_bids
WITH (TTL = '1m');
```

### Read from it the first time

The first time you read, request a snapshot to bootstrap your application.
`PROGRESS` is required, because progress messages are what tell you a timestamp
is complete and therefore safe to acknowledge.

```mzsql
BEGIN;
DECLARE c CURSOR FOR
  SUBSCRIBE USING DURABLE SUBSCRIPTION winning_bids_feed
  WITH (PROGRESS, SNAPSHOT true);
```

Then loop: fetch a batch, and buffer it until a progress message arrives.

```mzsql
FETCH ALL c WITH (timeout = '1s');
```

When you receive a row with `mz_progressed` set to `true`, everything before its
`mz_timestamp` is complete. Process the buffered data, commit it, and then
acknowledge that timestamp on the same connection:

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION winning_bids_feed AT 1723459200000;
```

You do not need to acknowledge every progress message. Acknowledging less often
reduces round trips, at the cost of re-processing more data after a failure and
retaining more history in the meantime.

### Resume after a disconnection

Reconnect and subscribe again, this time without a snapshot. You do not supply a
timestamp, because Materialize has it:

```mzsql
BEGIN;
DECLARE c CURSOR FOR
  SUBSCRIBE USING DURABLE SUBSCRIPTION winning_bids_feed
  WITH (PROGRESS, SNAPSHOT false);
```

The first message you receive is a progress message carrying the timestamp the
subscription resumed at, so your application can confirm it matches what it
expected.

If your application also lost its local state, request `SNAPSHOT true` instead.
You then receive a snapshot as of the acknowledged position, followed by
subsequent updates.

### Only one reader at a time

A durable subscription has a single position, so only one reader may use it at a
time. Subscribing again takes over: the new reader starts streaming and the
previous reader's stream fails with an error.

This is deliberate. It means a client that reconnects does not have to wait for
its own abandoned connection to time out. It also means you should not point two
application instances at the same durable subscription, and if you do, they will
take turns rather than share the work.

### Acknowledging from another connection

If you read with a streaming `SUBSCRIBE` rather than `DECLARE` and `FETCH`, the
PostgreSQL protocol does not allow you to send anything on that connection while
results are streaming. Acknowledge from a second connection instead. A
subscription is addressed by name, so no coordination between the connections is
needed:

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION winning_bids_feed AT 1723459200000;
```

## Related pages

*   [`ACKNOWLEDGE`](/sql/acknowledge/)
*   [`ALTER DURABLE SUBSCRIPTION`](/sql/alter-durable-subscription/)
*   [`DROP DURABLE SUBSCRIPTION`](/sql/drop-durable-subscription/)
*   [`SUBSCRIBE`](/sql/subscribe/)
*   [Durable subscriptions](/transform-data/patterns/durable-subscriptions/)
