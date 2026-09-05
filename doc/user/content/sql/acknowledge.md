---
title: "ACKNOWLEDGE"
description: "`ACKNOWLEDGE` advances the position of a durable subscription."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`ACKNOWLEDGE` tells Materialize how far you have processed a [durable
subscription](/sql/create-durable-subscription/), which advances the position it
resumes from and releases the history before it.

## Syntax

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION <name> UP TO <timestamp>
;
```

| Field | Use |
| --- | --- |
| `<name>` | The durable subscription to advance. |
| `<timestamp>` | An [`mz_timestamp`](/sql/types/mz_timestamp/). Asserts that you have durably processed every update at times **strictly before** this value. |

## Details

### What to acknowledge

Acknowledge the `mz_timestamp` of a progress message, which is why
[`SUBSCRIBE`](/sql/subscribe/#progress) must be run `WITH (PROGRESS)` when you
intend to acknowledge. A progress message with timestamp `t` means no further
updates will arrive at times strictly before `t`, which is exactly the claim
`ACKNOWLEDGE ... UP TO t` makes back to Materialize. The bound is exclusive in
both directions, so the number you read from the progress message is the number
you send back unchanged.

Do not acknowledge the timestamp of an ordinary row. Not every timestamp
produces a progress message, and a row at time `t` does not mean that time `t`
is complete, so acknowledging it can skip updates you have not seen.

`UP TO` is exclusive here for the same reason it is exclusive on
[`SUBSCRIBE`](/sql/subscribe/#up-to), which makes the batch pattern symmetric:
read `UP TO` a timestamp, then acknowledge `UP TO` that same timestamp.

### Order of operations

Commit your data first, then acknowledge. If you acknowledge before your own
processing is durable, and your application then fails, the acknowledged updates
are gone and cannot be re-delivered.

### Semantics

`ACKNOWLEDGE` is:

*   **Monotone.** Acknowledging a timestamp at or below the current position is
    accepted and has no effect, so retrying is safe.

*   **Idempotent.** Sending the same acknowledgement twice is indistinguishable
    from sending it once.

*   **Not transactional.** The acknowledgement takes effect immediately and is
    not undone by `ROLLBACK`. This is deliberate: your data really was
    committed, so rolling back must not un-acknowledge it.

Acknowledging a timestamp beyond the object's write frontier is an error. The
frontier is the largest timestamp for which the subscription could have sent you
a progress message, and it is reported for every object in
[`mz_internal.mz_frontiers`](/reference/system-catalog/mz_internal/#mz_frontiers).

### Where you can run it

`ACKNOWLEDGE` may be run on the same connection as the subscription, interleaved
between `FETCH` statements, or on a separate connection. Because a subscription
is named, no coordination between connections is needed, and because
acknowledgements are monotone, they cannot arrive out of order in any way that
matters.

Running `ACKNOWLEDGE` while no one is reading the subscription is allowed. This
matters for the separate-connection case, where the reading connection may drop
while an acknowledgement is in flight.

### Effect on resuming and on storage

The acknowledged position is where the subscription resumes, and it determines
how much history Materialize retains. Acknowledging more often releases storage
sooner and shortens the replay after a failure; acknowledging less often reduces
round trips. Materialize records the position durably on a short interval rather
than on every statement, so history is released slightly after you acknowledge.

When you resume without an explicit `AS OF`, Materialize positions the
subscription so that you receive updates at and after the acknowledged time. With
`SNAPSHOT true` you receive the state *as of* that time, with updates at that
time already folded in, followed by later updates. Either way you do not subtract
anything. The subtraction is only needed if you choose to pass [`AS
OF`](/sql/create-durable-subscription/#where-reading-starts-and-stops) yourself,
which is an exclusive bound.

## Examples

Acknowledging from a progress message received on the same connection:

```mzsql
FETCH ALL c WITH (timeout = '1s');
```

```nofmt
 mz_timestamp  | mz_progressed | mz_diff | auction_id | amount
---------------+---------------+---------+------------+--------
 1723459199000 | f             |       1 |          1 |     42
 1723459200000 | t             |         |            |
```

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION winning_bids_feed UP TO 1723459200000;
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/acknowledge" %}}

## Related pages

*   [`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/)
*   [`ALTER DURABLE SUBSCRIPTION`](/sql/alter-durable-subscription/)
*   [`DROP DURABLE SUBSCRIPTION`](/sql/drop-durable-subscription/)
*   [`SUBSCRIBE`](/sql/subscribe/)
*   [Resuming subscriptions](/transform-data/patterns/durable-subscriptions/)
