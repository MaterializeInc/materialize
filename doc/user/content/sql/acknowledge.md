---
title: "ACKNOWLEDGE"
description: "`ACKNOWLEDGE` advances the position of a durable subscription."
menu:
  main:
    parent: 'commands'
---

`ACKNOWLEDGE` tells Materialize how far you have processed a [durable
subscription](/sql/create-durable-subscription/), which advances the position it
resumes from and releases the history before it.

## Syntax

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION <name> AT <timestamp>
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
`ACKNOWLEDGE ... AT t` makes back to Materialize.

Do not acknowledge the timestamp of an ordinary row. Not every timestamp
produces a progress message, and a row at time `t` does not mean that time `t`
is complete, so acknowledging it can skip updates you have not seen.

You do not have to subtract anything from the timestamp you acknowledge. When you
resume without an explicit `AS OF`, Materialize positions the subscription so
that you receive updates at and after the acknowledged time, whether or not you
request a snapshot. The subtraction is only needed if you choose to pass [`AS
OF`](/sql/create-durable-subscription/#where-reading-starts) yourself, which is
an exclusive bound.

### Order of operations

Commit your data first, then acknowledge. If you acknowledge before your own
processing is durable, and your application then fails, the acknowledged updates
are gone and cannot be re-delivered.

### Semantics

`ACKNOWLEDGE` is:

*   **Monotone.** Acknowledging a timestamp at or below the current position has
    no effect. It is not an error, so retrying is safe.

*   **Idempotent.** Sending the same acknowledgement twice is indistinguishable
    from sending it once.

*   **Not transactional.** The acknowledgement takes effect immediately and is
    not undone by `ROLLBACK`. This is deliberate: your data really was
    committed, so rolling back must not un-acknowledge it.

Acknowledging a timestamp above the current time of the target object is an
error.

### Where you can run it

`ACKNOWLEDGE` may be run on the same connection as the subscription, interleaved
between `FETCH` statements, or on a separate connection. Because a subscription
is named, no coordination between connections is needed, and because
acknowledgements are monotone, they cannot arrive out of order in any way that
matters.

Running `ACKNOWLEDGE` while no one is reading the subscription is allowed. This
matters for the separate-connection case, where the reading connection may drop
while an acknowledgement is in flight.

### Effect on storage

The acknowledged position determines how much history Materialize retains for
the subscription. Acknowledging more often releases storage sooner; acknowledging
less often reduces round trips but retains more. Materialize records the position
durably on a short interval rather than on every statement, so history is
released slightly after you acknowledge.

## Examples

```mzsql
ACKNOWLEDGE DURABLE SUBSCRIPTION winning_bids_feed AT 1723459200000;
```

Acknowledging from a progress message received on the same connection:

```mzsql
FETCH ALL c WITH (timeout = '1s');
--  mz_timestamp  | mz_progressed | mz_diff | ...
--  1723459200000 | t             |         |
ACKNOWLEDGE DURABLE SUBSCRIPTION winning_bids_feed AT 1723459200000;
```

## Related pages

*   [`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/)
*   [`SUBSCRIBE`](/sql/subscribe/)
