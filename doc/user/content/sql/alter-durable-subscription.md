---
title: "ALTER DURABLE SUBSCRIPTION"
description: "`ALTER DURABLE SUBSCRIPTION` changes the time to live of a durable subscription, or resets one that has expired."
menu:
  main:
    parent: 'commands'
---

`ALTER DURABLE SUBSCRIPTION` changes the time to live of a [durable
subscription](/sql/create-durable-subscription/), or resets one that has
expired.

## Syntax

```mzsql
ALTER DURABLE SUBSCRIPTION <name> SET (TTL = <interval>);
ALTER DURABLE SUBSCRIPTION <name> RESET;
```

| Field | Use |
| --- | --- |
| `SET (TTL = <interval>)` | Change how long you have to acknowledge. Takes effect immediately, including for a subscription that is currently behind. |
| `RESET` | Re-arm an expired subscription at the current time. |

## Details

### Setting the time to live

Increasing the `TTL` gives a reader more time to recover, and increases the
history that may be retained on its behalf. Decreasing it can expire a
subscription immediately, if the reader is already further behind than the new
value allows.

The `TTL` must remain above the system-wide minimum, which exists because the
acknowledged position is recorded durably on an interval. A time to live close to
that interval would expire readers that are acknowledging correctly.

### Resetting an expired subscription

`RESET` moves an expired subscription back to the current time and makes it
usable again. It does not recover the history that was released when the
subscription expired, so the next read must request a snapshot.

Use `RESET` rather than dropping and recreating: it preserves the subscription's
name, owner, and privileges. Requiring it, instead of silently resuming from
whatever history happens to remain, is what keeps a gap in the data from passing
unnoticed.

`RESET` on a subscription that has not expired is an error. Fencing a live reader
by resetting its position is not something to do by accident.

## Examples

```mzsql
ALTER DURABLE SUBSCRIPTION winning_bids_feed SET (TTL = '5m');
```

```mzsql
ALTER DURABLE SUBSCRIPTION winning_bids_feed RESET;
```

## Related pages

*   [`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/)
*   [`DROP DURABLE SUBSCRIPTION`](/sql/drop-durable-subscription/)
