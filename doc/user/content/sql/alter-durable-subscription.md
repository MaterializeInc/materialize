---
title: "ALTER DURABLE SUBSCRIPTION"
description: "`ALTER DURABLE SUBSCRIPTION` changes the acknowledgement deadline of a durable subscription, resets one that has expired, or transfers ownership."
menu:
  main:
    parent: 'commands'
---

{{< private-preview />}}

`ALTER DURABLE SUBSCRIPTION` changes the acknowledgement deadline of a [durable
subscription](/sql/create-durable-subscription/), resets one that has expired, or
transfers its ownership.

## Syntax

```mzsql
ALTER DURABLE SUBSCRIPTION <name> SET (ACKNOWLEDGE WITHIN <interval>);
ALTER DURABLE SUBSCRIPTION <name> RESET;
ALTER DURABLE SUBSCRIPTION <name> OWNER TO <new_owner>;
```

| Field | Use |
| --- | --- |
| `SET (ACKNOWLEDGE WITHIN <interval>)` | Change how long you may go without acknowledging. Takes effect immediately, including for a subscription that is currently behind. |
| `RESET` | Re-arm an expired subscription at the current time. |
| `OWNER TO <new_owner>` | Transfer ownership to another role. |

## Details

### Changing the acknowledgement deadline

Increasing the deadline gives a reader more time to recover, and increases the
history that may be retained on its behalf. Decreasing it can expire a
subscription immediately, if the reader has already gone longer than the new
value without acknowledging.

The value must fall between the system-wide minimum and maximum, the same bounds
[`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/#acknowledgement-deadline)
enforces. A minimum exists because the acknowledged position is recorded durably
on an interval, so a deadline close to that interval would expire readers that
are acknowledging correctly.

### Resetting an expired subscription

`RESET` moves an expired subscription to the current time and makes it usable
again. It does not recover the history that was released when the subscription
expired, so the next read must request `SNAPSHOT true` to get a usable starting
state.

Use `RESET` rather than dropping and recreating: it preserves the subscription's
name, owner, and privileges. Requiring it, instead of silently resuming from
whatever history happens to remain, is what keeps a gap in the data from passing
unnoticed.

`RESET` on a subscription that has not expired is an error. Fencing a live reader
by resetting its position is not something to do by accident.

## Examples

```mzsql
ALTER DURABLE SUBSCRIPTION winning_bids_feed SET (ACKNOWLEDGE WITHIN '5m');
```

```mzsql
ALTER DURABLE SUBSCRIPTION winning_bids_feed RESET;
```

```mzsql
ALTER DURABLE SUBSCRIPTION winning_bids_feed OWNER TO analytics_owner;
```

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/alter-durable-subscription" %}}

## Related pages

*   [`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/)
*   [`ACKNOWLEDGE`](/sql/acknowledge/)
*   [`DROP DURABLE SUBSCRIPTION`](/sql/drop-durable-subscription/)
*   [`SUBSCRIBE`](/sql/subscribe/)
*   [Resuming subscriptions](/transform-data/patterns/durable-subscriptions/)
