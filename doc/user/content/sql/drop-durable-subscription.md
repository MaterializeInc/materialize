---
title: "DROP DURABLE SUBSCRIPTION"
description: "`DROP DURABLE SUBSCRIPTION` removes a durable subscription and releases the history it retains."
menu:
  main:
    parent: 'commands'
---

`DROP DURABLE SUBSCRIPTION` removes a [durable
subscription](/sql/create-durable-subscription/) and releases the history it was
retaining.

## Syntax

```mzsql
DROP DURABLE SUBSCRIPTION [IF EXISTS] <name>
;
```

| Field | Use |
| --- | --- |
| `IF EXISTS` | Do not return an error if the subscription does not exist. |
| `<name>` | The durable subscription to remove. |

## Details

Dropping a durable subscription releases its hold on the target's history
immediately. Any reader currently streaming from it fails with an error.

The position is gone. Recreating a subscription with the same name gives you a
new one positioned at the current time, not the one you dropped, so the next
read needs a snapshot. To keep a subscription's identity and privileges while
moving it to the current time, use [`ALTER DURABLE SUBSCRIPTION ...
RESET`](/sql/alter-durable-subscription/) instead.

Dropping the subscription is also how you stop paying for retained history that
nobody is reading. A subscription that is merely idle continues to retain
history until its time to live expires it.

## Examples

```mzsql
DROP DURABLE SUBSCRIPTION winning_bids_feed;
```

## Related pages

*   [`CREATE DURABLE SUBSCRIPTION`](/sql/create-durable-subscription/)
*   [`ALTER DURABLE SUBSCRIPTION`](/sql/alter-durable-subscription/)
