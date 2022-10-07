---
title: "CREATE SOURCE: Text over PubNub"
description: "Learn how to connect Materialize to an text-formatted PubNub stream"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a text–formatted
[PubNub](https://www.pubnub.com) channel.

{{< volatility-warning >}}PubNub{{< /volatility-warning >}}
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-pubnub-text.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="pubnub" formats="text" envelopes="append-only" keyConstraint=false %}}

## Examples

### PubNub raw market data

```sql
CREATE MATERIALIZED SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8'
CHANNEL 'pubnub-market-orders';
```

This creates a source that...

- Connects to the PubNub demo market orders channel.
- Treats message values as text.
