---
title: "CREATE SOURCE: JSON over PubNub"
description: "Learn how to connect Materialize to a JSON-formatted PubNub stream"
menu:
  main:
    parent: 'create-source'
---

{{% create-source/intro %}}
This document details how to connect Materialize to a JSON–formatted
[PubNub](https://www.pubnub.com) channel.

{{< volatility-warning >}}PubNub{{< /volatility-warning >}}
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-pubnub-text.svg" >}}

#### `with_options`

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-details connector="pubnub" formats="text json-bytes" envelopes="append-only" keyConstraint=false %}}

## Examples

### PubNub market data

```sql
CREATE SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-99084bc5-1844-4e1c-82ca-a01b18166ca8'
CHANNEL 'pubnub-market-orders';
```

This creates a source that...

- Connects to the PubNub demo market orders channel.
- Treats message values as text.

To use this data in views, you can extract the message fields using
Materialize's [`jsonb`](/sql/types/jsonb) functions:

```sql
CREATE MATERIALIZED VIEW market_orders AS
SELECT
    (text::jsonb)->>'bid_price' AS bid_price,
    (text::jsonb)->>'order_quantity' AS order_quantity,
    (text::jsonb)->>'symbol' AS symbol,
    (text::jsonb)->>'trade_type' AS trade_type,
    to_timestamp(((text::jsonb)->'timestamp')::bigint) AS timestamp
FROM market_orders_raw;
```
