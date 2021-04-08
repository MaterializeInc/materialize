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

{{% create-source/syntax-details connector="pubnub" formats="text" envelopes="append-only" %}}

## Examples

### PubNub market data

```sql
CREATE SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
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
    to_timestamp((text::jsonb)->'timestamp')::bigint) AS timestamp
FROM market_orders_raw;
```
