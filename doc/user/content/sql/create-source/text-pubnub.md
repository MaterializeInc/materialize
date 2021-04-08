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

{{% create-source/syntax-details connector="pubnub" formats="text" envelopes="append-only" %}}

## Examples

### PubNub raw market data

```sql
CREATE MATERIALIZED SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
CHANNEL 'pubnub-market-orders';
```

This creates a source that...

- Connects to the PubNub demo market orders channel.
- Treats message values as text.
