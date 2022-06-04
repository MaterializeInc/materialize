---
title: "CREATE SOURCE: PubNub"
description: "Connecting Materialize to a PubNub channel"
menu:
  main:
    parent: 'create-source'
    name: PubNub
aliases:
    - /sql/create-source/text-pubnub
    - /sql/create-source/json-pubnub
---

{{% create-source/intro %}}
This page describes how to connect Materialize to a [PubNub](https://www.pubnub.com) channel, which provides a quick way to get up and running with no external dependencies before plugging in your own data sources.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-pubnub.svg" >}}

#### `with_options` {#with-options}

{{< diagram "with-options.svg" >}}

{{% create-source/syntax-connector-details connector="pubnub" envelopes="append-only" %}}

## Features

### Generating sample data

PubNub provides multiple [sample data streams](https://www.pubnub.com/developers/realtime-data-streams/) that you can connect to and play around with as you ramp up with Materialize. To subscribe to a [PubNub channel](https://www.pubnub.com/docs/channels/subscribe), you must provide a `SUBSCRIBE KEY` and a `CHANNEL`:

```sql
CREATE SOURCE twitter_raw
FROM PUBNUB
SUBSCRIBE KEY 'sub-c-78806dd4-42a6-11e4-aed8-02ee2ddab7fe'
CHANNEL 'pubnub-twitter';
```

The subscribe key provided must have permission to access the channel. For more details and a complete list of sample data streams, see the [PubNub documentation](https://www.pubnub.com/docs/).

## Examples

### Creating a source

Using the [market orders](https://www.pubnub.com/developers/realtime-data-streams/financial-securities-market-orders/) PubNub data stream:

```sql
CREATE SOURCE market_orders_raw
FROM PUBNUB
SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
CHANNEL 'pubnub-market-orders';
```

The source will have one column containing JSON (named `text` by default). To use this data in views, you can extract the message fields using
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

## Related pages

- [`CREATE SOURCE`](../)
- [Get Started](/get-started)
