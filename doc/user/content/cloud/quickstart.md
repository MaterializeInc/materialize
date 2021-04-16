---
title: "Materialize Cloud Quickstart"
description: "Set up a Materialize Cloud account, create deployments, and connect to data sources."
menu:
  main:
    parent: "cloud"
    weight: 2
---

{{< cloud-notice >}}


To get you started, we'll walk you through the following:

1. Signing up for Materialize Cloud
2. Creating a Materialize Cloud deployment
3. Connecting to the deployment from a terminal
4. Connecting to a data source
5. Running sample queries

## Sign up for Materialize Cloud

Sign up at [https://cloud.materialize.com](https://cloud.materialize.com#signup).

## Create and connect to a Materialize Cloud deployment

Once you sign up for Materialize Cloud and [log in](https://cloud.materialize.com), you use the [Deployments](https::/cloud.materialize.com/deployments) page to create, upgrade, or destroy deployments, and to obtain the TLS certificates you need to install on your local machine so you can connect to deployments.

By default, you can create up to two deployments. If you're interested in more, [let us know](../support).

1. On the [Deployments](https::/cloud.materialize.com/deployments) page, click **Create deployment**. Materialize creates a deployment and assigns it a name and hostname.

{{% cloud-connection-details %}}

## Connect to a real-time stream and a create materialized view

For this example, we'll walk you through connecting to a [PubNub stream](https://www.pubnub.com/developers/realtime-data-streams/) as a data source. Note that PubNub demo streams should only be used for testing, since they do not meet the consistency and durability requirements necessary for Materialize to guarantee correctness over time.

1. From your shell, create a source (connect to the PubNub market orders stream):

    ```sql
    CREATE SOURCE market_orders_raw FROM PUBNUB
      SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
      CHANNEL 'pubnub-market-orders';
    ```
    This streams data as a single text column containing JSON.
2. Extract the JSON fields for each order's stock symbol and the bid price:

    ```sql
    CREATE VIEW market_orders AS
      SELECT
        val->>'symbol' AS symbol,
        (val->'bid_price')::float AS bid_price
      FROM (SELECT text::jsonb AS val FROM market_orders_raw);
    ```
3. Create a materialized view that determines the average bid price, then return the average:

    ```sql
    CREATE MATERIALIZED VIEW avg_bid AS
      SELECT symbol, AVG(bid_price) FROM market_orders
      GROUP BY symbol;
    SELECT * FROM avg_bid;
    ```
    ```
      symbol    |        avg
    ------------+--------------------
    Apple       |  199.3392717416626
    Google      | 299.40371152970334
    Elerium     | 155.04668809209852
    Bespin Gas  |  202.0260593073953
    Linen Cloth | 254.34273792647863
    ```
    Wait a few moments and issue `SELECT * FROM avg_bid;` again to get an updated result based on the latest data streamed in.

## Related topics

* [Connect to Materialize Cloud](../connect-to-materialize-cloud)
* [Materialize Cloud Account Limits](../account-limits)
* [Materialize Architecture](../../overview/architecture)
* [`CREATE SOURCE`](../../sql/create-source)
