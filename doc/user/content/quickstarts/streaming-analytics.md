---
title: "Build a real-time dashboard"
description: "How to build a real-time Business Intelligence (BI) dashboard using Materialize and Metabase"
menu:
  main:
    parent: 'quickstarts'
    weight: 20
    name: 'Build a real-time dashboard'
draft: true
aliases:
  - /demos/streaming-analytics
---

In this quickstart, you will support the analytics needs of our fictional eCommerce business. In this case we have streaming data coming in from Kafka in a variety of formats from various sources. We want to apply some logic to this data as soon as new events occur and set up some reporting on top.

In the world of streaming this can be a big ask. But the demo will showcase the flexibility of Materialize to support a wide range of transformations using traditional SQL along with the compatibility to work with a number of BI tools, for this demo we will be using Metabase.

The key concepts present in this quickstart will also apply to many other projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)

### Prepare the environment

1. Set up a [Materialize account.](/register)

1. [Install psql and connect](https://materialize.com/docs/get-started/#connect) to Materialize.

### Create the sources

Materialize provides public Kafka topics and a Confluent Schema Registry for its users. The Kafka topics contain data from a fictional eCommerce website and receive updates every second. You will use this sample data to build the analytics for a live-updating dashboard.

1. In your `psql` terminal, create a new [cluster](https://materialize.com/docs/sql/create-cluster/) and [schema](https://materialize.com/docs/sql/create-schema/):

    ```sql
    CREATE CLUSTER demo REPLICAS (r1 (SIZE = 'xsmall'))
    CREATE SCHEMA shop;
    ```

1. Within the same `psql` terminal, we will switch to the cluster and schema we just created. This way everything done for this demo will be safely isolated from any other workflows we may have running:

    ```sql
    SET cluster = demo;
    SET SCHEMA shop;
    ```

1. Create [a connection](/sql/create-connection/#confluent-schema-registry) to the Confluent Schema Registry:
    ```sql
    CREATE SECRET IF NOT EXISTS csr_username AS '<TBD>';
    CREATE SECRET IF NOT EXISTS csr_password AS '<TBD>';

    CREATE CONNECTION csr_basic_http
    FOR CONFLUENT SCHEMA REGISTRY
    URL '<TBD>',
    USERNAME = SECRET csr_username,
    PASSWORD = SECRET csr_password;
    ```

1. Create [a connection](/sql/create-connection/#kafka) to the Kafka broker:

    ```sql
    CREATE SECRET kafka_password AS '<TBD>';

    CREATE CONNECTION kafka_connection TO KAFKA (
        BROKER 'TBD',
        SASL MECHANISMS = 'PLAIN',
        SASL USERNAME = 'TBD',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

1. Create the sources, one per Kafka topic:

    ```sql
    CREATE SOURCE IF NOT EXISTS purchases
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'mysql.shop.purchases')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');

    CREATE SOURCE IF NOT EXISTS items
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'mysql.shop.items')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');

    CREATE SOURCE IF NOT EXISTS users
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'mysql.shop.users')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');
    ```

### Build the analytics

Materialized views compute and maintain the results of a query incrementally. Use them to build up-to-date analytics results at **lightning speeds**.

Reuse your `psql` session and build the analytics:

1. Create a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) to aggregate the purchase prices for vip users that have not been refunded.

    A `MATERIALIZED VIEW` is persisted in durable storage and is incrementally updated as new data arrives.

    ```sql
    CREATE MATERIALIZED VIEW vip_purchases AS
        SELECT
            user_id,
            is_vip,
            SUM(purchase_price) as total_purchase_price
        FROM purchases
        LEFT JOIN users
            ON purchases.user_id = users.id
        WHERE deleted = 0
        GROUP BY 1, 2;
    ```

    The above model will update as new purchases come in, as well as when existing purchases are marked as deleted (soft deletes). Handling soft deletes is a challenging use case in streaming, and most systems can only provide eventually consistent results. Materialize not only provides strong consistency guarantees when handling the resulting retractions, but also minimizes the amount of state that needs to be maintained for the join.

    1. Create a view that takes the on-time bids and finds the highest bid for each auction:

    ```sql
    CREATE VIEW highest_bid_per_auction AS
        SELECT grp.auction_id,
               bid_id,
               buyer,
               seller,
               item,
               amount,
               bid_time,
               end_time
        FROM
        (SELECT DISTINCT auction_id FROM on_time_bids) grp,
        LATERAL (
            SELECT * FROM on_time_bids
            WHERE auction_id = grp.auction_id
            ORDER BY amount DESC LIMIT 1
    );
    ```

    ```
    CREATE MATERIALIZED VIEW remaining_stock AS
    SELECT
      i.id AS item_id,
      MAX(i.inventory) - SUM(purchases.quantity) AS remaining_stock
    FROM items i
    JOIN purchases ON purchases.item_id = i.id
     AND purchases.created_at > i.inventory_updated_at
    GROUP BY i.id;
    ```

### Subscribe to updates

`SUBSCRIBE` can stream updates from materialized views as they occur. Use it to verify how the analytics change over time.

1. Subscribe to the vip purchases:
    ```sql
    COPY (SUBSCRIBE (SELECT * FROM vip_purchases)) TO STDOUT;
    ```

    As soon as we subscribe we will see the current state of our materialized view.
    ```
    1671724467569   1	f	35
    1671724467569   1	t	25
    ```

    If there is a new purchase or a soft delete, the subscription will show two new rows. The previous value and the new value. In this case there was a soft delete of a non vip purchase of $15.
    ```
    1671724467569	1	f	35
    1671724467569	1	t	25
    1671724514737	1	f	20
    1671724514737	-1	f	35
    ```

1. Press `CTRL + C` to interrupt the subscription after a few changes.

### Connect to a BI tool

Instead of subscribing to the changes in the materialized view, you can connect to a BI tool directly and create a dashboard that keeps track of its results in real time. Here, we'll use [Metabase](https://www.metabase.com/), but most tools that support PostgreSQL should work out-of-the-box since Materialize is wire-compatible with PostgreSQL.

1. In Metabase, add a new database connection:

    | Connection Field | Value |
    | --- | --- |
    | Database type | PostgreSQL |
    | Display Name | Materialize |
    | Host | `MATERIALIZE_HOST` |
    | Port | 6875 |
    | Database name | `materialize` |
    | Username | `MATERIALIZE_USERNAME` |
    | Password | `MATERIALIZE_PASSWORD` |
    | Use a secure connection (SSL) | True |

1. To build a visualization on top of the `vip_purchases` materialized view, click **Browse Data**. Then, drill down until you find the view. By default, the visualization will be a "Table", but you can change this to e.g. a "Bar" chart.

1. Click **Auto-refresh**, and select **1 minute**.

  60 seconds is the fastest refresh rate selectable in the UI, but if you copy the URL, open a new tab and edit the end of the url to change the `refresh=60` anchor to `refresh=1`, you can force Metabase to update every second.

[//]: # "TODO(morsapaes) Add GIF of Metabase dashboard."

## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   How to connect a BI tool to Materialize for data visualization

## Learn more

-   [`CREATE SOURCE`](/sql/create-source)
-   [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
