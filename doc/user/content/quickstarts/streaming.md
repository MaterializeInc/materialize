---
title: "Streaming analytics"
description: "How to build a live business intelligence dashboard using Materialize and Metabase"
menu:
  main:
    parent: 'quickstarts'
    weight: 30
    name: 'Streaming analytics'
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
        SASL MECHANISMS = 'SCRAM-SHA-256',
        SASL USERNAME = 'TBD',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

1. Create the sources, one per Kafka topic:
    <!-- ```sql
    CREATE TABLE IF NOT EXISTS users (
        id INT,
        email TEXT,
        is_vip BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS items (
        id INT,
        name TEXT,
        category TEXT,
        price INT,
        inventory INT,
        inventory_updated_at TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS purchases (
        id INT,
        user_id BIGINT,
        item_id BIGINT,
        status INT,
        quantity INT,
        purchase_price INT,
        deleted BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    INSERT INTO users VALUES (1, 'random@email.com', true, current_timestamp(), current_timestamp());
    INSERT INTO items VALUES (1, 'Random Random', 'Category', 230, 3, current_timestamp(), current_timestamp(), current_timestamp());
    INSERT INTO purchases VALUES (1, 1, 1, 1, 3, 10, false, current_timestamp(), current_timestamp());
    ``` -->

    ```sql
    CREATE SOURCE purchases
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'purchases')
    FORMAT ENVELOPE DEBEZIUM USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    WITH (SIZE = '3xsmall');

    CREATE SOURCE items
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'items')
    FORMAT ENVELOPE DEBEZIUM USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    WITH (SIZE = '3xsmall');

    CREATE SOURCE users
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'users')
    FORMAT ENVELOPE DEBEZIUM USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_basic_http
    WITH (SIZE = '3xsmall');
    ```

    <!-- Each topic contains different data:
    * Items: items by category and stock
    * Users: registration and vip access
    * Purchases: item purchases from users -->

### Build the analytics

Materialized views compute and maintain the results of a query incrementally. Use them to build up-to-date analytics results at **lightning speeds**.

Reuse your `psql` session and build the analytics:

1. Create a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) to aggregate the purchase prices for vip users that haver not been refunded.

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
        WHERE deleted is false
        GROUP BY 1, 2;
    ```

    The above model will update as new purchases come in and if existing purchases are marked as deleted later (soft deletes). Usually handling soft deletes or retractions is very difficult for streaming systems. Most other systems would require you to store that dimensional data in state indefinitely but with our views in Materialize we do not need to worry about the additional challenges of this use case.

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

### Connect to a BI Tool

Instead of subscribing to the changes in the materialized view, you can connect to a BI tool directly and create a dashboard that keeps track of its results in real time . Here, we'll use [Metabase](https://www.metabase.com/), but most tools that support PostgreSQL should work out-of-the-box since Materialize is wire-compatible with PostgreSQL.

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

1. To build a visualization on top of the `vip_purchases` materialized view, click "Browse Data". Then, drill down until you find the view. By default, the visualization will be a table, but you can change this to e.g. a "Bar" chart and get live updates for new purchases as they are created or deleted in the upstream MySQL database.

## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   How to connect a BI tool to Materialize for data visualization

## Learn more

-   [`CREATE SOURCE`](/sql/create-source)
-   [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
