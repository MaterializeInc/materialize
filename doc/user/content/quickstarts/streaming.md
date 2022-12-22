---
title: "Streaming Analytics"
description: "Streaming Analytics"
menu:
  main:
    parent: 'quickstarts'
    weight: 30
    name: 'Streaming Analytics'
draft: true
aliases:
  - /demos/streaming-analytics
---

In this quickstart, you'll build the analytics for an eCommerce sales dashboard using Materialize and a BI tool (Metabase).

The key concepts present in this quickstart will also apply to many other projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)

### Prepare the environment

1. Set up a [Materialize account.](/register)

1. [Install psql and connect](https://materialize.com/docs/get-started/#connect) to Materialize.

### Create the sources

Materialize provides public Kafka topics and a Confluent Schema Registry for its users. The Kafka topics contain data from a fictional eCommerce and receive updates every second. You will use the data in them to build the analytics for the dashboard.

1. In your `psql` terminal, create a new schema:

    ```sql
    CREATE SCHEMA shop;
    ```

1. Create the connection to the Confluent Schema Registry:
    ```sql
    CREATE SECRET IF NOT EXISTS shop.csr_username AS '<TBD>';
    CREATE SECRET IF NOT EXISTS shop.csr_password AS '<TBD>';

    CREATE CONNECTION shop.csr_basic_http
    FOR CONFLUENT SCHEMA REGISTRY
    URL '<TBD>',
    USERNAME = SECRET csr_username,
    PASSWORD = SECRET csr_password;
    ```

1. Create the connection to the Kafka broker:

    ```sql
    CREATE SECRET shop.kafka_password AS '<TBD>';

    CREATE CONNECTION shop.kafka_connection TO KAFKA (
        BROKER 'TBD',
        SASL MECHANISMS = 'SCRAM-SHA-256',
        SASL USERNAME = 'TBD',
        SASL PASSWORD = SECRET shop.kafka_password
    );
    ```

1. Create the sources, one per Kafka topic:
    <!-- ```sql
    CREATE TABLE IF NOT EXISTS shop.users (
        id INT,
        email TEXT,
        is_vip BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS shop.items (
        id INT,
        name TEXT,
        category TEXT,
        price INT,
        inventory INT,
        inventory_updated_at TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS shop.purchases (
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

    INSERT INTO shop.users VALUES (1, 'random@email.com', true, current_timestamp(), current_timestamp());
    INSERT INTO shop.items VALUES (1, 'Random Random', 'Category', 230, 3, current_timestamp(), current_timestamp(), current_timestamp());
    INSERT INTO shop.purchases VALUES (1, 1, 1, 1, 3, 10, false, current_timestamp(), current_timestamp());
    ``` -->

    ```sql
    CREATE SOURCE purchases
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'purchases')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
    WITH (SIZE = '3xsmall');

    CREATE SOURCE items
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'items')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
    WITH (SIZE = '3xsmall');

    CREATE SOURCE users
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'users')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
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

    A `MATERIALIZED VIEW` is persisted in a durable storage and is incrementally updated as new data arrives.

    ```sql
    CREATE MATERIALIZED VIEW vip_purchases AS
        SELECT
            is_vip,
            SUM(purchase_price) as total_purchase_price
        FROM analytics.dbt_dennis.purchases
        LEFT JOIN analytics.dbt_dennis.users
            ON purchases.user_id = users.id
        WHERE deleted is false
        GROUP BY 1;
    ```

    The above model will update as new purchases come in and if existing purchases are marked as deleted later (soft deletes).

### Subscribe to updates

`SUBSCRIBE` can stream updates from materialized views as they occur. Use it to verify how the analytics change over time.

1. Subscribe to the vip purchases:
    ```sql
    COPY (SUBSCRIBE (SELECT * FROM shop.vip_purchases)) TO STDOUT;
    ```

    As soon as we subscribe we will see the current state of our materialized view.
    ```
    1671724467569	1	f	35
    1671724467569	1	t	25
    ```

    If there is a new purchase or a soft delete, the subscription will show two new rows. The previous value and the new value. In this case there was a soft delete of a non vip purchase of $15.
    ```
    1671724467569	1	f	35
    1671724467569	1	t	25
    1671724514737	1	f	20
    1671724514737	-1	f	35
    ```

1. Press `CTRL + C` to interrupt the subscription after a few changes.

### Connect to BI Tool

Instead of using the subscribe we are will connect our BI tool directly on top of our materialized view. For this example we will use [Metabase](https://www.metabase.com/). Though since Materialize is wire-compatible with PostgreSQL, any tool that supports PostgreSQL should work.

1. In Metabase add a new database connection

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

2. To make a quick dashboard for our mmaterialized view, click "Browse Data" on the left side. Then drill down until you find the `vip_purchases` view. By default it will just be a table but we can change this to a "Bar" chart and get live udpates of new purchases as they are created or deleted.


## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   Connecting a BI tool to a materialized view

## Related pages

-   [`CREATE SOURCE`](/sql/create-source)
-   [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
