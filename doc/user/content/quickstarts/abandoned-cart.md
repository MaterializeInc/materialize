---
title: "Abandoned Cart"
description: "  "
menu:
    main:
        parent: quickstarts
weight: 20
aliases:
  - /demos/abandoned-cart
---

In this quickstart, you will learn how to use Materialize to send out abandoned cart notifications.
At the end of the guide, you will have a live view with a list of users who have abandoned their carts.

The key concepts present in this quickstart will also apply to many other real-time projects:

* [Sources](/sql/create-source/load-generator/)
* [Materialized views](/sql/create-materialized-view/)
* [Subscriptions](/sql/subscribe/)
* [Temporal filters](/sql/temporal-filters/)

### Prepare the environment
[This could be replaced]

1. Set up a [Materialize account.](/register)

1. Install [`psql`](/integrations/sql-clients/#installation-instructions-for-psql)

1. Open a terminal window and connect to Materialize.

### Create the sources

Sources are the first step in most Materialize projects. For this quickstart, you will use our public Kafka topics. They contain data from a fictional eCommerce and receives updates every second.

1. In your `psql` terminal, create the sources:

    ```sql
    CREATE SOURCE purchases
        FROM KAFKA CONNECTION ecommerce_kafka_connection (TOPIC 'mysql.shop.purchases')
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
        ENVELOPE DEBEZIUM
        WITH (SIZE = '3xsmall');

    CREATE SOURCE items
        FROM KAFKA CONNECTION ecommerce_kafka_connection (TOPIC 'mysql.shop.items')
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
        ENVELOPE DEBEZIUM
        WITH (SIZE = '3xsmall');

    CREATE SOURCE users
        FROM KAFKA CONNECTION ecommerce_kafka_connection (TOPIC 'mysql.shop.users')
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION schema_registry
        ENVELOPE DEBEZIUM
        WITH (SIZE = '3xsmall');

    CREATE SOURCE json_pageviews
        FROM KAFKA CONNECTION ecommerce_kafka_connection (TOPIC 'pageviews')
        FORMAT BYTES
        WITH (SIZE = '3xsmall');
    ```

    Now if you run SHOW SOURCES; in the CLI, you should see the four sources we created:

    ```sql
    materialize=> SHOW SOURCES;
        name
    ----------------
    items
    json_pageviews
    purchases
    users
    (4 rows)
    ```

### Create the views

With JSON-formatted messages, we don't know the schema so the [JSON is pulled in as raw bytes](/sql/create-source/#json) and we still need to CAST data into the proper columns and types.

1. [Create a `VIEW`](/sql/create-view/) that casts the raw bytes into a JSON object:

    ```sql
    CREATE VIEW v_pageviews AS
        SELECT
            (data->'user_id')::int AS user_id,
            -- Extract pageview type and target ID from URL
            regexp_match(data->>'url', '/(products|profiles)/')[1] AS pageview_type,
            (regexp_match(data->>'url', '/(?:products|profiles)/(\d+)')[1])::int AS target_id,
            data->>'channel' AS channel,
            (data->>'received_at')::bigint AS received_at
        FROM (SELECT CONVERT_FROM(data, 'utf8')::jsonb AS data FROM json_pageviews);
    ```

1. Define a view containing the incomplete orders:

    ```sql
    CREATE VIEW incomplate_purchases AS
        SELECT
            users.id AS user_id,
            users.email AS email,
            users.is_vip AS is_vip,
            purchases.item_id,
            purchases.status,
            purchases.quantity,
            purchases.purchase_price,
            purchases.created_at,
            purchases.updated_at
        FROM users
        JOIN purchases ON purchases.user_id = users.id
        WHERE purchases.status = 0;
    ```

1. Create a [`MATERIALIZED VIEW`](/sql/create-materialized-view/) to get all the users that are no longer browsing the site.

    A `MATERIALIZED VIEW` is persisted in durable storage and incrementally updated as new data arrives.

    ```sql
    CREATE MATERIALIZED VIEW inactive_users_last_3_mins AS
        SELECT
            user_id,
            date_trunc('minute', to_timestamp(received_at)) as visited_at_minute
        FROM v_pageviews
        WHERE
        mz_now() >= (received_at*1000 + 180000)::numeric
        GROUP BY 1,2;
    ```

    The above view uses a [temporal filter](/sql/patterns/temporal-filters/) to only include users who have not visited the site in the last 3 minutes.

1. Create a materialized view to join the incomplete purchases with the inactive users to get the abandoned carts:

    ```sql
    CREATE MATERIALIZED VIEW abandoned_cart AS
        SELECT
            incomplate_purchases.user_id,
            incomplate_purchases.email,
            incomplate_purchases.item_id,
            incomplate_purchases.purchase_price,
            incomplate_purchases.status
        FROM incomplate_purchases
        JOIN inactive_users_last_3_mins ON inactive_users_last_3_mins.user_id = incomplate_purchases.user_id
        GROUP BY 1, 2, 3, 4, 5;
    ```

1. Select from the `abandoned_cart` view to see the results:

    ```sql
    SELECT * FROM abandoned_cart LIMIT 10;
    ```

1. Use [`SUBSCRIBE`](/sql/subscribe) to the `abandoned_cart` view to see the results in real-time:

    ```sql
    COPY ( SUBSCRIBE ( SELECT * FROM abandoned_cart ) ) TO STDOUT;
    ```

    You can use that real-time data to send an email to the user or send a push notifications.

## Recap

In this quickstart, you saw:

- How to define sources and materialized views within Materialize
- How to use temporal filters
- How to subscribe to materialized views
- Materialize's features to process and serve results for real-time applications

As a next step, you can create a [Kafka sink](/sql/create-sink/) to write the results to a Kafka topic so that you can use the results in other applications.

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SUBSCRIBE`](/sql/subscribe)
