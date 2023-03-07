---
title: "Build a reactive workflow"
description: "How to build a data pipeline to power business workflows using Materialize"
menu:
  main:
    parent: 'quickstarts'
    weight: 30
    name: 'Build a reactive workflow'
draft: true
aliases:
  - /demos/abandoned-cart
---

**tl;dr** In this quickstart, you will learn how to use Materialize to send out abandoned cart notifications.
At the end of the guide, you will have a live view with a list of users who have abandoned their carts.

Microservices that work with large data sets often use a job scheduler to process incoming data in batches. These tools can range from basic (e.g. `cron`) to complex.

When an online shopper doesn't make a purchase after adding items to their cart, the likelihood of them completing the purchase decreases over time. Materialize can be used to reduce cart abandonment by sending a notification when a user has not completed their purchase after a certain amount of time.

Materialize allows you to eliminate the need for scheduling and jobs by simply sending data, describing transformations with SQL, and reading the results. Additionally, it can maintain the results of these transformations with sub-second latencies, enabling more agile services.

The key concepts present in this quickstart will also apply to many other real-time projects:

* [Sources](/sql/create-source/load-generator/)
* [Materialized views](/sql/create-materialized-view/)
* [Subscriptions](/sql/subscribe/)
* [Temporal filters](/sql/temporal-filters/)

### Prepare the environment

1. Set up a [Materialize account.](/register)

1. Install [`psql`](/integrations/sql-clients/#installation-instructions-for-psql).

1. Open a terminal window and connect to Materialize.

### Create the sources

Materialize provides public Kafka topics and a Confluent Schema Registry for its users. The Kafka topics contain data from a fictional eCommerce website and receive updates every second. You will use this sample data to build the abandoned cart pipeline.

Sources are the first step in most Materialize projects.

1. In your `psql` terminal, create [a connection](/sql/create-connection/#confluent-schema-registry) to the Confluent Schema Registry:

    ```sql
    CREATE SECRET IF NOT EXISTS csr_username AS '<TBD>';
    CREATE SECRET IF NOT EXISTS csr_password AS '<TBD>';

    CREATE CONNECTION schema_registry
        FOR CONFLUENT SCHEMA REGISTRY
        URL '<TBD>',
        USERNAME = SECRET csr_username,
        PASSWORD = SECRET csr_password;
    ```

1. Create [a connection](/sql/create-connection/#kafka) to the Kafka broker:

    ```sql
    CREATE SECRET kafka_password AS '<TBD>';

    CREATE CONNECTION ecommerce_kafka_connection TO KAFKA (
        BROKER 'TBD',
        SASL MECHANISMS = 'PLAIN',
        SASL USERNAME = 'TBD',
        SASL PASSWORD = SECRET kafka_password
    );
    ```

1. Create the sources:

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

    Now if you run `SHOW SOURCES;`, you should see the four sources we created:

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

With JSON-formatted messages, we don't know the schema so the [JSON is pulled in as raw bytes](/sql/create-source/#json) and we still need to cast data into the proper columns and types.

1. Create a [view](/sql/create-view/) that casts the raw bytes into a JSON object:

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
    CREATE VIEW incomplete_purchases AS
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
1. Create a [materialized view](/sql/create-materialized-view/) to get the last time a user visited the website using [`DISTINCT ON`](/sql/patterns/top-k/#top-1-using-distinct-on).

    A materialized view is persisted in durable storage and is incrementally updated as new data arrives.

    ```sql
    CREATE MATERIALIZED VIEW last_user_visit AS
        SELECT DISTINCT ON(user_id) user_id, received_at
        FROM v_pageviews
        ORDER BY 1, 2 DESC;
    ```

1. Create materialized view to get all the users that have been inactive for the last 3 minutes:

    ```sql
    CREATE MATERIALIZED VIEW inactive_users_last_3_mins AS
        SELECT
            user_id,
            date_trunc('minute', to_timestamp(received_at)) as visited_at_minute
        FROM last_user_visit
        WHERE mz_now() >= (received_at*1000 + 180000)::numeric
        GROUP BY 1,2;
    ```

    The above view uses a [temporal filter](/sql/patterns/temporal-filters/) to only include users who have not visited the website in the last 3 minutes.

1. Create a materialized view to join the incomplete purchases with the inactive users to get the abandoned carts:

    ```sql
    CREATE MATERIALIZED VIEW abandoned_cart AS
        SELECT
            incomplete_purchases.user_id,
            incomplete_purchases.email,
            incomplete_purchases.item_id,
            incomplete_purchases.purchase_price,
            incomplete_purchases.status
        FROM incomplete_purchases
        JOIN inactive_users_last_3_mins
            ON inactive_users_last_3_mins.user_id = incomplete_purchases.user_id
        GROUP BY 1, 2, 3, 4, 5;
    ```

1. To see the changes in the `abandoned_cart` materialized view as new data arrives, you can use [`SUBSCRIBE`](/sql/subscribe):

    ```sql
    SELECT * FROM abandoned_cart LIMIT 10;
    ```

1. To see the changes in the `abandoned_cart` materialized view as new data arrives, you can use [`SUBSCRIBE`](/sql/subscribe):

    ```sql
    COPY ( SUBSCRIBE ( SELECT * FROM abandoned_cart ) ) TO STDOUT;
    ```

    You can use that real-time data to trigger a customer workflow downstream, like sending an email or push notification to the user.

## Trigger a customer workflow

Materialize is wire-compatible with PostgreSQL, so you can use any PostgreSQL client to connect to Materialize. In this example, we'll use [Node.js](/integrations/node-js) and the `node-postgres` library to create a script that connects to Materialize and subscribes to the `abandoned_cart` view.

1. Create a new directory for your project and install the `node-postgres` library:
    ```bash
    mkdir abandoned-cart-demo
    cd abandoned-cart-demo
    npm init -y
    npm install pg
    ```
    The `pg` package is the official PostgreSQL client for Node.js, which we'll use to connect to Materialize.

1. Next, create a file called `index.js` and add the following code to it:

    ```javascript
    const { Client } = require('pg');
    // Define your Materialize connection details
    const client = new Client({
        user: MATERIALIZE_USERNAME,
        password: MATERIALIZE_PASSWORD,
        host: MATERIALIZE_HOST,
        port: 6875,
        database: 'materialize',
        ssl: true
    });
    async function main() {
        // Connect to Materialize
        await client.connect();
        // Subscribe to the abandoned_cart view
        await client.query('BEGIN');
        await client.query(`
            DECLARE c CURSOR FOR SUBSCRIBE abandoned_cart WITH (SNAPSHOT = false)
        `);
        while (true) {
            const res = await client.query('FETCH ALL c');
            if (res.rows.length > 0) {
                console.log(res.rows);
            }
        }
    }
    main();
    ```

    Run the script:
    ```bash
    node index.js
    ```

    Once you run the script, you will see the following output:
    ```javascript
    [
        {
            mz_timestamp: '1671102162899',
            mz_diff: '1',
            user_id: 1,
            email: 'test@test.com',
            item_id: 1,
            purchase_price: 2,
            status: 3
        },
        {
            mz_timestamp: '1671102162899',
            mz_diff: '1',
            user_id: 2,
            email: 'test2@test.com',
            item_id: 2,
            purchase_price: 3,
            status: 3
        }
    ]
    ```

    The script is continuously running and will print the results as they arrive.

    Alternatively, you can create a [Kafka sink](/sql/create-sink/) to write the results to a Kafka topic so that you can use the results in other applications.

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
