---
title: "Abandoned Cart"
description: "How to build a data pipeline to power customer workflows using Materialize"
menu:
    main:
        parent: quickstarts
weight: 20
draft: true
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

1. Set up a [Materialize account.](/register)

1. Install [`psql`](/integrations/sql-clients/#installation-instructions-for-psql).

1. Open a terminal window and connect to Materialize.

### Create the sources

Materialize provides public Kafka topics and a Confluent Schema Registry for its users. The Kafka topics contain data from a fictional eCommerce store and receive updates every second.

Sources are the first step in most Materialize projects. For this quickstart, you will use our public Kafka topics. They contain data from a fictional eCommerce and receive updates every second.

1. In your `psql` terminal, create the connection to the Confluent Schema Registry:

    ```sql
    CREATE SECRET IF NOT EXISTS csr_username AS '<TBD>';
    CREATE SECRET IF NOT EXISTS csr_password AS '<TBD>';

    CREATE CONNECTION schema_registry
        FOR CONFLUENT SCHEMA REGISTRY
        URL '<TBD>',
        USERNAME = SECRET csr_username,
        PASSWORD = SECRET csr_password;
    ```

1. Create the connection to the Kafka broker:

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

    A `MATERIALIZED VIEW` is persisted in a durable storage and is incrementally updated as new data arrives.

    ```sql
    CREATE MATERIALIZED VIEW inactive_users_last_3_mins AS
        SELECT
            user_id,
            date_trunc('minute', to_timestamp(received_at)) as visited_at_minute
        FROM v_pageviews
        WHERE mz_now() >= (received_at*1000 + 180000)::numeric
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
        JOIN inactive_users_last_3_mins
            ON inactive_users_last_3_mins.user_id = incomplate_purchases.user_id
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

    You can use that real-time data to send an email to the user or send a push notification.

## Send notifications to Telegram

Materialize is wire-compatible with PostgreSQL, so you can use any PostgreSQL client to connect to Materialize. In this example, we'll use the [Node.js](/integrations/node-js) and create a simple script that sends a message to a Telegram user when a user abandons a cart in real time.

### Create a Telegram bot

1. Sign up for a [Telegram account](https://telegram.org/).
1. Next create a bot on Telegram by talking to the `BotFather`.
1. Use the `/newbot` command to create a new bot, and follow the instructions to set a name and username for your bot.
1. The `BotFather` will give you a token that you will use to authenticate your bot. Keep this token safe, as it will allow anyone to control your bot.
1. Create a group on Telegram and add your bot to the group.
1. Get the group's chat ID, which you can obtain by using the `/getUpdates` method of the Bot API:

    ```bash
    curl https://api.telegram.org/bot<token>/getUpdates | jq '.result[0].message.chat.id'
    ```

### Create a Node.js script

1. Create a new directory for your project and install the [`node-telegram-bot-api`](https://www.npmjs.com/package/node-telegram-bot-api) package from `npm`:

    ```bash
    mkdir abandoned-cart-telegram-bot
    cd abandoned-cart-telegram-bot
    npm init -y
    npm install node-telegram-bot-api
    npm install pg
    ```

    The `node-telegram-bot-api` package provides an interface for the Telegram Bot API.

    The `pg` package is the official PostgreSQL client for Node.js which we'll use to connect to Materialize.

1. Here is an example of how you might use the `node-telegram-bot-api` package to send a message to the Telegram group:

    ```javascript
    const TelegramBot = require('node-telegram-bot-api');

    const token = '<YOUR_TELEGRAM_BOT_TOKEN>';
    const chatId = '<YOUR_CHAT_ID>';

    const bot = new TelegramBot(token, {polling: true});

    bot.sendMessage(chatId, 'Hello, world!');

    console.log('Message sent');
    ```

    This code will send a message saying "Hello, world!" to the user or group with the specified chat ID.

    Let's expand this example to send a message to the Telegram group when a user abandons a cart.

1. Next, create a file called `index.js` and add the following code to it:

    ```javascript
    const TelegramBot = require('node-telegram-bot-api');
    const { Client } = require('pg');

    // Define your Telegram bot token and chat ID
    const token = '<YOUR_TELEGRAM_BOT_TOKEN>';
    const chatId = '<YOUR_CHAT_ID>';

    // Define your Materialize connection details
    const client = new Client({
        user: MATERIALIZE_USERNAME,
        password: MATERIALIZE_PASSWORD,
        host: MATERIALIZE_HOST,
        port: 6875,
        database: 'materialize',
        ssl: true
    });

    const bot = new TelegramBot(token, {polling: true});

    async function main() {
        // Connect to Materialize
        await client.connect();

        // Subscribe to the abandoned_cart view
        await client.query('BEGIN');
        await client.query(`
            DECLARE c CURSOR FOR SUBSCRIBE abandoned_cart WITH (SNAPSHOT = false)
        `);

        // Send a message to the Telegram group when a user abandons a cart
        while (true) {
            const res = await client.query('FETCH ALL c');
            if (res.rows.length > 0) {
                bot.sendMessage(chatId, JSON.stringify(res.rows));
            }
            console.log(res.rows);
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

    Each time a user abandons a cart, the script will send a message to the Telegram group.

## Recap

In this quickstart, you saw:

- How to define sources and materialized views within Materialize
- How to use temporal filters
- How to subscribe to materialized views
- Materialize's features to process and serve results for real-time applications
- How to use Materialize with a PostgreSQL client to send notifications to Telegram

As a next step, you can create a [Kafka sink](/sql/create-sink/) to write the results to a Kafka topic so that you can use the results in other applications.

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE VIEW`](/sql/create-view)
- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SUBSCRIBE`](/sql/subscribe)
