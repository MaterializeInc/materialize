---
title: "Data applications"
description: "  "
menu:
    main:
        parent: quickstarts
weight: 20
aliases:
  - /demos/data-applications
---

<!--
* What you teach the child to cook isn’t really important. What’s important is that the child finds it enjoyable, and gains confidence, and wants to do it again.
* Through the things the child does, it will learn important things about cooking. It will learn what it is like to be in the kitchen, to use the utensils, to handle the food.
* This is because using software, like cooking, is a matter of craft. It’s knowledge - but it is practical knowledge, not theoretical knowledge.
* When we learn a new craft or skill, we always begin learning it by doing.
-->

In this quickstart, you will learn how to use Materialize to power data applications.
At the end of the guide, you will have the chance to try your code and prove your skills.

The key concepts present in this quickstart apply to many other real-time projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)
* [Subscriptions](https://materialize.com/docs/sql/subscribe/)

### Prepare the environment
[This could be replaced]

1. Set up a [Materialize account.](/register)

1. Install [psql](https://materialize.com/docs/integrations/sql-clients/#installation-instructions-for-psql)

1. Open a terminal window and connect to Materialize.

### Create the sources

Sources are the first step in most of the Materialize projects. For this quickstart, you will use our public Kafka topics. They contain data from a fictional eCommerce and receives new data every second.

1. In your `psql` terminal, create the sources:

    ```sql
    CREATE SCHEMA shop;
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
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    INSERT INTO shop.users VALUES (1, 'random@email.com', true, current_timestamp(), current_timestamp());
    INSERT INTO shop.items VALUES (1, 'Random Random', 'Category', 230, 3, current_timestamp(), current_timestamp(), current_timestamp());
    INSERT INTO shop.purchases VALUES (1, 1, 1, 1, 3, 10, current_timestamp(), current_timestamp());
    ```

    <!-- ```sql
    CREATE SOURCE purchases
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.purchases'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE itemsxw
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.items'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE users
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.users'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;
    ``` -->

    <!-- Each topic contains different data:
    * Items: items by category and stock
    * Users: registration and vip access
    * Purchases: item purchases from users -->

### Build the insights

Analytics are expensive. Calculating the same insight over and over again is **inefficient**. Materialized views maintain and compute a query's result fast, **lightning fast**. It turns swift and inexpensive for the data application to stay up-to-date.

Reuse your `psql` session and set up multiple eCommerce insights:

1. Calculate the total purchases:
    ```sql
    CREATE MATERIALIZED VIEW shop.total_purchases AS
    SELECT SUM(purchase_price * quantity) AS total_purchases
    FROM shop.purchases;
    ```

 1. Count the number of purchases:
    ```sql
    CREATE MATERIALIZED VIEW shop.count_purchases AS
    SELECT COUNT(1) AS count_purchases
    FROM shop.purchases;
    ```

1. Investigate the best sellers:
    ```sql
    CREATE MATERIALIZED VIEW shop.best_sellers AS
    SELECT I.name, I.category, COUNT(1) as purchases
    FROM shop.purchases P
    JOIN shop.items I ON (P.item_id = I.id)
    GROUP BY I.name, I.category
    ORDER BY purchases DESC
    LIMIT 10;
    ```

1. Follow the total users:
    ```sql
    CREATE MATERIALIZED VIEW shop.total_users AS
    SELECT COUNT(1) as total_users
    FROM shop.users;
    ```

### Subscribe to updates

Tracking and reacting over each update for a query's result using traditional databases is complex and expensive. In Materialize, use `SUBSCRIBE` to follow a query's result changes over time:

1. Subscribe to changes in the top 3 best sellers:
    ```sql
    COPY (SUBSCRIBE ( SELECT * FROM shop.best_sellers ) WITH (SNAPSHOT = false)) TO STDOUT;
    ```

1. Press `CTRL + C` to interrupt the subscription once you are done.

### Consume from Materialize

Materialize is wire-compatible with PostgreSQL and also supports HTTP requests. It is a flexible access point to consume from most services, lambdas or even edge functions.

As an example and to prove your skills, [insert your credentials in here](https://materialize-embedded-analytics.vercel.app/) and experience your work!

## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   Materialize's features to process and serve results for real-time applications

## Related pages

-   [`CREATE SOURCE`](/sql/create-source)
