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

In this quickstart, you will learn how to use Materialize to build data applications.
At the end of the guide, you will have the chance to use your code to power an existent data application.

The key concepts present in this quickstart will also apply to many other real-time projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)
* [Subscriptions](https://materialize.com/docs/sql/subscribe/)

### Prepare the environment
[This could be replaced]

1. Set up a [Materialize account.](/register)

1. Install [psql](https://materialize.com/docs/integrations/sql-clients/#installation-instructions-for-psql)

1. Open a terminal window and connect to Materialize.

### Create the sources

Sources are the first step in most Materialize projects. For this quickstart, you will use our public Kafka topics. They contain data from a fictional eCommerce and receives updates every second.

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

<!-- Calculating the same insights over and over again is **inefficient**. -->

### Build the insights

Use materialized views to compute and maintain insights up-to-date at **lightning speeds**.

Reuse your `psql` session and build the eCommerce insights:

1. The total amount of income:
    ```sql
    CREATE MATERIALIZED VIEW shop.total_purchases AS
    SELECT SUM(purchase_price * quantity) AS total_purchases
    FROM shop.purchases;
    ```

 1. The count of purchases:
    ```sql
    CREATE MATERIALIZED VIEW shop.count_purchases AS
    SELECT COUNT(1) AS count_purchases
    FROM shop.purchases;
    ```

1. The best sellers items:
    ```sql
    CREATE MATERIALIZED VIEW shop.best_sellers AS
    SELECT I.name, I.category, COUNT(1) as purchases
    FROM shop.purchases P
    JOIN shop.items I ON (P.item_id = I.id)
    GROUP BY I.name, I.category
    ORDER BY purchases DESC
    LIMIT 10;
    ```

1. The count of users:
    ```sql
    CREATE MATERIALIZED VIEW shop.total_users AS
    SELECT COUNT(1) as total_users
    FROM shop.users;
    ```

### Subscribe to updates

Use `SUBSCRIBE` and check how the query's result changes over time:

1. Subscribe to the best sellers items:
    ```sql
    COPY (SUBSCRIBE ( SELECT * FROM shop.best_sellers )) TO STDOUT;
    ```

1. Press `CTRL + C` to interrupt the subscription once you are done.

1. Subscribe to the best sellers items filtering by the `gadgets` category:
    ```sql
    COPY (SUBSCRIBE ( SELECT * FROM shop.best_sellers WHERE category = 'gadgets' )) TO STDOUT;
    ```

1. End the `psql` session.

### Consume from Materialize

Materialize is wire-compatible with PostgreSQL and also supports HTTP requests. It is a flexible access point to consume from services, lambdas or edge functions.

Use the following code snippets to query your insight:
{{< tabs tabID="1" >}}

{{< tab "Curl">}}

```bash
curl 'https://<MZ host address>/api/sql' \
    --header 'Content-Type: application/json' \
    --user '<username>:<passsword>' \
    --data '{
        "query": "SELECT * FROM shop.best_sellers;"
    }'
```

{{< /tab >}}

{{< tab "Go">}}
```go
package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"log"
)

func main() {

	ctx := context.Background()
	connStr := "postgres://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD@MATERIALIZE_HOST:6875/materialize"

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
}
```

{{< /tab >}}
{{< tab "Java">}}

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, props);
            System.out.println("Connected to Materialize successfully!");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public static void main(String[] args) {
        App app = new App();
        app.connect();
    }
}
```

{{< /tab >}}
{{< tab "Node.js">}}

```javascript
const { Client } = require('pg');
const client = new Client({
    user: MATERIALIZE_USERNAME,
    password: MATERIALIZE_PASSWORD,
    host: MATERIALIZE_HOST,
    port: 6875,
    database: 'materialize',
    ssl: true
});

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

{{< /tab >}}
{{< tab "PHP">}}

```php
<?php

function connect(string $host, int $port, string $db, string $user, string $password): PDO
{
    try {
        $dsn = "pgsql:host=$host;port=$port;dbname=$db;";

        // make a database connection
        return new PDO(
            $dsn,
            $user,
            $password,
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
        );
    } catch (PDOException $e) {
        die($e->getMessage());
    }
}

$connection = connect('MATERIALIZE_HOST', 6875, 'materialize', 'MATERIALIZE_USERNAME', 'MATERIALIZE_PASSWORD');
```

{{< /tab >}}
{{< tab "Python">}}

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
```

{{< /tab >}}

{{< tab "Ruby">}}

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")
```

{{< /tab >}}
{{< /tabs >}}

Assert your skills by [inserting your credentials here](https://materialize-embedded-analytics.vercel.app/) and experience your work!

## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   Materialize's features to process and serve results for real-time applications

## Related pages

-   [`CREATE SOURCE`](/sql/create-source)
