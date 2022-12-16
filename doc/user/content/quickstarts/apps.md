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

In this quickstart, you'll build the analytics for an eCommerce sales dashboard that queries Materialize.
At the end of this guide, you'll have the chance to explore an eCommerce data application powered by your work.

The key concepts present in this quickstart will also apply to many other projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)
* [Subscriptions](https://materialize.com/docs/sql/subscribe/)

### Prepare the environment

1. Set up a [Materialize account.](/register)

1. [Install psql and connect](https://materialize.com/docs/get-started/#connect) to Materialize.

### Create the sources

Materialize provides public Kafka topics for its users to use as a source. The Kafka topics contain data from a fictional eCommerce and receive updates every second. You will use the data in them to build the analytics.

1. In your `psql` terminal, create a new schema:

    ```sql
    CREATE SCHEMA shop;
    ```
1. Create the connection:

    ```sql
    CREATE CONNECTION shop.kafka_conn TO KAFKA (BROKER 'localhost:9092');
    ```

1. Create the sources, one per topic:
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
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );

    INSERT INTO shop.users VALUES (1, 'random@email.com', true, current_timestamp(), current_timestamp());
    INSERT INTO shop.items VALUES (1, 'Random Random', 'Category', 230, 3, current_timestamp(), current_timestamp(), current_timestamp());
    INSERT INTO shop.purchases VALUES (1, 1, 1, 1, 3, 10, current_timestamp(), current_timestamp());
    ``` -->

    ```sql
    CREATE SOURCE purchases
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.purchases'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE items
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.items'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;

    CREATE SOURCE users
    FROM KAFKA BROKER 'kafka:9092' TOPIC 'mysql.shop.users'
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
    ENVELOPE DEBEZIUM;
    ```

    <!-- Each topic contains different data:
    * Items: items by category and stock
    * Users: registration and vip access
    * Purchases: item purchases from users -->

<!-- Calculating the same insights over and over again is **inefficient**. -->

### Build the analytics

Materialized views compute and maintain the results of a query incrementally. Use them to build up-to-date analytics results at **lightning speeds**.

Reuse your `psql` session and build the analytics:

1. The sum of purchases:
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

1. The count of users:
    ```sql
    CREATE MATERIALIZED VIEW shop.total_users AS
    SELECT COUNT(1) as total_users
    FROM shop.users;
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

### Subscribe to updates

`SUBSCRIBE` can stream updates from materialized views as they occur. Use it to verify how the analytics change over time.

1. Subscribe to the best sellers items:
    ```sql
    COPY (SUBSCRIBE (SELECT * FROM shop.best_sellers)) TO STDOUT;
    ```

1. Press `CTRL + C` to interrupt the subscription after a few changes.

1. Subscribe to the best sellers items filtering by the `gadgets` category:
    ```sql
    COPY (SUBSCRIBE ( SELECT * FROM shop.best_sellers WHERE category = 'gadgets' )) TO STDOUT;
    ```

1. Press `CTRL + C` **two times** to end the `psql` session.

### Consume from Materialize

Materialize is wire-compatible with PostgreSQL and also supports HTTP requests.

Use the following code snippets and your preferred technology to query the best-selling items:
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

### Explore an actual data application

The materialized views you have running allow multiple services and applications to access the latest analytic results with little effort from Materialize.

Insert your credentials [here](https://materialize-embedded-analytics.vercel.app/) to explore your work powering an eCommerce's analytic dashboard!

## Recap

In this quickstart, you saw:

-   How to define sources and materialized views within Materialize
-   How to subscribe to materialized views
-   Materialize's features to process and serve results for data applications

## Related pages

-   [`CREATE SOURCE`](/sql/create-source)
-   [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/)
-   [`SUBSCRIBE`](/sql/subscribe/)
-   [`HTTP API`](/integrations/http-api/)
