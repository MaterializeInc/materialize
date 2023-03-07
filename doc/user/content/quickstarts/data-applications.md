---
title: "Build an interactive data app"
description: "How to build interactive data applications using Materialize"
menu:
  main:
    parent: 'quickstarts'
    weight: 40
    name: 'Build an interactive data app'
draft: true
aliases:
  - /demos/data-applications
---

In this quickstart, you'll build the analytics for an eCommerce sales dashboard that queries Materialize.
At the end of this guide, you'll have the chance to explore an eCommerce data application querying against your Materialize region.

The key concepts present in this quickstart will also apply to many other projects:

* [Sources](https://materialize.com/docs/sql/create-source/load-generator/)
* [Materialized views](https://materialize.com/docs/sql/create-materialized-view/)
* [Subscriptions](https://materialize.com/docs/sql/subscribe/)

### Prepare the environment

1. Set up a [Materialize account.](/register)

1. [Install psql and connect](https://materialize.com/docs/get-started/#connect) to Materialize.

### Create the sources

Materialize provides public Kafka topics and a Confluent Schema Registry for its users. The Kafka topics contain data from a fictional eCommerce website and receive updates every second. You will use this sample data to feed an embedded analytics dashboard.

1. In your `psql` terminal, create a new schema:

    ```sql
    CREATE SCHEMA shop;
    ```

1. Create [a connection](/sql/create-connection/#confluent-schema-registry) to the Confluent Schema Registry:
    ```sql
    CREATE SECRET IF NOT EXISTS shop.csr_username AS '<TBD>';
    CREATE SECRET IF NOT EXISTS shop.csr_password AS '<TBD>';

    CREATE CONNECTION shop.csr_basic_http
    FOR CONFLUENT SCHEMA REGISTRY
    URL '<TBD>',
    USERNAME = SECRET csr_username,
    PASSWORD = SECRET csr_password;
    ```

1. Create [a connection](/sql/create-connection/#kafka) to the Kafka broker:

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

    ```sql
    CREATE SOURCE purchases
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'purchases')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');

    CREATE SOURCE items
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'items')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');

    CREATE SOURCE users
    FROM KAFKA CONNECTION shop.kafka_connection (TOPIC 'users')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION shop.csr_basic_http
    ENVELOPE DEBEZIUM
    WITH (SIZE = '3xsmall');
    ```

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
	"fmt"

	"github.com/jackc/pgx/v4"
)

func main() {

	ctx := context.Background()
	connStr := "postgres://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD@MATERIALIZE_HOST:6875/materialize"

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Connected to Materialize!")
	}

	rows, err := conn.Query(ctx, "SELECT * FROM shop.best_sellers")
	if err != nil {
		fmt.Println(err)
	}

	type result struct {
		Category string
		Name string
        Purchases int
	}

	for rows.Next() {
		var r result
		err = rows.Scan(&r.Category, &r.Name, &r.Purchases)
		if err != nil {
			fmt.Println(err)
		}
		// operate on result
		fmt.Printf("%s %s %s\n", r.Category, r.Name, r.Purchases)
	}

	defer conn.Close(context.Background())
}
```

{{< /tab >}}
{{< tab "Java">}}

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;

class App {

    private final String url = "jdbc:postgresql://4eylxydzhfj2ef3sblalf5h32.us-east-1.aws.materialize.cloud:6875/materialize?sslmode=require";
    private final String user = "joaquin@materialize.com";
    private final String password = "mzp_80ed506798cd45b0b6637681dd9d90df97539a9c374a4245a4221b8796e88e2a";

    /**
     * Connect to Materialize
     *
     * @return a Connection object
     */
    public Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("ssl","true");

        return DriverManager.getConnection(url, props);

    }

    public void query() {

        String SQL = "SELECT * FROM shop.best_sellers";

        try (Connection conn = connect();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL)) {
            while (rs.next()) {
                System.out.println("Name: " + rs.getString("name"));
                System.out.println("Category: " + rs.getString("category"));
                System.out.println("Purchases: " + rs.getString("purchases"));
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
}

class Main {
  public static void main(String[] args) {
    App app = new App();
    app.query();
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
  const res = await client.query('SELECT * FROM shop.best_sellers');
  console.log(res.rows);
};

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

$sql = 'SELECT * FROM shop.best_sellers';
$statement = $connection->query($sql);

while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```

{{< /tab >}}
{{< tab "Python">}}

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM shop.best_sellers;")
    for row in cur:
        print(row)
```

{{< /tab >}}

{{< tab "Ruby">}}

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

res  = conn.exec('SELECT * FROM shop.best_sellers;')

res.each do |row|
  puts row
end
```

{{< /tab >}}
{{< /tabs >}}

### Explore an actual data application

The materialized views you have running allow multiple services and applications to access the latest analytic results with little effort from Materialize.

Authenticate using your credentials [here](https://materialize-embedded-analytics.vercel.app/) to explore your work powering an eCommerce's analytic dashboard!

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
