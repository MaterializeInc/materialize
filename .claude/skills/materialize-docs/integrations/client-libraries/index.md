---
audience: developer
canonical_url: https://materialize.com/docs/integrations/client-libraries/
complexity: advanced
description: Connect via client libraries/SQL drivers
doc_type: reference
keywords:
- create relations
- not guaranteed
- stream out results
- CREATE A
- CREATE TABLES
- 'execute

  queries'
- CREATE TABLE
- CREATE RELATIONS
- Client libraries
- 'Note:'
product_area: General
status: stable
title: Client libraries
---

# Client libraries

## Purpose
Connect via client libraries/SQL drivers

If you need to understand the syntax and options for this command, you're in the right place.


Connect via client libraries/SQL drivers



Applications can use various common language-specific PostgreSQL client
libraries to interact with Materialize and **create relations**, **execute
queries** and **stream out results**.

> **Note:** 
Client libraries tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if PostgreSQL is supported, it's **not guaranteed** that the same integration will work out-of-the-box.


- **Go**: [`pgx`](https://github.com/jackc/pgx) | See the [Go cheatsheet](/integrations/client-libraries/golang/).
- **Java**: [PostgreSQL JDBC driver](https://jdbc.postgresql.org/) | See the [Java cheatsheet](/integrations/client-libraries/java-jdbc/).
- **Node.js**: [`node-postgres`](https://node-postgres.com/) | See the [Node.js cheatsheet](/integrations/client-libraries/node-js/).
- **PHP**: [`pdo_pgsql`](https://www.php.net/manual/en/ref.pgsql.php) | See the [PHP cheatsheet](/integrations/client-libraries/php/).
- **Python**: [`psycopg2`](https://pypi.org/project/psycopg2/) | See the [Python cheatsheet](/integrations/client-libraries/python/).
- **Ruby**: [`pg` gem](https://rubygems.org/gems/pg/) | See the [Ruby cheatsheet](/integrations/client-libraries/ruby/).
- **Rust**: [`postgres-openssl`](https://crates.io/crates/postgres-openssl) | See the [Rust cheatsheet](/integrations/client-libraries/rust/).

👋 _Is there another client library you'd like to use with Materialize? Submit a
[feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._




---

## Golang cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Go applications can use the standard library's [`database/sql`](https://pkg.go.dev/database/sql) package with a PostgreSQL driver to interact with Materialize. In this guide, we'll use the [`pgx` driver](https://github.com/jackc/pgx) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://pkg.go.dev/github.com/jackc/pgx#ConnConfig) to Materialize using `pgx`, you can use either a URI or DSN [connection string](https://pkg.go.dev/github.com/jackc/pgx#ParseConnectionString):

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
```text

To create a concurrency-safe connection pool, import the [`pgxpool` package](https://pkg.go.dev/github.com/jackc/pgx/v4/pgxpool) and use `pgxpool.Connect`.

The remainder of this guide uses the [`*pgx.Conn`](https://pkg.go.dev/github.com/jackc/pgx#Conn) connection handle from the [connect](#connect) section to interact with Materialize.

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```go
createTableSQL := `
    CREATE TABLE IF NOT EXISTS countries (
        code CHAR(2),
        name TEXT
    );
`

_, err := conn.Exec(ctx, createTableSQL)
if err != nil {
    log.Fatal(err)
}
```bash

## Insert data into tables

To [insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```go
insertSQL := "INSERT INTO countries (code, name) VALUES ($1, $2)"

_, err := conn.Exec(ctx, insertSQL, "GH", "GHANA")
if err != nil {
    log.Fatal(err)
}
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: Go executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` using a `SELECT` statement:

```go
rows, err := conn.Query(ctx, "SELECT * FROM countries")
if err != nil {
    log.Fatal(err)
}

for rows.Next() {
    var r result
    err = rows.Scan(&r...)
    if err != nil {
        log.Fatal(err)
    }
    // operate on result
}
```bash

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's possible to use a Go app to execute common DDL statements.

### Create a source from Go

```go
createSourceSQL := `
    CREATE SOURCE IF NOT EXISTS auction
    FROM LOAD GENERATOR AUCTION FOR ALL TABLES;
`

_, err = conn.Exec(ctx, createSourceSQL)
if err != nil {
    log.Fatal(err)
}
```text
For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Go

```go
createViewSQL := `
    CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS
        SELECT sum(amount)
    FROM bids;
`

_, err = conn.Exec(ctx, createViewSQL)
if err != nil {
    log.Fatal(err)
}
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Go application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```go
tx, err := conn.Begin(ctx)
if err != nil {
    log.Fatal(err)
    return
}
defer tx.Rollback(ctx)

_, err = tx.Exec(ctx, "DECLARE c CURSOR FOR SUBSCRIBE amount_sum")
if err != nil {
    log.Fatal(err)
    return
}

for {
    rows, err := tx.Query(ctx, "FETCH ALL c")
    if err != nil {
        log.Fatal(err)
        tx.Rollback(ctx)
        return
    }

    for rows.Next() {
        var r subscribeResult
        if err := rows.Scan(&r.MzTimestamp, &r.MzDiff, ...); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("%+v\n", r)
        // operate on subscribeResult
    }
}

err = tx.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
```text

The [SUBSCRIBE output format](/sql/subscribe/#output) of `subscribeResult` contains all of the columns of `amount_sum`, prepended with several additional columns that describe the nature of the update.  When a row of a subscribed view is **updated,** two objects will show up in the result set:

```go
{MzTimestamp:1646868332570 MzDiff:1 row...}
{MzTimestamp:1646868332570 MzDiff:-1 row...}
```text

An `MzDiff` value of `-1` indicates that Materialize is deleting one row with the included values. An update is just a retraction (`MzDiff:-1`) and an insertion (`MzDiff:1`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## Go ORMs

ORM frameworks like **GORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.




---

## Java cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Java
applications can use common PostgreSQL clients to interact with Materialize.
This guide uses the [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/) to
connect to Materialize and issue SQL commands.

## Connect

To [connect](https://jdbc.postgresql.org/documentation/head/connect.html) to Materialize using the PostgreSQL JDBC Driver:

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
```text

To establish the connection to Materialize, call the `getConnection()` method on the `DriverManager` class.

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

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

    public void createTable() {

        String SQL = "CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT)";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("Table created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.createTable();
    }

}
```bash

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

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

    public void insert() {

        try (Connection conn = connect()) {
            String code = "GH";
            String name = "Ghana";
            PreparedStatement st = conn.prepareStatement("INSERT INTO countries(code, name) VALUES(?, ?)");
            st.setString(1, code);
            st.setString(2, name);
            int rowsDeleted = st.executeUpdate();
            System.out.println(rowsDeleted + " rows inserted.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.insert();
    }
}
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: Java executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` using a `SELECT` statement:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

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

        String SQL = "SELECT * FROM countries";

        try (Connection conn = connect();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SQL)) {
            while (rs.next()) {
                System.out.println(rs.getString("code") + " " + rs.getString("name"));
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.query();
    }
}
```text

For more details, see the [JDBC](https://jdbc.postgresql.org/documentation/head/query.html) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Java app to execute common DDL statements.

### Create a source from Java

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

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

    public void source() {

        String SQL = "CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("Source created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.source();
    }
}
```text

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Java

```java
    public void view() {

        String SQL = "CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS "
                   + "SELECT sum(amount)"
                   + "FROM bids;";

        try (Connection conn = connect()) {
            Statement st = conn.createStatement();
            st.execute(SQL);
            System.out.println("View created.");
            st.close();
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Java application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {

    private final String url = "jdbc:postgresql://MATERIALIZE_HOST:6875/materialize";
    private final String user = "MATERIALIZE_USERNAME";
    private final String password = "MATERIALIZE_PASSWORD";

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

    public void subscribe() {
        try (Connection conn = connect()) {

            Statement stmt = conn.createStatement();
            stmt.execute("BEGIN");
            stmt.execute("DECLARE c CURSOR FOR SUBSCRIBE amount_sum");
            while (true) {
                ResultSet rs = stmt.executeQuery("FETCH ALL c");
                if(rs.next()) {
                    System.out.println(rs.getString(1) + " " + rs.getString(2));
                }
            }
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void main(String[] args) {
        App app = new App();
        app.subscribe();
    }
}
```text

The [`SUBSCRIBE` output format](/sql/subscribe/#output) of `rs` is a `ResultSet` of view updates. When a row of a subscribed view is **updated,** two objects will show up in the `rows` array:

```java
    ...
    1648567756801 1 value_3
    1648567761801 1 value_4
    1648567785802 -1 value_4
    ...
```text

A `mz_diff` value of `-1` indicates that Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## Java ORMs

ORM frameworks like **Hibernate** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.




---

## Node.js cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Node.js applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the  [`node-postgres` library](https://node-postgres.com/) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://node-postgres.com/features/connecting) to Materialize using `node-postgres`, you can use the connection URI shorthand (`postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`):

```js
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
```bash

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```js
const { Client } = require('pg');

const client = new Client({
    user: MATERIALIZE_USERNAME,
    password: MATERIALIZE_PASSWORD,
    host: MATERIALIZE_HOST,
    port: 6875,
    database: 'materialize',
    ssl: true
});

const createTableSQL = `
    CREATE TABLE IF NOT EXISTS countries (
        code CHAR(2),
        name TEXT
    );
`;

async function main() {
    await client.connect();
    const res = await client.query(createTableSQL);
    console.log(res);
}

main();
```bash

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```js
const { Client } = require('pg');

const client = new Client({
    user: MATERIALIZE_USERNAME,
    password: MATERIALIZE_PASSWORD,
    host: MATERIALIZE_HOST,
    port: 6875,
    database: 'materialize',
    ssl: true
});

const text = 'INSERT INTO countries(code, name) VALUES($1, $2);';
const values = ['GH', 'GHANA'];

async function main() {
    await client.connect();
    const res = await client.query(text, values);
    console.log(res);
}

main();
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: Node.js executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table:

```js
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
  const res = await client.query('SELECT * FROM countries');
  console.log(res.rows);
};

main();
```text

For more details, see the [`node-postgres` query](https://node-postgres.com/features/queries) and [pg.Result](https://node-postgres.com/api/result) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's also possible to use a Node.js app to execute common DDL statements.

### Create a source from Node.js

```js
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
    const res = await client.query(
        `CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;`
        );
    console.log(res);
}

main();
```text

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Node.js

```js
const { Client } = require('pg');

const client = new Client({
    user: MATERIALIZE_USERNAME,
    password: MATERIALIZE_PASSWORD,
    host: MATERIALIZE_HOST,
    port: 6875,
    database: "materialize",
    ssl: true,
});

async function main() {
    await client.connect();
    const res = await client.query(
        `CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS
            SELECT sum(amount)
        FROM bids;`
        );
    console.log(res);
}

main();
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Node.js application, instead of [querying](#query) Materialize for the state of a view at a point in time, you can use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```js
const { Client } = require('pg');

async function main() {
  const client = new Client({
    user: MATERIALIZE_USERNAME,
    password: MATERIALIZE_PASSWORD,
    host: MATERIALIZE_HOST,
    port: 6875,
    database: "materialize",
    ssl: true,
  });
  await client.connect();

  await client.query('BEGIN');
  await client.query('DECLARE c CURSOR FOR SUBSCRIBE amount_sum WITH (SNAPSHOT = FALSE)');

  while (true) {
    const res = await client.query('FETCH ALL c');
    console.log(res.rows);
  }
}

main();
```text

The [`SUBSCRIBE` output format](/sql/subscribe/#output) of `res.rows` is an array of view update objects. When a row of a subscribed view is **updated,** two objects will show up in the `rows` array:

```js
[
    ...
    {
        mz_timestamp: '1627225629000',
        mz_diff: '1',
        sum: 'value_1',
    },
    {
        mz_timestamp: '1627225629000',
        mz_diff: '-1',
        sum: 'value_2',
    },
    ...
]
```text

An `mz_diff` value of `-1` indicates that Materialize is deleting one row with the included values. An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

<!--- NOT PUBLISHABLE UNTIL https://github.com/brianc/node-postgres/pull/2573 is merged
### Using pg-query-stream

```js
const { Client } = require('pg');
const QueryStream = require('pg-query-stream');

const client = new Client('postgres://materialize@localhost:6875/materialize');

client.connect((err, client) => {
  if (err) {
    throw err;
  }
  const stream = client.query(new QueryStream('SUBSCRIBE avg_bid', []));
  stream.pipe(process.stdout);
});
```text
--->

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## Node.js ORMs

ORM frameworks like **Prisma**, **Sequelize**, or **TypeORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.




---

## PHP cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that PHP applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [PDO_PGSQL driver](https://www.php.net/manual/en/ref.pdo-pgsql.php) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.php.net/manual/en/ref.pdo-pgsql.connection.php) to Materialize using `PDO_PGSQL`:

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
```text

You can add the above code to a `config.php` file and then include it in your application with `require 'connect.php';`.

## Create tables

Most data in Materialize will stream in via an external system, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'CREATE TABLE IF NOT EXISTS countries (
    code CHAR(2),
    name TEXT
)';

$statement = $connection->prepare($sql);
$statement->execute();
```bash

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into the `countries` table in Materialize.

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'INSERT INTO countries (name, code) VALUES (?, ?)';
$statement = $connection->prepare($sql);
$statement->execute(['United States', 'US']);
$statement->execute(['Canada', 'CA']);
$statement->execute(['Mexico', 'MX']);
$statement->execute(['Germany', 'DE']);

$countStmt = "SELECT COUNT(*) FROM countries";
$count = $connection->query($countStmt);
while (($row = $count->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: PHP executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = 'SELECT * FROM countries';
$statement = $connection->query($sql);

while (($row = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
    var_dump($row);
}
```text

For more details, see the [PHP `PDOStatement`](https://www.php.net/manual/en/pdostatement.fetch.php) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a PHP app to execute common DDL statements.

### Create a source from PHP

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = "CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;";

$statement = $connection->prepare($sql);
$statement->execute();

$sources = "SHOW SOURCES";
$statement = $connection->query($sources);
$result = $statement->fetchAll(PDO::FETCH_ASSOC);
var_dump($result);

```text

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from PHP

```php
<?php
// Include the Postgres connection details
require 'connect.php';

$sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS SELECT SUM(amount) FROM bids;";

$statement = $connection->prepare($sql);
$statement->execute();

$views = "SHOW MATERIALIZED VIEWS";
$statement = $connection->query($views);
$result = $statement->fetchAll(PDO::FETCH_ASSOC);
var_dump($result);
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a PHP application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```php
<?php
// Include the Postgres connection details
require 'connect.php';

// Begin a transaction
$connection->beginTransaction();
// Declare a cursor
$statement = $connection->prepare('DECLARE c CURSOR FOR SUBSCRIBE amount_sum WITH (FETCH = true);');
// Execute the statement
$statement->execute();

/* Fetch all of the remaining rows in the result set */
while (true) {
    //$result = $statement->fetchAll();
    $subscribe = $connection->prepare('FETCH ALL c');
    $subscribe->execute();
    $result = $subscribe->fetchAll(PDO::FETCH_ASSOC);
    print_r($result);
}
```text

The [SUBSCRIBE output format](/sql/subscribe/#output) of `result` is an array of view updates objects. When a row of a subscribed view is **updated,** two objects will show up in the `result` array:

```php
    ...
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646310999683
                    [mz_diff] => 1
                    [sum] => 'value_1'
                )

        )
        Array
        (
            [0] => Array
                (
                    [mz_timestamp] => 1646311002682
                    [mz_diff] => -1
                    [sum] => 'value_2'
                )

        )
    ...
```text

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS ammount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## PHP ORMs

ORM frameworks like **Eloquent** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.




---

## Python cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Python applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [`psycopg2`](https://pypi.org/project/psycopg2/) adapter to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://www.psycopg.org/docs/usage.html) to a local Materialize instance using `psycopg2`:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
```bash

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT);")

with conn.cursor() as cur:
    cur.execute("SHOW TABLES")
    print(cur.fetchone())
```bash

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

cur = conn.cursor()
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('United States', 'US'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Canada', 'CA'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Mexico', 'MX'))
cur.execute("INSERT INTO countries (name, code) VALUES (%s, %s)", ('Germany', 'DE'))
conn.commit()
cur.close()

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM countries;")
    print(cur.fetchone())

conn.close()
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: Python executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("SELECT * FROM countries;")
    for row in cur:
        print(row)
```text

For more details, see the [Psycopg](https://www.psycopg.org/docs/usage.html) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Python app to execute common DDL statements.

### Create a source from Python

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("CREATE SOURCE auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;")

with conn.cursor() as cur:
    cur.execute("SHOW SOURCES")
    print(cur.fetchone())
```text

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Python

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)
conn.autocommit = True

with conn.cursor() as cur:
    cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS " \
            "SELECT sum(amount)" \
            "FROM bids;")

with conn.cursor() as cur:
    cur.execute("SHOW VIEWS")
    print(cur.fetchone())
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).

## Stream

To take full advantage of incrementally updated materialized views from a Python application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```python
#!/usr/bin/env python3

import psycopg2
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg2.connect(dsn)

with conn.cursor() as cur:
    cur.execute("DECLARE c CURSOR FOR SUBSCRIBE amount_sum")
    while True:
        cur.execute("FETCH ALL c")
        for row in cur:
            print(row)
```text

The [SUBSCRIBE output format](/sql/subscribe/#output) of `cur` is a data access object that can be used to iterate over the set of rows. When a row of a subscribed view is **updated,** two objects will show up in the `rows` array:

```python
    ...
(Decimal('1648737001490'), 1, 'my_value_1')
(Decimal('1648737001490'), 1, 'my_value_2')
(Decimal('1648737001490'), 1, 'my_value_3')
(Decimal('1648737065479'), -1, 'my_value_3')
(Decimal('1648737065479'), 1, 'my_value_4')
    ...
```text

A `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same timestamp.

### Streaming with `psycopg3`

Although `psycopg3` can function identically as the `psycopg2` example above,
it provides a `stream` feature where rows are not buffered, which allows you to use `SUBSCRIBE` directly:

```python
#!/usr/bin/env python3

import psycopg
import sys

dsn = "user=MATERIALIZE_USERNAME password=MATERIALIZE_PASSWORD host=MATERIALIZE_HOST port=6875 dbname=materialize sslmode=require"
conn = psycopg.connect(dsn)

with conn.cursor() as cur:
    for row in cur.stream("SUBSCRIBE amount_sum"):
        print(row)
```bash

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## Python ORMs

ORM frameworks tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.

Check out the [integrations page](/integrations/) for a list of ORM frameworks
that are known to work well with Materialize.




---

## Ruby cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Ruby applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the  [`pg` gem](https://rubygems.org/gems/pg/) to connect to Materialize and issue SQL commands.

## Connect

To connect to Materialize using `pg`:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")
```text

If you don't have a `pg` gem, you can install it with:

```bash
gem install pg
```bash

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

conn.exec("CREATE TABLE IF NOT EXISTS countries (code CHAR(2), name TEXT);")

res  = conn.exec('SHOW TABLES')

res.each do |row|
  puts row
end
```bash

## Insert data into tables

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

conn.exec("INSERT INTO countries (code, name) VALUES ('US', 'United States');")
conn.exec("INSERT INTO countries (code, name) VALUES ('CA', 'Canada');")

res  = conn.exec('SELECT * FROM countries')

res.each do |row|
  puts row
end
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database: Ruby executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize keeps results incrementally updated, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query the `countries` table using a `SELECT` statement:

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

res  = conn.exec('SELECT * FROM countries')

res.each do |row|
  puts row
end
```text

For more details, see the [`exec` instance method](https://rubydoc.info/gems/pg/0.10.0/PGconn#exec-instance_method) documentation.

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's also possible to use a Ruby app to execute common DDL statements.

### Create a source from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

# Create a source
src = conn.exec(
    "CREATE SOURCE IF NOT EXISTS auction FROM LOAD GENERATOR AUCTION FOR ALL TABLES;"
);

puts src.inspect

# Show the source
res = conn.exec("SHOW SOURCES")
res.each do |row|
  puts row
end
```text

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Ruby

```ruby
require 'pg'

conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")

# Create a view
view = conn.exec(
    "CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS
      SELECT sum(amount)
    FROM bids;"
);
puts view.inspect

# Show the view
res = conn.exec("SHOW MATERIALIZED VIEWS")
res.each do |row|
  puts row
end
```text

For more information, see [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view/).


## Stream

To take full advantage of incrementally updated materialized views from a Ruby application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`SUBSCRIBE` statement](/sql/subscribe/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`SUBSCRIBE` with `FETCH`](/sql/subscribe/#subscribing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```ruby
require 'pg'

# Locally running instance:
conn = PG.connect(host:"MATERIALIZE_HOST", port: 6875, user: "MATERIALIZE_USERNAME", password: "MATERIALIZE_PASSWORD")
conn.exec('BEGIN')
conn.exec('DECLARE c CURSOR FOR SUBSCRIBE amount_sum')

while true
  conn.exec('FETCH c') do |result|
    result.each do |row|
      puts row
    end
  end
end
```text

Each `result` of the [SUBSCRIBE output format](/sql/subscribe/#output) has exactly object. When a row of a subscribed view is **updated,** two objects will show up:

```json
...
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"1"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"2"}
{"mz_timestamp"=>"1648126887708", "mz_diff"=>"1", "sum"=>"3"}
{"mz_timestamp"=>"1648126897364", "mz_diff"=>"-1", "sum"=>"1"}
...
```text

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a retraction (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```bash

## Ruby ORMs

ORM frameworks like **Active Record** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.




---

## Rust cheatsheet


Materialize is **wire-compatible** with PostgreSQL, which means that Rust applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the [`postgres-openssl`](https://docs.rs/postgres-openssl/latest/postgres_openssl/) crate (the TLS support for [`tokio-postgres`](https://crates.io/crates/tokio-postgres) via `openssl`) to connect to Materialize and issue SQL commands.

## Connect

To connect to Materialize using `postgres-openssl`:

```rust
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres::{Client, Error};
use postgres_openssl::MakeTlsConnector;

pub(crate) fn create_client() -> Result<Client, Error> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());

    let config = "postgres://MATERIALIZE_USERNAME:APP_SPECIFIC_PASSWORD@MATERIALIZE_HOST:6875/materialize?sslmode=require";
    Client::connect(config, connector)
}
```bash

## Create tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

To create a table named `countries` in Materialize:

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_table() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE TABLE IF NOT EXISTS countries (
            code CHAR(2),
            name TEXT
        );
    ",
        &[],
    )
}
```bash

## Insert data into tables

To [insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn insert() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    let code = "GH";
    let name = "Ghana";

    client.execute(
        "INSERT INTO countries(code, name) VALUES($1, $2)",
        &[&code, &name],
    )
}
```bash

## Query

Querying Materialize is identical to querying a PostgreSQL database. Here's how to do a SELECT statement:

```rust
use crate::connection::create_client;

pub(crate) fn run_query () {
    let mut client = create_client().expect("Error creating client.");

    let results = client.query("SELECT code, name FROM countries;", &[]).expect("Error running query.");

    for row in results {
        println!("{:} - {:}", row.get::<usize, String>(0), row.get::<usize, String>(1));
    };
}
```bash

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Rust app to execute common DDL statements.

### Create a source from Rust

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_source() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE SOURCE IF NOT EXISTS auction
        FROM LOAD GENERATOR AUCTION FOR ALL TABLES;
    ",
        &[],
    )
}
```bash

### Create a view from Rust

```rust
use postgres::Error;

use crate::connection::create_client;

pub(crate) fn create_materialized_view() -> Result<u64, Error> {
    let mut client = create_client().expect("Error creating client.");

    client.execute(
        "
        CREATE MATERIALIZED VIEW IF NOT EXISTS amount_sum AS
        SELECT sum(amount)
        FROM bids;
    ",
        &[],
    )
}
```bash

## Stream

Materialize is designed to stream changes to views. To subscribe to a stream of updates in Rust, you can use the `SUBSCRIBE` feature. Here's how to subscribe to a stream:

```rust
use crate::connection::create_client;

pub(crate) fn subscribe() {
    let mut client = create_client().expect("Error creating client.");
    let mut transaction = client.transaction().expect("Error creating transaction.");
    transaction.execute("DECLARE c CURSOR FOR SUBSCRIBE (SELECT sum::text FROM amount_sum) WITH (SNAPSHOT = false);", &[]).expect("Error creating cursor.");

    loop {
        let results = transaction.query("FETCH ALL c;", &[]).expect("Error running fetch.");
        for row in results {
            println!("{:}", row.get::<usize, String>(2));
        }
    }
}
```text

The [SUBSCRIBE output format](/sql/subscribe/#output) of the `amount_sum` view contains all of the columns of the view, prepended with several additional columns that describe the nature of the update.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS amount_sum;
DROP SOURCE IF EXISTS auction CASCADE;
DROP TABLE IF EXISTS countries;
```

## ORM

Rust ORMs like `Diesel` and `sqlx` tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.



