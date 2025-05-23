---
title: "Golang cheatsheet"
description: "Use Go to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/golang/
  - /integrations/golang/
menu:
  main:
    parent: "client-libraries"
    name: Go
---

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
```

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
```

## Insert data into tables

To [insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```go
insertSQL := "INSERT INTO countries (code, name) VALUES ($1, $2)"

_, err := conn.Exec(ctx, insertSQL, "GH", "GHANA")
if err != nil {
    log.Fatal(err)
}
```

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
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's possible to use a Go app to execute common DDL statements.

### Create a source from Go

```go
createSourceSQL := `
    CREATE SOURCE IF NOT EXISTS counter
    FROM LOAD GENERATOR COUNTER
    (TICK INTERVAL '500ms');
`

_, err = conn.Exec(ctx, createSourceSQL)
if err != nil {
    log.Fatal(err)
}
```
For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Go

```go
createViewSQL := `
    CREATE MATERIALIZED VIEW IF NOT EXISTS counter_sum AS
        SELECT sum(counter)
    FROM counter;
`

_, err = conn.Exec(ctx, createViewSQL)
if err != nil {
    log.Fatal(err)
}
```

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

_, err = tx.Exec(ctx, "DECLARE c CURSOR FOR SUBSCRIBE counter_sum")
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
```

The [SUBSCRIBE output format](/sql/subscribe/#output) of `subscribeResult` contains all of the columns of `counter_sum`, prepended with several additional columns that describe the nature of the update.  When a row of a subscribed view is **updated,** two objects will show up in the result set:

```go
{MzTimestamp:1646868332570 MzDiff:1 row...}
{MzTimestamp:1646868332570 MzDiff:-1 row...}
```

An `MzDiff` value of `-1` indicates that Materialize is deleting one row with the included values. An update is just a retraction (`MzDiff:-1`) and an insertion (`MzDiff:1`) with the same timestamp.

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS counter_sum;
DROP SOURCE IF EXISTS counter;
DROP TABLE IF EXISTS countries;
```

## Go ORMs

ORM frameworks like **GORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
