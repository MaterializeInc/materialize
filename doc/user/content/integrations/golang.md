---
title: "Golang cheatsheet"
description: "Use Go to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/golang/
menu:
  main:
    parent: "client-libraries"
    name: Go
---

Materialize is **wire-compatible** with PostgreSQL, which means that Go applications can use the standard library's [`database/sql`](https://pkg.go.dev/database/sql) package with a PostgreSQL driver to interact with Materialize. In this guide, we'll use the [`pgx` driver](https://github.com/jackc/pgx) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://pkg.go.dev/github.com/jackc/pgx#ConnConfig) to a local Materialize instance using `pgx`, you can use either a URI or DSN [connection string](https://pkg.go.dev/github.com/jackc/pgx#ParseConnectionString):

```go
package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"log"
)

func main() {

	ctx := context.Background()
	connStr := "postgres://materialize@localhost:6875/materialize?sslmode=disable"

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
}
```

To create a concurrency-safe connection pool, import the [`pgxpool` package](https://pkg.go.dev/github.com/jackc/pgx/v4/pgxpool) and use `pgxpool.Connect`.

The remainder of this guide uses the [`*pgx.Conn`](https://pkg.go.dev/github.com/jackc/pgx#Conn) connection handle from the [connect](#connect) section to interact with Materialize.

## Stream

To take full advantage of incrementally updated materialized views from a Go application, instead of [querying](#query) Materialize for the state of a view at a point in time, use a [`TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query:

```go
tx, err := conn.Begin(ctx)
if err != nil {
    log.Fatal(err)
    return
}
defer tx.Rollback(ctx)

_, err = tx.Exec(ctx, "DECLARE c CURSOR FOR TAIL my_view")
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
        var r tailResult
        if err := rows.Scan(&r.MzTimestamp, &r.MzDiff, ...); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("%+v\n", r)
        // operate on tailResult
    }
}

err = tx.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
```

The [TAIL output format](/sql/tail/#output) of `tailResult` contains all of the columns of `my_view`, prepended with several additional columns that describe the nature of the update.  When a row of a tailed view is **updated,** two objects will show up in the result set:

```go
{MzTimestamp:1646868332570 MzDiff:1 row...}
{MzTimestamp:1646868332570 MzDiff:-1 row...}
```

An `MzDiff` value of `-1` indicates that Materialize is deleting one row with the included values. An update is just a retraction (`MzDiff:-1`) and an insertion (`MzDiff:1`) with the same timestamp.

## Query

Querying Materialize is identical to querying a PostgreSQL database: Go executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query a view `my_view` using a `SELECT` statement:

```go
rows, err := conn.Query(ctx, "SELECT * FROM my_view")
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

## Insert data into tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

```go
insertSQL := "INSERT INTO countries (code, name) VALUES ($1, $2)"

_, err := conn.Exec(ctx, insertSQL, "GH", "GHANA")
if err != nil {
    log.Fatal(err)
}
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, but it's possible to use a Go app to execute common DDL statements.

### Create a source from Go

```go
createSourceSQL := `CREATE SOURCE counter FROM LOAD GENERATOR COUNTER`

_, err = conn.Exec(ctx, createSourceSQL)
if err != nil {
    log.Fatal(err)
}
```
For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Go

```go
createViewSQL := `CREATE VIEW market_orders_2 AS
                    SELECT
                        val->>'symbol' AS symbol,
                        (val->'bid_price')::float AS bid_price
                    FROM (SELECT text::jsonb AS val FROM market_orders_raw)`
_, err = conn.Exec(ctx, createViewSQL)
if err != nil {
    log.Fatal(err)
}
```

For more information, see [`CREATE VIEW`](/sql/create-view/).

## Go ORMs

ORM frameworks like **GORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.
