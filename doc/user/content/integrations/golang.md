---
title: "Golang Cheatsheet"
description: "Use Go to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/golang/
menu:
  main:
    parent: "client-libraries"
    name: Go
---

Materialize is **PostgreSQL-compatible**, which means that Go applications can use the standard library's [`database/sql`](https://pkg.go.dev/database/sql) package with a PostgreSQL driver to access Materialize as if it were a PostgreSQL database.
The [`pq` driver](https://github.com/lib/pq) was the standard by default, but is no longer in active development. In this guide we'll use the [`pgx` driver](https://github.com/jackc/pgx) connect to Materialize and issue PostgreSQL commands.

## Connect

You connect to Materialize the same way you [connect to PostgreSQL with `pgx`](https://pkg.go.dev/github.com/jackc/pgx#ConnConfig).

### Local Instance
Connect to a local Materialize instance just as you would connect to a PostgreSQL instance, using either a URI or DSN [connection string](https://pkg.go.dev/github.com/jackc/pgx#ParseConnectionString).

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

To create a concurrency-safe connection pool, import [`github.com/jackc/pgx/v4/pgxpool`](https://pkg.go.dev/github.com/jackc/pgx/v4/pgxpool) and use `pgxpool.Connect()`.

The rest of this guide uses the [`*pgx.Conn`](https://pkg.go.dev/github.com/jackc/pgx#Conn) connection handle from the [connect](#connect) section to interact with Materialize.

## Stream

To take full advantage of incrementally updated materialized views from a Go application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query.

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

The [TAIL Output format](/sql/tail/#output) of `tailResult` contains all of the columns of `my_view`, prepended with several additional columns that describe the nature of the update.  When a row of a tailed view is **updated,** two objects will show up in our result set:

```go
{MzTimestamp:1646868332570 MzDiff:1 row...}
{MzTimestamp:1646868332570 MzDiff:-1 row...}
```
An `MzDiff` value of `-1` indicates Materialize is deleting one row with the included values. An update is just a deletion (`MzDiff:-1`) and an insertion (`MzDiff:1`) with the same `MzTimestamp`.

## Query

Querying Materialize is identical to querying a traditional PostgreSQL database using Go: the database object executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

Query a view `my_view` with a select statement:

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

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE`](/sql/create-table/) can be helpful for supplementary data.
For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```go
insertSQL := "INSERT INTO countries (code, name) VALUES ($1, $2)"

_, err := conn.Exec(ctx, insertSQL, "GH", "GHANA")
if err != nil {
    log.Fatal(err)
}
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Go app to execute common DDL statements.

### Create a source from Golang

```go
createSourceSQL := `CREATE SOURCE market_orders_raw FROM PUBNUB
                SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe
                CHANNEL 'pubnub-market-orders'`

_, err = conn.Exec(ctx, createSourceSQL)
if err != nil {
    log.Fatal(err)
}
```
For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Golang

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

## Golang ORMs

Materialize doesn't currently support the full catalog of PostgreSQL system metadata API endpoints, including the system calls that object relational mapping systems (ORMs) like **GORM** use to introspect databases and do extra work behind the scenes. This means that ORM system attempts to interact with Materialize will currently fail. Once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented, the features that depend on  `pg_catalog` may work properly.
