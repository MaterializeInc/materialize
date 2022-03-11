---
title: "Golang"
description: "Use Go to connect, insert, manage, query and stream from Materialize."
weight:
menu:
  main:
    parent: guides
---

Materialize is **PostgreSQL-compatible**, which means that Go applications can use the standard library's [`database/sql`](https://pkg.go.dev/database/sql) package with a postgres driver to access Materialize as if it were a PostgreSQL database.
 [`pq`](https://github.com/lib/pq) was the standard by default, but is no longer in active development. In this guide we'll use [`pgx`](https://github.com/jackc/pgx).

## Connect
{{< tabs >}}
{{< tab "Local Instance">}}
Connect to a local Materialize instance with code that uses the connection URI shorthand `postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`.
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
{{< /tab >}}
{{< tab "Materialize Cloud Instance">}}

Download your instance's certificate files from the Materialize Cloud [Connect](/cloud/connect-to-cloud/) dialog and specify the path to each file in the [`ssl` property](https://node-postgres.com/features/ssl).
We'll specify our connection parameters explicitly here. Replace `MY_INSTANCE_ID` in the property with a Materialize Cloud instance ID.
```go
package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"log"
	"fmt"
)

func main() {

	ctx := context.Background()
	connStr := fmt.Sprint(
		" host=MY_INSTANCE_ID.materialize.cloud",
		" user=materialize",
		" port=6875",
		" dbname=materialize",
		" sslmode=verify-full",
		" sslrootcert=ca.crt",
		" sslkey=materialize.key",
		" sslcert=materialize.crt",
	)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		log.Fatal("Trouble Connecting to the database:", err)
	}
	defer conn.Close()
}
```
{{< /tab >}}
{{< /tabs >}}

To create a concurrency-safe connection pool, import [`github.com/jackc/pgx/v4/pgxpool`](https://pkg.go.dev/github.com/jackc/pgx/v4/pgxpool) and use pgxpool.Connect().
These two interfaces can be used interchangeably in the rest of this guide.

## Surface Data

Materialize ingests streams of data from Kafka, Kinesis, PostgreSQL, S3, or local files that we declare as “sources”.
These sources monitor changes in the upstream data and automatically pull new records into our Materialize instance.
We'll use our [`*pgx.Conn`](https://pkg.go.dev/github.com/jackc/pgx#Conn) connection handle from above to interact with our database.

### Create a source
{{< tabs >}}
{{< tab "Kafka">}}
```go
createSource := `CREATE SOURCE current_predictions
  FROM KAFKA BROKER 'localhost:9092' TOPIC 'events'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'https://localhost:8081'
  ENVELOPE UPSERT;`
```
Find more helpful Kafka source information [here](/sql/create-source/kafka/).
{{< /tab >}}
{{< tab "Kinesis">}}
```go
createSource := `CREATE SOURCE text_source
  FROM KINESIS ARN 'arn:aws:kinesis:aws-region::stream/fake-stream'
  WITH ( access_key_id = 'access_key_id',
         secret_access_key = 'secret_access_key' )
  FORMAT TEXT;'`
```
Find more helpful Kinesis source information [here](/sql/create-source/kinesis/).
{{< /tab >}}
{{< tab "File">}}
```go
createSource := `CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'`
```
Find more helpful File source information [here](/sql/create-source/text-file/).
{{< /tab >}}
{{< tab "S3">}}
```go
createSource := `CREATE SOURCE csv_source
  FROM S3 DISCOVER OBJECTS MATCHING '**/*.csv' USING
    BUCKET SCAN 'analytics'
  WITH (region = 'us-east-2')
  FORMAT CSV WITH 1 COLUMNS;`
```
Find more helpful S3 source information [here](/sql/create-source/s3/).
{{< /tab >}}
{{< tab "PubNub">}}
```go
createSource := `CREATE SOURCE market_orders_raw FROM PUBNUB
SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe
CHANNEL 'pubnub-market-orders'`
```
Find more helpful PubNub source information [here](/sql/create-source/text-pubnub/).

{{< /tab >}}
{{< /tabs >}}
```go
_, err = conn.Exec(ctx, createSource)
if err != nil {
    log.Fatal(err)
}
```

### Create and insert data into a table

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE`](/sql/create-table/) can be helpful for supplementary data.
Use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```go
createQuery := "CREATE TABLE countries (code text, name text NOT NULL);"
if _, err := conn.Exec(ctx, createQuery); err != nil {
log.Fatal(err)
}

insertQuery := "INSERT INTO countries (code, name) VALUES ($1, $2)"
if _, err := conn.Exec(ctx, insertQuery, "GH", "GHANA"); err != nil {
    log.Fatal(err)
}
```

## Transform Data

### Create a view
A [`MATERIALIZED VIEW`](/sql/create-materialized-view/) embeds a query like a traditional SQL view, but —unlike a SQL view— computes and incrementally updates the results of the embedded query.
This will later on let us read from materialized views and receive fresh answers with incredibly low latencies.
**Importantly**, Materialize supports views over multi-way joins with complex aggregations, and can do incremental updates in the presence of arbitrary inserts, updates, and deletes in the input streams.
```go
createView := `CREATE MATERIALIZED VIEW my_view AS
                    SELECT
                        val->>'symbol' AS symbol,
                        (val->'bid_price')::float AS bid_price
                    FROM (SELECT text::jsonb AS val FROM market_orders_raw)`
_, err = conn.Exec(ctx, createView)
if err != nil {
    log.Fatal(err)
}
```

## Use Data
### Query

Querying Materialize is identical to querying a traditional PostgreSQL database using Go: the database object executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

Query a view `my_view` with a select statement:

```go
rows, err := conn.Query(ctx, "SELECT * FROM my_view")
if err != nil {
    log.Fatal(err)
}

var results []result

for rows.Next() {
    var r result
    err = rows.Scan(&r...)
    if err != nil {
        log.Fatal(err)
    }
    // operate on our result struct
}
```

### Stream

To take full advantage of incrementally updated materialized views, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

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
        // operate on our tailResult struct
    }
}

err = tx.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
```

The [TAIL Output format](/sql/tail/#output) contains all of the columns of `my_view`, prepended with several additional columns that describe the nature of the update.
An `MzDiff` value of `-1` indicates Materialize is deleting one row with the included values. When a row of a tailed view is **updated,** two objects will show up in our result set: a deletion (`MzDiff:-1`) and an insertion (`MzDiff:1`) with the same `MzTimestamp`.

```go
{MzTimestamp:1646868332570 MzDiff:1 row...}
{MzTimestamp:1646868332570 MzDiff:-1 row...}
```

## Extra Credit
Use Golang to feed your source.

Intermediary System | Notes
-------------|-------------
**Kafka** | Produce messages from [Go to Kafka](https://pkg.go.dev/github.com/segmentio/kafka-go#section-readme) using one of the excellent go client libraries out there, and create a [Materialize Kafka Source](/sql/create-source/json-kafka/) to consume them. Kafka is recommended for scenarios where low-latency and high-throughput are important.
**Kinesis** | Send data to a kinesis stream and consume them with a [Materialize Kinesis source](/sql/create-source/json-kinesis/). Kinesis is easier to configure and maintain than Kafka but less fully-featured and configurable. For example, Kinesis is a [volatile source](/overview/volatility/) because it cannot do infinite retention.
**PubNub** | Streams as a service provider PubNub provides a [Go SDK](https://www.pubnub.com/docs/sdks/go) to send data into a stream, [Materialize PubNub source](/sql/create-source/json-pubnub/) subscribes to the stream and consumes data.
**S3** | [Write data from Go to S3](https://docs.aws.amazon.com/sdk-for-go/?id=docs_gateway) in an append-only fashion, use the Materialize [S3 Source](/sql/create-source/json-s3), to scan the S3 bucket for data and [listen for new data via SQS notifications](/sql/create-source/s3/#listening-to-sqs-notifications). If data is already sent to S3 and minute latency is not an issue, this is an economical and low-maintenance option.
**PostgreSQL** | Send data to PostgreSQL and the [Materialize PostgreSQL source](https://materialize.com/docs/sql/create-source/postgres/) receives events from the change feed (the write-ahead log) of the database. Ideal for apps that already use PostgreSQL and fast-changing relational data.
