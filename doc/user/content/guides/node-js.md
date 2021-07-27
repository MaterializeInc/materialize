---
title: "Node.js and Materialize"
description: "This guide explains how to use a Node.js application to connect, insert data, manage, and read data from Materialize."
weight: 10
menu:
  main:
    parent: guides
---

This guide explains all you need to know to start working with Materialize from a Node.js Application.

Materialize is **PostgreSQL-compatible**, this means that a Node.js application can use any existing PostgreSQL client like `pg` ([node-postgres](https://node-postgres.com/)) to connect, manage, and read from Materialize as if it were a PostgreSQL database.

## Connect

Connecting to Materialize is the same as [connecting to PostgreSQL with `node-postgres`](https://node-postgres.com/features/connecting).

### Local Instance

Connect to a local materialized instance using a connection URI:

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

This code uses connection URI shorthand (`postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`) to connect to a local materialized instance.

### Materialize Cloud Instance

Connect to a Materialize Cloud instance using a connection URI and certificates:

```js
const { Client } = require('pg');
const { fs } = require('fs');
const client = new Client({
    connectionString: "postgresql://materialize@MY_INSTANCE_ID.materialize.cloud:6875/materialize"
    ssl: {
        ca   : fs.readFileSync("server-ca.pem").toString(),
        key  : fs.readFileSync("client-key.pem").toString(),
        cert : fs.readFileSync("client-cert.pem").toString(),
    }
});

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

Replace `MY_INSTANCE_ID`) in the code above with your Materialize Cloud instance ID. The [`ssl` property](https://node-postgres.com/features/ssl), loads the certificate files downloaded from the Materialize Cloud [Connect](/cloud/connect-to-materialize-cloud/) dialog.

## Stream

To take advantage of incrementally updated materialized views from a Node.js application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

Read a stream of updates from a materialized view named `my_view` into Node.js:

```js
const { Client } = require('pg');

async function main() {
  const client = new Client('postgres://materialize@localhost:6875/materialize');
  await client.connect();

  await client.query('BEGIN');
  await client.query('DECLARE c CURSOR FOR TAIL my_view');

  while (true) {
    const res = await client.query('FETCH ALL c');
    console.log(res.rows);
  }
}

main();
```

The code above opens a long-lived transaction with `BEGIN` and uses [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to receive a stream of updates to `my_view`. The [TAIL Output format](/sql/tail/#output) of `res.rows` is an array of view update objects.

**Note on output format:** When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

```js
[
    ...
    {
        mz_timestamp: '1627225629000',
        mz_diff: '1',
        my_column_one: 'ABC',
        my_column_two: 'new_value'
    },
    {
        mz_timestamp: '1627225629000',
        mz_diff: '-1',
        my_column_one: 'ABC',
        my_column_two: 'old_value'
    },
    ...
]
```

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values.  An update is just a deletion (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.

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
  const stream = client.query(new QueryStream('TAIL avg_bid', []));
  stream.pipe(process.stdout);
});
```
--->

## Query

Querying Materialize is identical to querying a traditional PostgreSQL database: Node.js executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Query a view `my_view` with a select statement:

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
  await client.connect();
  const res = await client.query('SELECT * FROM my_view');
  console.log(res.rows);
};

main();
```

Polling _(repeatedly querying)_ of a view is fine, and response times are much faster than traditional database queries because Materialize maintains materialized views in memory. For more, refer to the node-postgres documentation of [Queries in node-postgres](https://node-postgres.com/features/queries) and [pg.Result](https://node-postgres.com/api/result) for output reference.

**Note:** If your SELECT queries do more than read out of a materialization, think about whether you should materialize part of the query, or the whole thing.

## Push data to a source

Data produced in a Node.js app **cannot be sent directly to a [`SOURCE` in Materialize](/sql/create-source/)** for a couple of reasons:

-  Materialize relies on another system (a traditional database, S3 or Kafka) to serve as the "system of record" for data. Materialize processes a live stream of data and maintains views in memory.
-  Sending data to an intermediary system helps decouple the operation of the Node.js app from the operation of Materialize.

The table below lists the options for **intermediary systems** that a Node.js Application can send data through to reach Materialize.

Intermediary System | Notes and Resources
-------------|-------------
**Kafka** | Produce messages from [Node.js to Kafka](https://kafka.js.org/docs/getting-started), and create a [Materialize Kafka Source](/sql/create-source/json-kafka/) to consume them. Kafka is recommended for scenarios where low-latency and high-throughput are important.
**Kinesis** | Send data from [Node.js to a Kinesis stream](https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-implementation-app-nodejs.html) and consume them with a [Materialize Kinesis source](/sql/create-source/json-kinesis/). Kinesis is easier to configure and maintain than Kafka but less fully-featured and configurable. For example, Kinesis is a [volatile source](/overview/volatility/) because it cannot do infinite retention.
**PostgreSQL** | Node.js sends data to PostgreSQL, and the [Materialize PostgreSQL source](https://materialize.com/docs/sql/create-source/postgres/) receives events from the change feed (the write-ahead log) of the database. Ideal for Node.js apps that already use PostgreSQL and fast-changing relational data.
**PubNub** | Streams as a service provider PubNub provides a [Node.js SDK](https://www.pubnub.com/docs/sdks/javascript/nodejs) to send data into a stream, [Materialize PubNub source](/sql/create-source/json-pubnub/) subscribes to the stream and consumes data.
**S3** | [Write data from Node.js to S3](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/getting-started-nodejs.html) in an append-only fashion, use the Materialize [S3 Source](/sql/create-source/json-s3), to scan the S3 bucket for data and [listen for new data via SQS notifications](/sql/create-source/json-s3/#listening-to-sqs-notifications). If data is already sent to S3 and minute latency is not an issue, this is an economical and low-maintenance option.

## Insert data into tables

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE` in Materialize](https://materialize.com/docs/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](https://materialize.com/docs/sql/insert/) of data into a table named `countries` in Materialize.

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

const text = 'INSERT INTO countries(code, name) VALUES($1, $2);';
const values = ['GH', 'GHANA'];

async function main() {
    await client.connect();
    const res = await client.query(text, values);
    console.log(res);
}

main();
```

## Manage sources, views, indexes

Creation of sources, views and indexes in Materialize should be treated with the same care as schema creation and updates in a traditional database. These are typically done during Materialize deployment. However, a Node.js app is perfectly capable of executing these DDL statements.

**Create a source from Node.js:**

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
    await client.connect();
    const res = await client.query(
        `CREATE SOURCE market_orders_raw_2 FROM PUBNUB
            SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
            CHANNEL 'pubnub-market-orders'`
        );
    console.log(res);
}

main();
```

See [`CREATE SOURCE`](/sql/create-source/) reference for more information.

**Create a view from Node.js:**

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
    await client.connect();
    const res = await client.query(
        `CREATE VIEW market_orders_2 AS
            SELECT
                val->>'symbol' AS symbol,
                (val->'bid_price')::float AS bid_price
            FROM (SELECT text::jsonb AS val FROM market_orders_raw)`
        );
    console.log(res);
}

main();
```

See [`CREATE VIEW`](/sql/create-view/) reference for more information.

## Node.js ORMs

While it communicates over the same wire protocol as PostgreSQL, Materialize doesn't currently respond to the entire catalog of PostgreSQL system metadata API endpoints. Object-relational-mapping (ORM) systems like **Prisma**, **Sequelize** or **TypeORM** use these PostgreSQL system calls (often within the `pg_catalog` namespace) to introspect the database and do extra work behind the scenes.

Attempting to use an ORM system to interact with Materialize as if it were a PostgreSQL database will currently not work. Libraries that currently return `pg_catalog` errors may work properly once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented.
