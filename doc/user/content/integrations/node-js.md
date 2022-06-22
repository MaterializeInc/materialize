---
title: "Node.js Cheatsheet"
description: "Use Node.js to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/node-js/
menu:
  main:
    parent: 'client-libraries'
    name: "Node.js"
---

Materialize is **PostgreSQL-compatible**, which means that Node.js applications can use any existing PostgreSQL client to interact with Materialize as if it were a PostgreSQL database. In this guide, we'll use the  [`node-postgres` library](https://node-postgres.com/) to connect to Materialize and issue PostgreSQL commands.

## Connect

You connect to Materialize the same way you [connect to PostgreSQL with `node-postgres`](https://node-postgres.com/features/connecting).

### Local Instance

You can connect to a local Materialize instance with code that uses the connection URI shorthand (`postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`) to define the URI:

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

### Materialize Cloud Instance

Download your instance's certificate files from the Materialize Cloud [Connect](/cloud/connect-to-cloud/) dialog and specify the path to each file in the [`ssl` property](https://node-postgres.com/features/ssl). Replace `MY_INSTANCE_ID` in the `connectionString` property with your Materialize Cloud instance ID.

```js
const { Client } = require('pg');
const fs = require('fs');
const client = new Client({
    connectionString: "postgresql://materialize@MY_INSTANCE_ID.materialize.cloud:6875/materialize",
    ssl: {
        ca   : fs.readFileSync("ca.crt").toString(),
        key  : fs.readFileSync("materialize.key").toString(),
        cert : fs.readFileSync("materialize.crt").toString(),
    }
});

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

## Stream

To take full advantage of incrementally updated materialized views from a Node.js application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query.

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

The [TAIL Output format](/sql/tail/#output) of `res.rows` is an array of view update objects. When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

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

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

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

For more details, see the  [`node-postgres` query](https://node-postgres.com/features/queries) and [pg.Result](https://node-postgres.com/api/result) documentation.

## Push data to a source

Materialize processes live streams of data and maintains views in memory, relying on other systems (traditional databases, or Kafka) to serve as "systems of record" for the data. Instead of updating Materialize directly, **Node.js should send data to an intermediary system**. Materialize connects to the intermediary system as a "source" and reads streaming updates from that.

The table below lists the intermediary systems a Node.js application can use to stream data for Materialize:

Intermediary System | Notes
-------------|-------------
**Kafka** | Produce messages from [Node.js to Kafka](https://kafka.js.org/docs/getting-started), and create a [Materialize Kafka Source](/sql/create-source/json-kafka/) to consume them. Kafka is recommended for scenarios where low-latency and high-throughput are important.
**PostgreSQL** | Node.js sends data to PostgreSQL, and the [Materialize PostgreSQL source](/sql/create-source/postgres/) receives events from the change feed (the write-ahead log) of the database. Ideal for Node.js apps that already use PostgreSQL and fast-changing relational data.
**PubNub** | Streams as a service provider PubNub provides a [Node.js SDK](https://www.pubnub.com/docs/sdks/javascript/nodejs) to send data into a stream, [Materialize PubNub source](/sql/create-source/json-pubnub/) subscribes to the stream and consumes data.

## Insert data into tables

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

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

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Node.js app to execute common DDL statements.

### Create a source from Node.js

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

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Node.js

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

For more information, see [`CREATE VIEW`](/sql/create-view/).

## Node.js ORMs

Materialize doesn't currently support the full catalog of PostgreSQL system metadata API endpoints, including the system calls that object relational mapping systems (ORMs) like **Prisma**, **Sequelize**, or **TypeORM** use to introspect databases and do extra work behind the scenes. This means that ORM system attempts to interact with Materialize will currently fail. Once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented, the features that depend on  `pg_catalog` may work properly.
