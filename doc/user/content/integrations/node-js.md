---
title: "Node.js cheatsheet"
description: "Use Node.js to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/node-js/
menu:
  main:
    parent: 'client-libraries'
    name: "Node.js"
---

Materialize is **wire-compatible** with PostgreSQL, which means that Node.js applications can use common PostgreSQL clients to interact with Materialize. In this guide, we'll use the  [`node-postgres` library](https://node-postgres.com/) to connect to Materialize and issue SQL commands.

## Connect

To [connect](https://node-postgres.com/features/connecting) to a local Materialize instance using `node-postgres`, you can use the connection URI shorthand (`postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`):

```js
const { Client } = require('pg');
const client = new Client('postgres://materialize@localhost:6875/materialize');

async function main() {
    await client.connect();
    /* Work with Materialize */
}

main();
```

## Stream

To take full advantage of incrementally updated materialized views from a Node.js application, instead of [querying](#query) Materialize for the state of a view at a point in time, you can use a [`TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query:

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

The [`TAIL` output format](/sql/tail/#output) of `res.rows` is an array of view update objects. When a row of a tailed view is **updated,** two objects will show up in the `rows` array:

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
  const stream = client.query(new QueryStream('TAIL avg_bid', []));
  stream.pipe(process.stdout);
});
```
--->

## Query

Querying Materialize is identical to querying a PostgreSQL database: Node.js executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

To query a view `my_view` using a `SELECT` statement:

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

Materialize processes live streams of data and maintains views in memory, relying on external systems (like PostgreSQL, or Kafka) to serve as "systems of record" for the data. Instead of updating Materialize directly, **Node.js should send data to an intermediary system**. Materialize connects to the intermediary system as a [source](/sql/create-source/) and reads streaming updates from it.

The table below lists the intermediary systems a Node.js application can use to feed data into Materialize:

Intermediary System | Notes
-------------|-------------
**Kafka** | Produce messages from [Node.js to Kafka](https://kafka.js.org/docs/getting-started), and create a [Kafka source](/sql/create-source/json-kafka/) to consume them. This is recommended for scenarios where low-latency and high-throughput are important.
**PostgreSQL** | Send data from Node.js to PostgreSQL, and create a [PostgreSQL source](/sql/create-source/postgres/) that consumes a replication stream from the database based on its write-ahead log. This is recommended for Node.js apps that already use PostgreSQL and fast-changing transactional data.
**PubNub** | Send data to a PubNub stream using the [Node.js SDK](https://www.pubnub.com/docs/sdks/javascript/nodejs), and create a [PubNub source](/sql/create-source/json-pubnub/) that subscribes to the stream.

## Insert data into tables

Most data in Materialize will stream in via an external system, but a [table](/sql/create-table/) can be helpful for supplementary data. For example, you can use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize:

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

Typically, you create sources, views, and indexes when deploying Materialize, but it's also possible to use a Node.js app to execute common DDL statements.

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

ORM frameworks like **Prisma**, **Sequelize**, or **TypeORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.
