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
```

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
```

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
```

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
```

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
        `CREATE SOURCE counter FROM LOAD GENERATOR COUNTER;`
        );
    console.log(res);
}

main();
```

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
        `CREATE MATERIALIZED VIEW IF NOT EXISTS counter_sum AS
            SELECT sum(counter)
        FROM counter;`
        );
    console.log(res);
}

main();
```

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
  await client.query('DECLARE c CURSOR FOR SUBSCRIBE counter_sum WITH (SNAPSHOT = FALSE)');

  while (true) {
    const res = await client.query('FETCH ALL c');
    console.log(res.rows);
  }
}

main();
```

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
  const stream = client.query(new QueryStream('SUBSCRIBE avg_bid', []));
  stream.pipe(process.stdout);
});
```
--->

## Clean up

To clean up the sources, views, and tables that we created, first connect to Materialize using a [PostgreSQL client](/integrations/sql-clients/) and then, run the following commands:

```mzsql
DROP MATERIALIZED VIEW IF EXISTS counter_sum;
DROP SOURCE IF EXISTS counter;
DROP TABLE IF EXISTS countries;
```

## Node.js ORMs

ORM frameworks like **Prisma**, **Sequelize**, or **TypeORM** tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if a tool is compatible with PostgreSQL, it’s not guaranteed that the same integration will work out-of-the-box.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.
