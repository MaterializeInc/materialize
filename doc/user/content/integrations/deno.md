---
title: "Deno Cheatsheet"
description: "Use Deno to connect, insert, manage, query and stream from Materialize."
aliases:
  - /guides/deno/
menu:
  main:
    parent: 'client-libraries'
    name: "Deno"
---

Materialize is **PostgreSQL-compatible**, which means that Deno applications can use any existing PostgreSQL client to interact with Materialize as if it were a PostgreSQL database. In this guide, we'll use the [Deno PostgreSQL driver](https://deno.land/x/postgres) to connect to Materialize and issue PostgreSQL commands.

## Connect

You connect to Materialize the same way you [connect to PostgreSQL with `deno-postgres`](https://deno.land/x/postgres).

### Local Instance

You can connect to a local Materialize instance just as you would connect to a PostgreSQL instance:

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client({
    user: "materialize",
    database: "materialize",
    password: "materialize",
    hostname: "127.0.0.1",
    port: 6875
})

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()
        /* Work with Materialize */

    } catch (err) {
        response.status = 500
        response.body = {
            success: false,
            msg: err.toString()
        }
    } finally {
        await client.end()
    }
}
export { main }
```

Alternatively, you can connect to Materialize instance with code that uses the connection URI shorthand (`postgres://<USER>@<HOST>:<PORT>/<SCHEMA>`) to define the URI:

```js
const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')
```

## Stream

To take full advantage of incrementally updated materialized views from a Deno application, instead of [querying](#query) Materialize for the state of a view at a point in time, use [a `TAIL` statement](/sql/tail/) to request a stream of updates as the view changes.

To read a stream of updates from an existing materialized view, open a long-lived transaction with `BEGIN` and use [`TAIL` with `FETCH`](/sql/tail/#tailing-with-fetch) to repeatedly fetch all changes to the view since the last query.

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()

        await client.queryObject('BEGIN');
        await client.queryObject('DECLARE c CURSOR FOR TAIL products');

        while (true) {
            const res = await client.queryObject('FETCH ALL c');
            console.log(res.rows);
        }
    } catch (err) {
        console.error(err.toString())
    } finally {
        await client.end()
    }
}

export { main }
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

An `mz_diff` value of `-1` indicates Materialize is deleting one row with the included values. An update is just a deletion (`mz_diff: '-1'`) and an insertion (`mz_diff: '1'`) with the same `mz_timestamp`.

## Query

Querying Materialize is identical to querying a traditional PostgreSQL database: Deno executes the query, and Materialize returns the state of the view, source, or table at that point in time.

Because Materialize maintains materialized views in memory, response times are much faster than traditional database queries, and polling (repeatedly querying) a view doesn't impact performance.

Query a view `my_view` with a select statement:

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()
        const result = await client.queryObject("SELECT * FROM my_view")
        console.log(result.rows)
    } catch (err) {
        console.error(err.toString())
    } finally {
        await client.end()
    }
}
export { main }
```

For more details, see the [`deno-postgres` executing queries](https://deno-postgres.com/#/?id=executing-queries) documentation.

## Insert data into tables

Most data in Materialize will stream in via a `SOURCE`, but a [`TABLE` in Materialize](/sql/create-table/) can be helpful for supplementary data. For example, use a table to join slower-moving reference or lookup data with a stream.

**Basic Example:** [Insert a row](/sql/insert/) of data into a table named `countries` in Materialize.

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()

        await client.queryObject(
            "INSERT INTO countries(code, name) VALUES($1, $2)",
            ['GH', 'GHANA'],
        );

        const result = await client.queryObject("SELECT * FROM countries")
        console.log(result.rows)
    } catch (err) {
        console.error(err.toString())
    } finally {
        await client.end()
    }
}
export { main }
```

## Manage sources, views, and indexes

Typically, you create sources, views, and indexes when deploying Materialize, although it is possible to use a Deno app to execute common DDL statements.

### Create a source from Deno

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()

        await client.queryObject(
            `CREATE SOURCE market_orders_raw FROM PUBNUB
                SUBSCRIBE KEY 'sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe'
                CHANNEL 'pubnub-market-orders'`
        );

        const result = await client.queryObject("SHOW SOURCES")
        console.log(result.rows)
    } catch (err) {
        console.error(err.toString())
    } finally {
        await client.end()
    }
}
export { main }
```

For more information, see [`CREATE SOURCE`](/sql/create-source/).

### Create a view from Deno

```js
import { Client } from "https://deno.land/x/postgres/mod.ts";

const client = new Client('postgres://materialize@127.0.0.1:6875/materialize')

const main = async ({ response }: { response: any }) => {
    try {
        await client.connect()

        await client.queryObject(
            `CREATE VIEW market_orders AS
                SELECT
                    val->>'symbol' AS symbol,
                    (val->'bid_price')::float AS bid_price
                FROM (SELECT text::jsonb AS val FROM market_orders_raw)`
        );

        const result = await client.queryObject("SHOW VIEWS")
        console.log(result.rows)
    } catch (err) {
        console.error(err.toString())
    } finally {
        await client.end()
    }
}
export { main }
```

For more information, see [`CREATE VIEW`](/sql/create-view/).

## Deno ORMs

Materialize doesn't currently support the full catalog of PostgreSQL system metadata API endpoints, including the system calls that object relational mapping systems (ORMs) like **TypeORM** use to introspect databases and do extra work behind the scenes. This means that ORM system attempts to interact with Materialize will currently fail. Once [full `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157) is implemented, the features that depend on `pg_catalog` may work properly.
