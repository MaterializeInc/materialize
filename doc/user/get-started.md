---
title: "Get started"
description: "Get started with Materialize"
menu: "main"
weight: 3
---

To help you get started with Materialize, we'll:

- Install, run, and connect to Materialize
- Explore its API
- Set up a real-time stream to perform aggregations on

## Prerequisites

To complete this demo, you need:

- Command line and network access.
- A [PostgreSQL-compatible CLI](../connect/cli/). If you have PostgreSQL installed `psql` works.

We also highly recommend checking out [What is Materialize?](../overview/what-is-materialize)

## Install, run, connect

1. Install the `materialized` binary using [these instructions](../install).
1. Run the `materialized` binary. For example, if you installed it in your `$PATH`:

    ```shell
    materialized
    ```

    This starts the daemon listening on port 6875.
1. Connect to `materialized` through your PostgreSQL CLI, e.g.:

    ```shell
    psql -h localhost -p 6875 materialize
    ```

## Explore Materialize's API

Materialize offers ANSI Standard SQL, but is not simply a relational database. Instead of tables of data, you typically connect Materialize to external sources of data (called **sources**), and then create materialized views of the data that Materialize sees from those sources.

To get started, though, we'll begin with a simple version that doesn't require connecting to an external data source.

1. From your PostgreSQL CLI, create a materialized view that contains actual data we can work with.

    ```sql
    CREATE MATERIALIZED VIEW pseudo_source AS
        SELECT column1 as key, column2 as value FROM (VALUES
            ('a', 1),
            ('a', 2),
            ('a', 3),
            ('a', 4),
            ('b', 5),
            ('c', 6),
            ('c', 7)) AS tbl;
    ```

    You'll notice that we end up entering data into Materialize by creating a materialized view from some other data, rather than the typical `INSERT` operation. This is how one interacts with Materialize. In most cases, this data would have come from an external source and get fed into Materialize from a file or a stream.

1. With data in a materialized view, we can perform arbitrary `SELECT` statements on the data.

    Let's start by viewing all of the data:

    ```sql
    SELECT * FROM pseudo_source;
    ```
    ```nofmt
     key | value
    -----+-------
     a   |     1
     a   |     2
     a   |     3
     a   |     4
     b   |     5
     c   |     6
     c   |     7
    ```

1. Determine the sum of the values for each key:

    ```sql
    SELECT key, sum(value) FROM pseudo_source GROUP BY key;
    ```
    ```nofmt
     key | sum
    -----+-----
     a   |  10
     b   |   5
     c   |  13
    ```

    We can actually then save this query as its own materialized view:

    ```sql
    CREATE MATERIALIZED VIEW key_sums AS
        SELECT key, sum(value) FROM pseudo_source GROUP BY key;
    ```

1. Determine the sum of all keys' sums:

    ```sql
    SELECT sum(sum) FROM key_sums;
    ```

1. We can also perform complex operations like `JOIN`s. Given the simplicity of our data, the `JOIN` clauses themselves aren't very exciting, but Materialize offers support for a full range of arbitrarily complex `JOIN`s.

    ```sql
    CREATE MATERIALIZED VIEW lhs AS
        SELECT column1 as key, column2 as value FROM (VALUES
            ('x', 'a'),
            ('y', 'b'),
            ('z', 'c')) AS lhs;
    ```
    ```sql
    SELECT lhs.key, sum(rhs.value)
    FROM lhs
    JOIN pseudo_source AS rhs
    ON lhs.value = rhs.key
    GROUP BY lhs.key;
    ```

Of course, these are trivial examples, but hope begin to illustrate some of Materialize's potential.

## Create a real-time stream

Materialize is built to handle streams of data, and provide incredibly low-latency answers to queries over that data. To show off that capability, in this section we'll set up a real-time stream, and then see how Materialize lets you query it.

1. We'll set up a stream of Wikipedia's recent changes, and simply write all data that we see to a file.

    From your shell, run:
    ```
    while true; do
      curl --max-time 9999999 -N https://stream.wikimedia.org/v2/stream/recentchange >> wikirecent
    done
    ```

    Note the absolute path of the location where you write `wikirecent`, which we'll need in the next step.

1. From within the CLI, create a source from the `wikirecent` file:

    ```sql
    CREATE SOURCE wikirecent
    FROM FILE '[path to wikirecent]'
    WITH ( tail=true )
    FORMAT REGEX '^data: (?P<data>.*)';
    ```

    This source takes the lines from the stream, finds those that begins with `data:`, and then captures the rest of the line in a column called `data`

    You can see the columns that get generated for this source:

    ```sql
    SHOW COLUMNS FROM wikirecent
    ```

1. Because this stream comes in as JSON, we'll need to normalize the data to perform aggregations on it. Materialize offers the ability to do this easily using our built-in [`jsonb` functions](/docs/sql/functions/#json).

    ```sql
    CREATE MATERIALIZED VIEW recentchanges AS
        SELECT
        val->>'$schema' AS r_schema,
        (val->'bot')::bool AS bot,
        val->>'comment' AS comment,
        (val->'id')::float::int AS id,
        (val->'length'->'new')::float::int AS length_new,
        (val->'length'->'old')::float::int AS length_old,
        val->'meta'->>'uri' AS meta_uri,
        val->'meta'->>'id' as meta_id,
        (val->'minor')::bool AS minor,
        (val->'namespace')::float AS namespace,
        val->>'parsedcomment' AS parsedcomment,
        (val->'revision'->'new')::float::int AS revision_new,
        (val->'revision'->'old')::float::int AS revision_old,
        val->>'server_name' AS server_name,
        (val->'server_script_path')::text AS server_script_path,
        val->>'server_url' AS server_url,
        (val->'timestamp')::float AS r_ts,
        val->>'title' AS title,
        val->>'type' AS type,
        val->>'user' AS user,
        val->>'wiki' AS wiki
        FROM (SELECT data::jsonb AS val FROM wikirecent);
    ```

1. From here we can start building our aggregations. The simplest place to start is simply counting the number of items we've seen:

    ```sql
    CREATE MATERIALIZED VIEW counter
    AS SELECT COUNT(*) FROM recentchanges;
    ```

1. However,  we can also see more interesting things from our stream. For instance, who are making the most changes to Wikipedia?

    ```sql
    CREATE MATERIALIZED VIEW useredits AS SELECT user, count(*) FROM recentchanges GROUP BY user;
    ```

    ```sql
    SELECT * FROM useredits ORDER BY count DESC;
    ```

1. If this is a factoid we often want to know, we could also create a view of just the top 10 editors we've seen.

    ```sql
    CREATE MATERIALIZED VIEW top10
    AS SELECT * FROM useredits ORDER BY "count" DESC LIMIT 10;
    ```

    We can then quickly get the answer to who the top 10 editors are:

    ```sql
    SELECT * FROM top10 ORDER BY "count" DESC;
    ```

Naturally, there are many interesting views of this data. If you're interested in continuing to explore it, you can checkout the stream's documentation from Wikipedia.

Once you're done, don't forget to stop `curl` and `rm wikirecent`.

## Up next

Checkout out [architecture overview](../overview/architecture).
