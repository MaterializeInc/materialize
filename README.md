[![Build status](https://badge.buildkite.com/97d6604e015bf633d1c2a12d166bb46f3b43a927d3952c999a.svg?branch=master)](https://buildkite.com/materialize/tests)
[![Doc reference](https://img.shields.io/badge/doc-reference-orange)](https://materialize.io/docs)

[<img src="https://materializeinc.wpengine.com/wp-content/uploads/2020/01/materialize_logo_primary.png" width=60%>](https://materialize.io)

Materialize is a streaming data warehouse.

Materialize lets you ask questions of your streaming data, and get the latest answers back in real time – offering the power and flexibility of a SQL data warehouse for the world of real-time data. Materialize is powered by [timely dataflow](https://github.com/TimelyDataflow/timely-dataflow).

Materialize computes correct answers. We support a large fraction of  PostgreSQL, and are actively working on supporting more builtin PostgreSQL functions. Please file an issue if there's something that you expected to work that didn't!

## Get data in

Materialize reads Avro, Protobuf, JSON, and newline-delimited text. Need something else? [Just ask](https://github.com/MaterializeInc/materialize/issues/new/choose).

Materialize can read data from Kafka topics or just tail local files.

**Coming soon**:

-   Support for AWS Kinesis streams, Azure Event Hub
-   Reading ORC and Parquet files on object storage
-   Getting data in from arbitrary HTTP endpoints

## Transform, manipulate, and read your data

Once you've got the data in, define views and perform reads via the PostgreSQL protocol. Use your favorite PostgreSQL CLI, including the `psql` you probably already have on your system!

Materialize supports a comprehensive variery of SQL features, all using the PostgreSQL dialect and protocol:

-   Joins, Joins, Joins! Materialize supports multi-column join conditions, multi-way joins over, self-joins, cross-joins, inner joins, outer joins, etc.
-   Delta-join execution to avoid intermediate state blowup - joins tested up to 64 topics.
-   Support for subqueries. Materialize's SQL optimizer performs subquery decorrelation out-of-the-box, avoiding the need to manually rewrite subqueries into joins.
-   Materialize supports streams that contain CDC data (currently supporting the [Debezium](https://debezium.io/blog/2017/09/25/streaming-to-another-database/) format). Materialize can incrementally maintain views in the presence of arbitrary inserts, updates, and deletes. No asterisks.
-   All the aggregations. `GROUP BY` , `MIN`, `MAX`, `COUNT`, `SUM`, `STDDEV`, `HAVING`, etc.
-   `ORDER BY`
-   `LIMIT`
-   `DISTINCT`
-   JSON support in the PostgreSQL dialect including operators and functions like `->`, `->>`, `@>`, `?`, `jsonb_array_element`, `jsonb_each`! Materialize automatically plans lateral joins for efficient `jsonb_each` support.
-   Nest views on views on views!
-   Multiple views that have overlapping subplans can share underlying indices for space and compute efficiency, so just declaratively define _what you want_, and we'll worry about how to efficiently maintain them.

### Just show us what it can do!

Here's an example join query that works fine in Materialize, `TPC-H` query 15:

```sql
CREATE VIEW revenue (supplier_no, total_revenue) AS
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount))
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-04-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' month
    GROUP BY
        l_suppkey;

CREATE MATERIALIZED VIEW tpch_q15 AS
  SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            max(total_revenue)
        FROM
            revenue
    )
ORDER BY
    s_suppkey

```

Stream inserts, updates, and deletes on the underlying tables (`lineitem` and `supplier`), and Materialize keeps the final materialized view incrementally updated (with the intermediate non-materialized view taking up no additional state!).

## Get data out

**Pull based**: Use any PostgreSQL-compatible driver in any language/environment to make `SELECT` queries against your views. Tell them they're talking to a PostgreSQL database, they don't ever need to know otherwise.

**Push based**: Or configure Materialize to stream results to a Kafka topic as soon as the views change.

If you want to use an ORM, [chat with us](https://github.com/MaterializeInc/materialize/issues/new/choose). They're surprisingly tricky.

## Get started

Check out [our getting started guide](https://materialize.io/docs/get-started/).

## Documentation

Check out [our documentation](https://materialize.io/docs/).

## License

Materialize is source-available and licensed under the BSL, converting to the open-source Apache 2.0 license after 4 years. Materialize is free forever on a single node.

Materialize is available as [a paid cloud service](https://materializeinc.wpengine.com/download/) with high availability.

## How does it work?

Materialize is built on top of [differential dataflow](https://github.com/TimelyDataflow/differential-dataflow) and [timely dataflow](https://github.com/TimelyDataflow/timely-dataflow), and built on a half-decade of cutting-edge stream processing research.

## For developers

Materialize is built entirely in Rust.

Materialize's developers can find docs at [doc/developer](doc/developer),
and Rust API documentation is hosted at <https://mtrlz.dev/api/>. The Materialize development roadmap is divided up into roughly month-long milestones, and [managed in GitHub](https://github.com/MaterializeInc/materialize/milestones?direction=asc&sort=due_date&state=open).

Contributions are welcome. Prospective code contributors might find the [good first issue tag](https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+is%3Aissue+label%3A%22D-good+first+issue%22) useful. We value all contributions equally, but bug reports are more equal.

## Credits

Materialize is lovingly built by [a team of developers](https://github.com/MaterializeInc/materialize/graphs/contributors) and one bot. [Join us](https://jobs.lever.co/materialize).
