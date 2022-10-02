[![Build status](https://badge.buildkite.com/97d6604e015bf633d1c2a12d166bb46f3b43a927d3952c999a.svg?branch=main)](https://buildkite.com/materialize/tests)
[![Doc reference](https://img.shields.io/badge/doc-reference-orange)](https://materialize.com/docs)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20slack-purple)](https://materialize.com/s/chat)

[<img src="https://user-images.githubusercontent.com/23521087/168297221-5d346edc-3a55-4055-b355-281b4bd76963.png" width=55%>](https://materialize.com)

Materialize is a streaming database powered by [Timely](https://github.com/TimelyDataflow/timely-dataflow#timely-dataflow) and [Differential Dataflow](https://github.com/timelydataflow/differential-dataflow#differential-dataflow), purpose-built for low-latency applications. It lets you ask complex questions about your data using SQL, and maintains the results of these SQL queries incrementally up-to-date as the underlying data changes.

## Sign up for Early Access

We are rolling out Early Access to the new, cloud-native version of Materialize. [Sign up](https://materialize.com/register/) to get on the list! 🚀

## About

Materialize is designed to help you interactively explore your streaming data, perform analytics against live relational data, or just increase the freshness *and* reduce the load of your dashboard and monitoring tasks. The moment you need a refreshed answer, you can get it in milliseconds.

It focuses on providing correct and [consistent](https://materialize.com/docs/overview/isolation-level/) answers with minimal latency, and does not ask you to accept either approximate answers or eventual consistency. Whenever Materialize answers a query, that answer is the correct result on some specific (and recent) version of your data. Materialize does all of this by recasting your SQL queries as *dataflows*, which can react efficiently to changes in your data as they happen.

We support a large fraction of PostgreSQL, and are actively working on supporting more builtin PostgreSQL functions. Please file an issue if there's something that you expected to work that didn't!

## Get data in

Materialize can read data from [Kafka](https://materialize.com/docs/sql/create-source/kafka/) and [Redpanda](https://materialize.com/docs/integrations/redpanda/), as well as directly from a [PostgreSQL](https://materialize.com/docs/sql/create-source/postgres/) replication stream. It also supports regular database tables to which you can insert, update, and delete rows.

## Transform, manipulate, and read your data

Once you've got the data in, define views and perform reads via the PostgreSQL protocol. Use your favorite PostgreSQL CLI, including the `psql` you probably already have on your system.

Materialize supports a comprehensive variety of SQL features, all using the PostgreSQL dialect and protocol:

-   Joins, Joins, Joins! Materialize supports multi-column join conditions, multi-way joins, self-joins, cross-joins, inner joins, outer joins, etc.
-   Delta-joins avoid intermediate state blowup compared to systems that can only plan nested binary joins - tested on joins of up to 64 relations.
-   Support for subqueries. Materialize's SQL optimizer performs subquery decorrelation out-of-the-box, avoiding the need to manually rewrite subqueries into joins.
-   Materialize supports streams that contain CDC data (currently supporting the [Debezium](https://debezium.io/blog/2017/09/25/streaming-to-another-database/) format). Materialize can incrementally maintain views in the presence of arbitrary inserts, updates, and deletes. No asterisks.
-   All the aggregations. `GROUP BY` , `MIN`, `MAX`, `COUNT`, `SUM`, `STDDEV`, `HAVING`, etc.
-   `ORDER BY`
-   `LIMIT`
-   `DISTINCT`
-   JSON support in the PostgreSQL dialect including operators and functions like `->`, `->>`, `@>`, `?`, `jsonb_array_element`, `jsonb_each`. Materialize automatically plans lateral joins for efficient `jsonb_each` support.
-   Nest views on views on views!
-   Multiple views that have overlapping subplans can share underlying indices for space and compute efficiency, so just declaratively define _what you want_, and we'll worry about how to efficiently maintain them.

### Just show us what it can do!

Here's an example join query that works fine in Materialize, `TPC-H` query 15:

```sql
-- Views define commonly reused subqueries.
CREATE VIEW revenue (supplier_no, total_revenue) AS
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount))
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' month
    GROUP BY
        l_suppkey;

-- The MATERIALIZED keyword is the trigger to begin
-- eagerly, consistently, and incrementally maintaining
-- results that are stored directly in durable storage.
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
    s_suppkey;

-- Creating an index keeps results always up to date and in memory.
-- In this example, the index will allow for fast point lookups of
-- individual supply keys.
CREATE INDEX tpch_q15_idx ON tpch_q15 (s_suppkey);
```

Stream inserts, updates, and deletes on the underlying tables (`lineitem` and `supplier`), and Materialize keeps the materialized view incrementally updated. You can type `SELECT * FROM tpch_q15` and expect to see the current results immediately!

## Get data out

**Pull based**: Use any PostgreSQL-compatible driver in any language/environment to make `SELECT` queries against your views. Tell them they're talking to a PostgreSQL database, they don't ever need to know otherwise.

**Push based**: Listen to changes directly using `SUBSCRIBE` or configure Materialize to stream results to a Kafka topic as soon as the views change.

If you want to use an ORM, [chat with us](https://github.com/MaterializeInc/materialize/issues/new/choose). They're surprisingly tricky.

## Documentation

Check out [our documentation](https://materialize.com/docs/).

## License

Materialize is source-available and [licensed](LICENSE) under the BSL 1.1, converting to the open-source Apache 2.0 license after 4 years. As stated in the BSL, Materialize is free forever on a single node.

Materialize is also available as [a paid cloud service](https://materialize.com/pricing/) with additional features such as high availability via multi-active replication.

## For developers

Materialize is primarily written in Rust.

Developers can find docs at [doc/developer](doc/developer), and Rust API documentation is hosted at <https://dev.materialize.com/api/rust/>. The Materialize development roadmap is divided up into roughly month-long milestones, and [managed in GitHub](https://github.com/MaterializeInc/materialize/milestones?direction=asc&sort=due_date&state=open).

Contributions are welcome. Prospective code contributors might find the [good first issue tag](https://github.com/MaterializeInc/materialize/issues?q=is%3Aopen+is%3Aissue+label%3A%22D-good+first+issue%22) useful. We value all contributions equally, but bug reports are more equal.

## Credits

Materialize is lovingly crafted by [a team of developers](https://github.com/MaterializeInc/materialize/graphs/contributors) and one bot. [Join us](https://materialize.com/careers/).
