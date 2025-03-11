[![Build status](https://badge.buildkite.com/97d6604e015bf633d1c2a12d166bb46f3b43a927d3952c999a.svg?branch=main)](https://buildkite.com/materialize/test)
[![Doc reference](https://img.shields.io/badge/doc-reference-orange)](https://materialize.com/docs)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20slack-purple)](https://materialize.com/s/chat)

[<img src="https://github.com/MaterializeInc/materialize/assets/23521087/39270ecb-7ac4-4829-b98b-c5b5699a16b8" width=35%>](https://materialize.com)

Materialize is a real-time data integration platform that creates and continually updates consistent views of transactional data from across your organization. Its SQL interface democratizes the ability to serve and access live data. Materialize can be deployed anywhere your infrastructure runs.

Use Materialize to do things like deliver fresh context for AI/RAG pipelines, power operational dashboards, and create more dynamic customer experiences without building time-consuming custom data pipelines.


The three most common patterns for adopting Materialize are the following:

- Query Offload (CQRS) - Scale complex read queries more efficiently than a read replica, and without the headaches of cache invalidation.
- Integration Hub (ODS) - Extract, load, and incrementally transform data from multiple sources. Create live views of your data that can be queried directly or pushed downstream.
- Operational Data Mesh (ODM) - Use SQL to create and deliver real-time, strongly consistent data products to streamline coordination across services and domains.


## Get started

Ready to try out Materialize? You can [sign up](https://materialize.com/register/) for a free cloud trial or [download](https://materialize.com/download/) our community edition, which is free forever for deployments using less than 24 GiB of memory and 48 GiB of disk!

Have questions? We'd love to hear from you:
  * [Join our Slack](https://materialize.com/s/chat)
  * [Send us mail](https://materialize.com/contact/)

## About

Materialize focuses on providing correct and [consistent](https://materialize.com/docs/overview/isolation-level/) answers with minimal latency, and does not ask you to accept either approximate answers or eventual consistency. Whenever Materialize answers a query, that answer is the correct result on some specific (and recent) version of your data. Materialize does all of this by recasting your SQL queries as *dataflows*, which can react efficiently to changes in your data as they happen.

Our fully managed service is cloud-native, featuring **high availability** through multi-active replication, **horizontal scalability** by seamlessly scaling dataflows across multiple machines, and **near-infinite storage** by leveraging cloud object storage (e.g., Amazon S3). You can self-manage Materialize using our Enterprise or Community editions.

We support a large fraction of PostgreSQL features and are actively expanding support for more built-in PostgreSQL functions. Please file an issue if you have an idea for an improvement!

## Get data in

Materialize can read data directly from a [PostgreSQL](https://materialize.com/docs/sql/create-source/postgres/) or [MySQL](https://materialize.com/docs/sql/create-source/mysql/) replication stream, from [Kafka](https://materialize.com/docs/sql/create-source/kafka/) (and other Kafka API-compatible systems like [Redpanda](https://materialize.com/docs/integrations/redpanda/)), or from SaaS applications [via webhooks](https://materialize.com/docs/sql/create-source/webhook/).

## Transform, manipulate, and read your data

Once you've got the data in, define views and perform reads via the PostgreSQL protocol. Use your favorite SQL client, including the `psql` you probably already have on your system. Customers using Materialize in production tend to use [dbt Core](https://www.getdbt.com/).

Materialize supports a comprehensive variety of SQL features, all using the PostgreSQL dialect and protocol:

-   Joins, joins, joins! Materialize supports multi-column join conditions, multi-way joins, self-joins, cross-joins, inner joins, outer joins, etc.
-   Delta-joins avoid intermediate state blowup compared to systems that can only plan nested binary joins - tested on joins of up to 64 relations.
-   Support for subqueries. Materialize's SQL optimizer performs subquery decorrelation out-of-the-box, avoiding the need to manually rewrite subqueries into joins.
-   Materialize can incrementally maintain views in the presence of arbitrary inserts, updates, and deletes. No asterisks.
-   All the aggregations: `min`, `max`, `count`, `sum`, `stddev`, etc.
-   `HAVING`
-   `ORDER BY`
-   `LIMIT`
-   `DISTINCT`
-   JSON support in the PostgreSQL dialect including operators and functions like `->`, `->>`, `@>`, `?`, `jsonb_array_element`, `jsonb_each`. Materialize automatically plans lateral joins for efficient `jsonb_each` support.
-   Nest views on views on views!
-   Multiple views that have overlapping subplans can share underlying indices for space and compute efficiency, so just declaratively define _what you want_, and we'll worry about how to efficiently maintain them.

We’ve also extended our SQL support to enable [recursion](https://materialize.com/blog/recursion-in-materialize/) that supports incrementally updating tree and graph structures.

### Just show us what it can do!

Here's an example join query that works fine in Materialize, `TPC-H` query 15:

```sql
CREATE SOURCE tpch
  FROM LOAD GENERATOR TPCH (SCALE FACTOR 1)
  FOR ALL TABLES;

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

**Pull based**: Use any PostgreSQL-compatible driver in any language/environment to make `SELECT` queries against your views. Tell them they're talking to a PostgreSQL database, they don't ever need to know otherwise. This is particularly helpful for pointing services and BI tools directly at Materialize.

**Push based**: Listen to changes directly using `SUBSCRIBE` or configure Materialize to stream results to a Kafka topic as soon as the views change. You can also copy updates to object storage.

## Documentation

Check out [our documentation](https://materialize.com/docs/).

## License

Materialize is provided as a self-managed product and a fully managed cloud service with
[credit-based pricing](https://materialize.com/pricing/). Included in the price
are proprietary cloud-native features like horizontal scalability, high
availability, and a web management console.

We're big believers in advancing the frontier of human knowledge. To
that end, the source code of the standalone database engine is publicly
available, in this repository, and [licensed](LICENSE) under the BSL 1.1,
converting to the open-source Apache 2.0 license after 4 years. As stated in the
BSL, use of the standalone database engine on a single node is free forever.

Materialize depends upon many open source Rust crates. We maintain a [list of
these crates and their licenses](https://dev.materialize.com/licenses.html),
including links to their source repositories.

## For developers

Materialize is primarily written in Rust.

Developers can find docs at [doc/developer](doc/developer), and Rust API documentation is hosted at <https://dev.materialize.com/api/rust/>.

Contributions are welcome. Prospective code contributors might find the [D-good
for external
contributors](https://github.com/MaterializeInc/materialize/discussions/categories/contribute-to-materialize?discussions_q=is%3Aopen+category%3A%22Contribute+to+Materialize%22+label%3A%22D-good+for+external+contributors%22)
discussion label useful. See
[CONTRIBUTING.md](https://github.com/MaterializeInc/materialize/blob/main/CONTRIBUTING.md)
for additional guidance.

## Credits

Materialize is lovingly crafted by [a team of developers](https://github.com/MaterializeInc/materialize/graphs/contributors) and one bot. [Join us](https://materialize.com/careers/).
