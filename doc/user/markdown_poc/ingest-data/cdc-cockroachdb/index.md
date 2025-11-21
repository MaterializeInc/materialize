<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Ingest data](/docs/ingest-data/)

</div>

# CockroachDB CDC using Kafka and Changefeeds

<div class="tip">

**💡 Tip:** For help getting started with your own data, you can
schedule a [free guided
trial](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

</div>

Change Data Capture (CDC) allows you to track and propagate changes in a
CockroachDB database to downstream consumers. In this guide, we’ll cover
how to use Materialize to create and efficiently maintain real-time
views with incrementally updated results on top of CockroachDB CDC data.

## A. Configure CockroachDB

### 1. Enable rangefeeds

As a first step, you must ensure
[rangefeeds](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds#enable-rangefeeds)
are enabled in your CockroachDB instance so you can create changefeeds
for the tables you want to replicate to Materialize.

1.  As a user with the `admin` role, enable the `kv.rangefeed.enabled`
    [cluster
    setting](https://www.cockroachlabs.com/docs/stable/set-cluster-setting):

    <div class="highlight">

    ``` chroma
    SET CLUSTER SETTING kv.rangefeed.enabled = true;
    ```

    </div>

### 2. Configure per-table changefeeds

[Changefeeds](https://www.cockroachlabs.com/docs/stable/change-data-capture-overview)
capture row-level changes resulting from `INSERT`, `UPDATE`, and
`DELETE` operations against CockroachDB tables and publish them as
events to Kafka (or other Kafka API-compatible broker). You can then use
the [Kafka source](/docs/sql/create-source/kafka/#using-debezium) to
consume these changefeed events into Materialize, making the data
available for transformation.

1.  [Create a
    changefeed](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds?)
    for each table you want to replicate:

    <div class="highlight">

    ``` chroma
    CREATE CHANGEFEED FOR TABLE my_table
      INTO 'kafka://broker:9092'
      WITH format = avro,
        confluent_schema_registry = 'http://registry:8081',
        diff,
        envelope = wrapped
    ```

    </div>

    We recommend creating changefeeds using the Avro format
    (`format = avro`) and the default [diff
    envelope](https://www.cockroachlabs.com/docs/v24.3/create-changefeed#envelope)
    (`envelope = wrapped`), which is compatible with the message format
    Materialize expects. Each table will produce data to a dedicated
    Kafka topic, which can then be consumed by Materialize.

For detailed instructions on configuring your CockroachDB instance for
CDC, refer to the [CockroachDB
documentation](https://www.cockroachlabs.com/docs/stable/create-changefeed).

## B. Ingest data in Materialize

### 1. (Optional) Create a cluster

<div class="note">

**NOTE:** If you are prototyping and already have a cluster to host your
Kafka source (e.g. `quickstart`), **you can skip this step**. For
production scenarios, we recommend separating your workloads into
multiple clusters for [resource
isolation](/docs/sql/create-cluster/#resource-isolation).

</div>

In Materialize, a [cluster](/docs/concepts/clusters/) is an isolated
environment, similar to a virtual warehouse in Snowflake. When you
create a cluster, you choose the size of its compute resource allocation
based on the work you need the cluster to do, whether ingesting data
from a source, computing always-up-to-date query results, serving
results to external clients, or a combination.

In this step, you’ll create a dedicated cluster for ingesting source
data from topics in your Kafka (or Kafka-API compatible) broker.

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE CLUSTER`](/docs/sql/create-cluster/) command to create the
    new cluster:

    <div class="highlight">

    ``` chroma
    CREATE CLUSTER ingest_kafka (SIZE = '100cc');

    SET CLUSTER = ingest_kafka;
    ```

    </div>

    A cluster of [size](/docs/sql/create-cluster/#size) `100cc` should
    be enough to accommodate multiple Kafka sources, depending on the
    source characteristics (e.g., sources with
    [`ENVELOPE UPSERT`](/docs/sql/create-source/#upsert-envelope) or
    [`ENVELOPE DEBEZIUM`](/docs/sql/create-source/#debezium-envelope)
    will be more memory-intensive) and the upstream traffic patterns.
    You can readjust the size of the cluster at any time using the
    [`ALTER CLUSTER`](/docs/sql/alter-cluster) command:

    <div class="highlight">

    ``` chroma
    ALTER CLUSTER <cluster_name> SET ( SIZE = <new_size> );
    ```

    </div>

### 2. Create a connection

Now that you’ve created an ingestion cluster, you can connect
Materialize to your Kafka broker and start ingesting data. The exact
steps depend on your authentication and networking configurations, so
refer to the [`CREATE CONNECTION`](/docs/sql/create-connection/#kafka)
documentation for further guidance.

1.  In the [SQL Shell](/docs/console/), or your preferred SQL client
    connected to Materialize, use the
    [`CREATE SECRET`](/docs/sql/create-secret/) command to securely
    store the credentials to connect to your Kafka broker and,
    optionally, schema registry:

    <div class="highlight">

    ``` chroma
    CREATE SECRET kafka_ssl_key AS '<BROKER_SSL_KEY>';
    CREATE SECRET kafka_ssl_crt AS '<BROKER_SSL_CRT>';
    CREATE SECRET csr_password AS '<CSR_PASSWORD>';
    ```

    </div>

2.  Use the [`CREATE CONNECTION`](/docs/sql/create-connection/#kafka)
    command to create a connection object with access and authentication
    details for Materialize to use:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION kafka_connection TO KAFKA (
      BROKER '<host>',
      SSL KEY = SECRET kafka_ssl_key,
      SSL CERTIFICATE = SECRET kafka_ssl_crt
    );
    ```

    </div>

    If you’re using a schema registry, create an additional connection
    object:

    <div class="highlight">

    ``` chroma
    CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (
      URL '<csr_url>',
      SSL KEY = SECRET csr_ssl_key,
      SSL CERTIFICATE = SECRET csr_ssl_crt,
      USERNAME = 'foo',
      PASSWORD = SECRET csr_password
    );
    ```

    </div>

## 3. Start ingesting data

1.  Use the [`CREATE SOURCE`](/docs/sql/create-source/) command to
    connect Materialize to your Kafka broker and start ingesting data
    from the target topic:

    <div class="highlight">

    ``` chroma
    CREATE SOURCE kafka_repl
      IN CLUSTER ingest_kafka
      FROM KAFKA CONNECTION kafka_connection (TOPIC 'my_table')
      -- CockroachDB's default envelope structure for changefeed messages is
      -- compatible with the Debezium format, so you can use ENVELOPE DEBEZIUM
      -- to interpret the data.
      ENVELOPE DEBEZIUM;
    ```

    </div>

    By default, the source will be created in the active cluster; to use
    a different cluster, use the `IN CLUSTER` clause.

### 4. Monitor the ingestion status

When a new source is created, Materialize performs a sync of all data
available in the upstream Kafka topic before it starts ingesting new
data — an operation known as *snapshotting*. Because the initial
snapshot is persisted in the storage layer atomically (i.e., at the same
ingestion timestamp), you will **not able to query the source until
snapshotting is complete**.

In this step, you’ll monitor the progress of the initial snapshot using
the observability features in the [Materialize Console](/docs/console/).

1.  If not already logged in, [log in to the Materialize
    Console](/docs/console/).

2.  Navigate to **Monitoring** \> **Sources** and click through to the
    source you created in the previous step. In the source overview
    page, you will see a progress bar with the status and progress of
    the snapshot.

3.  For the duration of the snapshotting operation, the source status
    will show as `Snapshotting`. Once the source status transitions from
    `Snapshotting` to `Running`, the source is ready for querying and
    you can move on to the next step.

    If the source fails to transition to this state, check the
    [ingestion troubleshooting
    guide](/docs/ingest-data/troubleshooting/).

### 5. Create a view

A [view](/docs/concepts/views/) saves a query under a name to provide a
shorthand for referencing the query. During view creation, the
underlying query is not executed.

<div class="highlight">

``` chroma
CREATE VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```

</div>

### 6. Create an index on the view

In Materialize, [indexes](/docs/concepts/indexes) on views compute and,
as new data arrives, incrementally update view results in memory within
a [cluster](/docs/concepts/clusters/) instead of recomputing the results
from scratch.

Create an index on `cnt_table1` view. Then, as new change events stream
in through Kafka (as the result of `INSERT`, `UPDATE` and `DELETE`
operations in the upstream database), the index incrementally updates
the view results in memory, such that the in-memory up-to-date results
are immediately available and computationally free to query.

<div class="highlight">

``` chroma
CREATE INDEX idx_cnt_table1_field1 ON cnt_table1(field1);
```

</div>

For best practices on when to index a view, see
[Indexes](/docs/concepts/indexes/) and [Views](/docs/concepts/views/).

## Next steps

With Materialize ingesting your CockroachDB data into durable storage,
you can start exploring the data, computing real-time results that stay
up-to-date as new data arrives, and serving results efficiently.

- Explore your data with [`SHOW SOURCES`](/docs/sql/show-sources) and
  [`SELECT`](/docs/sql/select/).

- Compute real-time results in memory with
  [`CREATE VIEW`](/docs/sql/create-view/) and
  [`CREATE INDEX`](/docs/sql/create-index/) or in durable storage with
  [`CREATE MATERIALIZED VIEW`](/docs/sql/create-materialized-view/).

- Serve results to a PostgreSQL-compatible SQL client or driver with
  [`SELECT`](/docs/sql/select/) or [`SUBSCRIBE`](/docs/sql/subscribe/)
  or to an external message broker with
  [`CREATE SINK`](/docs/sql/create-sink/).

- Check out the [tools and integrations](/docs/integrations/) supported
  by Materialize.

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/cdc-cockroachdb.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>
