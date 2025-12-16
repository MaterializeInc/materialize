<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/self-managed/v25.2/)  /  [Ingest
data](/docs/self-managed/v25.2/ingest-data/)
 /  [PostgreSQL](/docs/self-managed/v25.2/ingest-data/postgres/)

</div>

# PostgreSQL CDC using Kafka and Debezium

<div class="warning">

**WARNING!** You can use [Debezium](https://debezium.io/) to propagate
Change Data Capture (CDC) data to Materialize from a PostgreSQL
database, but we **strongly recommend** using the native
[PostgreSQL](/docs/self-managed/v25.2/sql/create-source/postgres/)
source instead.

</div>

For help getting started with your own data, you can schedule a \[free
guided
trial\](https://materialize.com/demo/?utm_campaign=General&utm_source=documentation).

Change Data Capture (CDC) allows you to track and propagate changes in a
PostgreSQL database to downstream consumers based on its Write-Ahead Log
(`WAL`). In this guide, we’ll cover how to use Materialize to create and
efficiently maintain real-time views with incrementally updated results
on top of CDC data.

## Kafka + Debezium

You can use [Debezium](https://debezium.io/) and the [Kafka
source](/docs/self-managed/v25.2/sql/create-source/kafka/#using-debezium)
to propagate CDC data from PostgreSQL to Materialize in the unlikely
event that using the[native PostgreSQL
source](/docs/self-managed/v25.2/sql/create-source/postgres/) is not an
option. Debezium captures row-level changes resulting from `INSERT`,
`UPDATE` and `DELETE` operations in the upstream database and publishes
them as events to Kafka using Kafka Connect-compatible connectors.

### A. Configure database

**Minimum requirements:** PostgreSQL 11+

Before deploying a Debezium connector, you need to ensure that the
upstream database is configured to support [logical
replication](https://www.postgresql.org/docs/current/logical-replication.html).

<div class="code-tabs">

<div class="tab-content">

<div id="tab-self-hosted" class="tab-pane" title="Self-hosted">

As a *superuser*:

1.  Check the [`wal_level`
    configuration](https://www.postgresql.org/docs/current/wal-configuration.html)
    setting:

    <div class="highlight">

    ``` chroma
    SHOW wal_level;
    ```

    </div>

    The default value is `replica`. For CDC, you’ll need to set it to
    `logical` in the database configuration file (`postgresql.conf`).
    Keep in mind that changing the `wal_level` requires a restart of the
    PostgreSQL instance and can affect database performance.

2.  Restart the database so all changes can take effect.

</div>

<div id="tab-aws-rds" class="tab-pane" title="AWS RDS">

We recommend following the [AWS
RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_PostgreSQL.html#PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication)
documentation for detailed information on logical replication
configuration and best practices.

As a *superuser* (`rds_superuser`):

1.  Create a custom RDS parameter group and associate it with your
    instance. You will not be able to set custom parameters on the
    default RDS parameter groups.

2.  In the custom RDS parameter group, set the `rds.logical_replication`
    static parameter to `1`.

3.  Add the egress IP addresses associated with your Materialize region
    to the security group of the RDS instance.

4.  Restart the database so all changes can take effect.

</div>

<div id="tab-aws-aurora" class="tab-pane" title="AWS Aurora">

<div class="note">

**NOTE:** Aurora Serverless (v1) [does **not**
support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations)
logical replication, so it’s not possible to use this service with
Materialize.

</div>

We recommend following the [AWS
Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html#AuroraPostgreSQL.Replication.Logical.Configure)
documentation for detailed information on logical replication
configuration and best practices.

As a *superuser*:

1.  Create a DB cluster parameter group for your instance using the
    following settings:

    Set **Parameter group family** to your version of Aurora PostgreSQL.

    Set **Type** to **DB Cluster Parameter Group**.

2.  In the DB cluster parameter group, set the `rds.logical_replication`
    static parameter to `1`.

3.  In the DB cluster parameter group, set reasonable values for
    `max_replication_slots`, `max_wal_senders`,
    `max_logical_replication_workers`, and
    `max_worker_processes parameters` based on your expected usage.

4.  Add the egress IP addresses associated with your Materialize region
    to the security group of the DB instance.

5.  Restart the database so all changes can take effect.

</div>

<div id="tab-azure-db" class="tab-pane" title="Azure DB">

We recommend following the [Azure DB for
PostgreSQL](https://docs.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-logical#pre-requisites-for-logical-replication-and-logical-decoding)
documentation for detailed information on logical replication
configuration and best practices.

1.  In the Azure portal, or using the Azure CLI, [enable logical
    replication](https://docs.microsoft.com/en-us/azure/postgresql/concepts-logical#set-up-your-server)
    for the PostgreSQL instance.

2.  Add the egress IP addresses associated with your Materialize region
    to the list of allowed IP addresses under the “Connections security”
    menu.

3.  Restart the database so all changes can take effect.

</div>

<div id="tab-cloud-sql" class="tab-pane" title="Cloud SQL">

We recommend following the [Cloud SQL for
PostgreSQL](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication#configuring-your-postgresql-instance)
documentation for detailed information on logical replication
configuration and best practices.

As a *superuser* (`cloudsqlsuperuser`):

1.  In the Google Cloud Console, enable logical replication by setting
    the `cloudsql.logical_decoding` configuration parameter to `on`.

2.  Add the egress IP addresses associated with your Materialize region
    to the list of allowed IP addresses.

3.  Restart the database so all changes can take effect.

</div>

</div>

</div>

Once logical replication is enabled:

1.  Grant enough privileges to ensure Debezium can operate in the
    database. The specific privileges will depend on how much control
    you want to give to the replication user, so we recommend following
    the [Debezium
    documentation](https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-replication-user-privileges).

2.  If a table that you want to replicate has a **primary key** defined,
    you can use your default replica identity value. If a table you want
    to replicate has **no primary key** defined, you must set the
    replica identity value to `FULL`:

    <div class="highlight">

    ``` chroma
    ALTER TABLE repl_table REPLICA IDENTITY FULL;
    ```

    </div>

    This setting determines the amount of information that is written to
    the WAL in `UPDATE` and `DELETE` operations. Setting it to `FULL`
    will include the previous values of all the table’s columns in the
    change events.

    As a heads up, you should expect a performance hit in the database
    from increased CPU usage. For more information, see the [PostgreSQL
    documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).

### B. Deploy Debezium

**Minimum requirements:** Debezium 1.5+

Debezium is deployed as a set of Kafka Connect-compatible connectors, so
you first need to define a SQL connector configuration and then start
the connector by adding it to Kafka Connect.

<div class="warning">

**WARNING!** If you deploy the PostgreSQL Debezium connector in
[Confluent
Cloud](https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-debezium.html),
you **must** override the default value of `After-state only` to
`false`.

</div>

<div class="code-tabs">

<div class="tab-content">

<div id="tab-debezium-15" class="tab-pane" title="Debezium 1.5+">

1.  Create a connector configuration file and save it as
    `register-postgres.json`:

    <div class="highlight">

    ``` chroma
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "database.server.name": "pg_repl",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schemas.enable": false
        }
    }
    ```

    </div>

    You can read more about each configuration property in the [Debezium
    documentation](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka
    topic named `serverName.schemaName.tableName`.

2.  Start the PostgreSQL Debezium connector using the configuration
    file:

    <div class="highlight">

    ``` chroma
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```

    </div>

3.  Check that the connector is running:

    <div class="highlight">

    ``` chroma
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    </div>

    The first time it connects to a PostgreSQL server, Debezium takes a
    [consistent
    snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed
    into your Kafka topic:

    <div class="highlight">

    ``` chroma
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```

    </div>

</div>

<div id="tab-debezium-20" class="tab-pane" title="Debezium 2.0+">

1.  Beginning with Debezium 2.0.0, Confluent Schema Registry support is
    not included in the Debezium containers. To enable the Confluent
    Schema Registry for a Debezium container, install the following
    Confluent Avro converter JAR files into the Connect plugin
    directory:

    - `kafka-connect-avro-converter`
    - `kafka-connect-avro-data`
    - `kafka-avro-serializer`
    - `kafka-schema-serializer`
    - `kafka-schema-registry-client`
    - `common-config`
    - `common-utils`

    You can read more about this in the [Debezium
    documentation](https://debezium.io/documentation/reference/stable/configuration/avro.html#deploying-confluent-schema-registry-with-debezium-containers).

2.  Create a connector configuration file and save it as
    `register-postgres.json`:

    <div class="highlight">

    ``` chroma
    {
        "name": "your-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "plugin.name":"pgoutput",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname" : "postgres",
            "topic.prefix": "pg_repl",
            "schema.include.list": "public",
            "table.include.list": "public.table1",
            "publication.autocreate.mode":"filtered",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schema.registry.url": "http://<scheme-registry>:8081",
            "value.converter.schemas.enable": false
        }
    }
    ```

    </div>

    You can read more about each configuration property in the [Debezium
    documentation](https://debezium.io/documentation/reference/2.4/connectors/postgresql.html#postgresql-connector-properties).
    By default, the connector writes events for each table to a Kafka
    topic named `serverName.schemaName.tableName`.

3.  Start the Debezium Postgres connector using the configuration file:

    <div class="highlight">

    ``` chroma
    export CURRENT_HOST='<your-host>'

    curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" \
    http://$CURRENT_HOST:8083/connectors/ -d @register-postgres.json
    ```

    </div>

4.  Check that the connector is running:

    <div class="highlight">

    ``` chroma
    curl http://$CURRENT_HOST:8083/connectors/your-connector/status
    ```

    </div>

    The first time it connects to a Postgres server, Debezium takes a
    [consistent
    snapshot](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-snapshots)
    of the tables selected for replication, so you should see that the
    pre-existing records in the replicated table are initially pushed
    into your Kafka topic:

    <div class="highlight">

    ``` chroma
    /usr/bin/kafka-avro-console-consumer \
      --bootstrap-server kafka:9092 \
      --from-beginning \
      --topic pg_repl.public.table1
    ```

    </div>

</div>

</div>

</div>

### C. Create a source

<div class="note">

**NOTE:** Currently, Materialize only supports Avro-encoded Debezium
records. If you're interested in JSON support, please reach out in the
community Slack or submit a [feature
request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests).

</div>

Debezium emits change events using an envelope that contains detailed
information about upstream database operations, like the `before` and
`after` values for each record. To create a source that interprets the
[Debezium
envelope](/docs/self-managed/v25.2/sql/create-source/kafka/#using-debezium)
in Materialize:

<div class="highlight">

``` chroma
CREATE SOURCE kafka_repl
    FROM KAFKA CONNECTION kafka_connection (TOPIC 'pg_repl.public.table1')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

</div>

By default, the source will be created in the active cluster; to use a
different cluster, use the `IN CLUSTER` clause.

This allows you to replicate tables with `REPLICA IDENTITY DEFAULT`,
`INDEX`, or `FULL`.

### D. Create a view on the source

A [view](/docs/self-managed/v25.2/concepts/views/) saves a query under a
name to provide a shorthand for referencing the query. During view
creation, the underlying query is not executed.

<div class="highlight">

``` chroma
CREATE VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```

</div>

### E. Create an index on the view

In Materialize, [indexes](/docs/self-managed/v25.2/concepts/indexes) on
views compute and, as new data arrives, incrementally update view
results in memory within a
[cluster](/docs/self-managed/v25.2/concepts/clusters/) instead of
recomputing the results from scratch.

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
[Indexes](/docs/self-managed/v25.2/concepts/indexes/) and
[Views](/docs/self-managed/v25.2/concepts/views/).

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
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/ingest-data/postgres/postgres-debezium.md"
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
