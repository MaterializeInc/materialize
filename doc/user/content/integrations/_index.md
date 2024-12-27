---
title: Tools and integrations
description: "Get details about third-party tools and integrations supported by Materialize"
disable_list: true
make_table_row_headers_searchable: true
aliases:
    - /third-party/supported-tools/
    - /third-party/
    - /third-party/postgres-cloud/
    - /guides/postgres-cloud/
    - /guides/

menu:
  main:
    parent: integrations
    name: "Overview"
    weight: 5

---

[//]: # "TODO(morsapaes) Once database-issues#2562 lands, link the page here"

Materialize is **wire-compatible** with PostgreSQL, which means it integrates
with many SQL clients and other tools in the data ecosystem that support
PostgreSQL. This page describes the status, level of support, and usage notes
for commonly used and requested Materialize integrations and tools.

{{< note >}}
If there's a tool that you'd like to use with Materialize but is not listed, let us know by submitting a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)!

For listed tools that are not yet production-ready, you can register your interest by clicking the **Notify Me** button. This helps us prioritize integration work, and follow up with you once the support level for the tool has evolved.
{{</ note >}}

| Support level                                               | Description                                                                                                                                                                                                                                                       |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| {{< supportLevel production >}} <a name="production"></a>   | We are committed to prioritizing bugs in the interaction between these tools and Materialize.                                                                                                                                                                     |
| {{< supportLevel beta >}} <a name="beta"></a>               | There may be small performance issues and minor missing features, but Materialize supports the major use cases for this tool. We can't guarantee [bug reports or feature requests](https://github.com/MaterializeInc/materialize/discussions) will be prioritized. |
| {{< supportLevel alpha >}} <a name="alpha"></a>             | Some of our community members have made this integration work, but we haven’t tested it internally and can’t vouch for its stability.                                                                                                                             |
| {{< supportLevel in-progress >}} <a name="in-progress"></a> | **There are known issues** preventing the integration from working, but we are actively developing features that unblock the integration.                                                                                                                         |
| {{< supportLevel researching >}} <a name="researching"></a> | **There are known issues** preventing the integration from working, and we are gathering user feedback and gauging interest in supporting these integrations.                                                                                                     |

## Message brokers

### Kafka

Kafka is supported as a [**source**](/concepts/sources), with features like **upserts** and **Debezium** CDC, and as a [**sink**](/concepts/sinks) with **exactly-once** semantics.

| Service                               | Support level                   | Notes                                                                                                                                                                                                                                                                                                                                                      |             |
| ------------------------------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| Apache Kafka                          | {{< supportLevel production >}} | See the [source](/sql/create-source/kafka/) and [sink](/sql/create-sink/kafka) documentation for more details.                                                                                                                                                                                                                                      |             |
| Confluent Cloud                       | {{< supportLevel production >}} | Use SASL authentication to securely connect to a Confluent Cloud cluster. See the [Confluent Cloud guide](/integrations/confluent-cloud) for a step-by-step breakdown of the integration.                                                                                                                   |             |
| Amazon MSK (Managed Streaming for Apache Kafka) | {{< supportLevel production >}} | See the [source documentation](/sql/create-source/kafka/) for more details, and the [Amazon MSK guide](/integrations/aws-msk/) for a step-by-step breakdown of the integration.                                                                                                                                                                               |             |
| Heroku Kafka                          | {{< supportLevel alpha >}}      | Use SSL authentication and the Heroku-provided provided keys and certificates for security, and the `KAFKA_URL` as the broker address, after removing `kafka+ssl://`. |
| WarpStream                            | {{< supportLevel beta >}}       | See the [WarpStream guide](/integrations/warpstream/) for a step-by-step breakdown of the integration.                                                                                                                                                                                                                                                    |             |

### Redpanda

Being Kafka API-compatible, Redpanda is supported as a [**source**](/concepts/sources)
and as a [**sink**](/concepts/sinks) at the same level and with the same
features as Kafka.

| Service        | Support level             | Notes                                                                                                                                                                                                                                                                                                                                 |             |
| -------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| Redpanda       | {{< supportLevel production >}} | See the [source](/sql/create-source/kafka/) and [sink](/sql/create-sink/kafka) documentation for more details.                                                                                                                                                                                                                 | [](#notify) |
| Redpanda Cloud | {{< supportLevel production >}} | Use SASL authentication to securely connect to Redpanda Cloud clusters. See the [Redpanda documentation](https://docs.redpanda.com/docs/security/acls/#acls) for more details, and the [Redpanda Cloud guide](/integrations/redpanda-cloud/) for a step-by-step breakdown of the integration. | [](#notify) |

### Kinesis Data Streams

| Service                  | Support level                    | Notes                                           |             |
| ------------------------ | -------------------------------- | ----------------------------------------------- | ----------- |
| AWS Kinesis Data Streams | {{< supportLevel researching >}} | Subscribe via “Notify Me” to register interest. | [](#notify) |

### Other message brokers

| Service          | Support level                    | Notes                                                                                                                                                                                                                                                                                                           |             |
| ---------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| GCP Pub/Sub      | {{< supportLevel researching >}} | Integration requires implementing a new connector.                                                                                                                                                                                                                                              | [](#notify) |
| Azure Event Hubs | {{< supportLevel researching >}} | Integration requires implementing a new connector. <br> Event Hubs provides a [Kafka-compatible endpoint](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview) that may enable interoperability with Materialize, but this hasn't been officially tested. | [](#notify) |
| Apache Pulsar    | {{< supportLevel researching >}} | Integration requires implementing a new connector. <br> Pulsar provides a [Kafka-compatible wrapper](https://pulsar.apache.org/docs/en/adaptors-kafka/) that may enable interoperability with Materialize, but this hasn't been officially tested.                                              | [](#notify) |

👋 _Is there another message broker you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

## Databases

Materialize can efficiently maintain real-time materialized views on top
of **Change Data Capture (CDC)** data originating from a database, either by
directly consuming its replication stream or via [Debezium](/integrations/debezium/).

### PostgreSQL

PostgreSQL 11+ is supported as a [**source**](/concepts/sources), both through
the [direct PostgreSQL source](/sql/create-source/postgres/) and through
[Debezium](/integrations/debezium/) (via Kafka or other Kafka API-compatible
broker). Using a PostgreSQL instance as a source requires enabling **logical
replication**.

| Service                         | Support level                    | Notes                                                                                                                                                                                                                                                                                         |             |
| ------------------------------- | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| PostgreSQL _(direct)_           | {{< supportLevel production >}}        | See the [source documentation](/sql/create-source/postgres/) for more details, and the relevant integration guide for step-by-step instructions:<p></p><ul><li>[Amazon RDS for PostgreSQL](/ingest-data/postgres-amazon-rds)</li><li>[Amazon Aurora for PostgreSQL](/ingest-data/postgres-amazon-aurora)</li><li>[Azure DB for PostgreSQL](/ingest-data/postgres-azure-db)</li><li>[Google Cloud SQL for PostgreSQL](/ingest-data/postgres-google-cloud-sql)</li><li>[AlloyDB for PostgreSQL](/ingest-data/postgres-alloydb)</li><li>[Self-hosted PostgreSQL](/ingest-data/postgres-self-hosted)</li></ul> |
| PostgreSQL _(via Debezium)_     | {{< supportLevel production >}}  | See the [PostgreSQL guide](/ingest-data/cdc-postgres-kafka-debezium/) for a step-by-step breakdown of the integration. |

### MySQL

MySQL 5.7+ is supported as a [**source**](/concepts/sources) both through the
[direct MySQL source](/sql/create-source/mysql/) and through [Debezium](/integrations/debezium/)
(via Kafka or other Kafka API-compatible broker). Using a MySQL database as a
source requires enabling [**GTID-based binlog replication**](/sql/create-source/mysql/#change-data-capture).

| Service                | Support level                    | Notes                                                                                                |             |
| ---------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------- |
| MySQL _(direct)_       | {{< supportLevel production >}} | See the [source documentation](/sql/create-source/mysql/) for more details, and the relevant integration guide for step-by-step instructions:<p></p><ul><li>[Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds)</li><li>[Amazon Aurora for MySQL](/ingest-data/mysql/amazon-aurora)</li><li>[Azure DB for MySQL](/ingest-data/mysql/azure-db)</li><li>[Google Cloud SQL for MySQL](/ingest-data/mysql/google-cloud-sql)</li><li>[Self-hosted MySQL](/ingest-data/mysql/self-hosted)</li></ul> |
| MySQL _(via Debezium)_ | {{< supportLevel production >}}  | See the [MySQL CDC guide](/integrations/cdc-mysql/) for a step-by-step breakdown of the integration. |             |

### CockroachDB

CockroachDB is supported as a [**source**](/concepts/sources) through
[Changefeeds](https://www.cockroachlabs.com/docs/stable/create-and-configure-changefeeds?)
(via Kafka or other Kafka API-compatible broker).

| Service                | Support level                    | Notes                                                                                                |             |
| ---------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------- | ----------- |
| CockroachDB _(via Changefeeds)_ | {{< supportLevel production >}}  | See the [CockroachDB CDC guide](/ingest-data/cdc-cockroachdb/) for a step-by-step breakdown of the integration. |             |

### Other databases

{{< note >}}
OLAP databases like Snowflake, BigQuery or Redshift aren't a good fit as a data source in Materialize, as these are typically _consumers_ and not _producers_ of change events.
{{</ note >}}

Debezium has an extensive ecosystem of connectors, but each database has its own quirks. Materialize expects a specific message structure that includes the row data `before` and `after` the change event, which is **not guaranteed** for every connector.

| Service                     | Support level                    | Notes                                                                                                                                                                                                                                                                                                                                                                                                                        |             |
| --------------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| MongoDB _(via Debezium)_    | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest.                                                                                                                                                                                                                                                                                                                                           | [](#notify) |
| SQL Server _(via Debezium)_ | {{< supportLevel alpha >}}       | Supported with known limitations. See the [SQL Server CDC guide](/integrations/cdc-sql-server/) for a step-by-step breakdown of the integration.                                                                                                                                                                                                                                                             | [](#notify) |

👋 _Is there another database you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

## Object storage services

### S3

| Service              | Support level                    | Notes                                                                                                                                                                                                            |             |
| -------------------- | -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| Amazon S3            | {{< supportLevel production >}} | Supported as a _one-shot_ sink. See the [Amazon S3 integration guide](/serve-results/s3/) for a step-by-step breakdown of the integration. |

### Other object storage services

| Service            | Support level                    | Notes                                                                                                               |             |
| ------------------ | -------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ----------- |
| GCP Cloud Storage  | {{< supportLevel researching >}} | Integration requires implementing a new connector. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Azure Blob Storage | {{< supportLevel researching >}} | Integration requires implementing a new connector. Subscribe via "Notify Me" to register interest.                  | [](#notify) |

👋 _Is there another object storage tool you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

## Database and infrastructure management tools

### dbt

Materialize integrates with dbt through the [`dbt-materialize`](https://github.com/MaterializeInc/materialize/tree/main/misc/dbt-materialize) adapter. The adapter implements custom materializations and macros to **build**, **run** and **version-control** models that transform streaming data in real time.

| Service   | Support level                    | Notes                                                                                                                                                                                                                          |             |
| --------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| dbt Core  | {{< supportLevel beta >}}        | See the [`dbt-materialize` reference documentation](https://materialize.com/docs/manage/dbt/) for more details, and the [development workflows guide](/integrations/dbt/) for common dbt patterns. | [](#notify) |
| dbt Cloud | {{< supportLevel in-progress >}} | Not supported yet. We are working with the dbt community to bring native Materialize support to dbt Cloud soon.                                                                                                                | [](#notify) |

### Terraform

Materialize maintains a
[Terraform provider](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs) to help you safely and predictably provision and manage connections, sources, and other database objects.

Materialize also maintains several [Terraform modules](https://registry.terraform.io/namespaces/MaterializeInc) to help manage your other
cloud resources. Modules allow you to bypass manually configuring cloud
resources and are an efficient way of deploying essential infrastructure for
your organization.

{{< note >}}
While Materialize offers support for its provider, Materialize does not offer
support for these modules.
{{</ note >}}

### SQL clients

| Service      | Support level                    | Notes                                                                                                                                                                                                                                                                             |             |
| ------------ | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| psql         | {{< supportLevel production >}}  | See [SQL Clients](/integrations/sql-clients/#psql) for more details. Some backslash meta-commands are not yet supported.
| DBeaver      | {{< supportLevel production >}}  | Connect using the [Materialize database driver](/integrations/sql-clients/#dbeaver). See [SQL Clients](/integrations/sql-clients/#dbeaver) for more details.                   |
| DataGrip IDE | {{< supportLevel beta >}}        | Connect using the [PostgreSQL database driver](https://www.jetbrains.com/datagrip/features/postgresql/). See [SQL Clients](/integrations/sql-clients/#datagrip) for more details.
| pgAdmin      | {{< supportLevel in-progress >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| TablePlus    | {{< supportLevel alpha >}}       | Connect using the [PostgreSQL database driver](https://tableplus.com/blog/2019/09/jdbc-connection-strings.html). See [SQL Clients](/integrations/sql-clients/#tableplus) for more details.
| VSCode       | {{< supportLevel production >}}  | Connect using the [Materialize extension for VS Code](https://github.com/MaterializeInc/vscode-extension).

👋 _Is there another SQL client you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

### Monitoring

| Service      | Support level                    | Notes                                                                                                                                                                                                                                                                             |             |
| ------------ | -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| Datadog      | {{< supportLevel production >}}  | See the [Datadog guide](/manage/monitor/datadog/) for a step-by-step breakdown of the integration.
| Grafana      | {{< supportLevel production >}}  | See the [Grafana guide](/manage/monitor/grafana/) for a step-by-step breakdown of the integration.                                                                                                                                                                                              |             |

👋 _Is there another SQL client you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

## Client libraries and ORMs

Applications can use any common language-specific PostgreSQL drivers and PostgreSQL-compatible ORMs to interact with Materialize and **create relations**, **execute queries** and **stream out results**.

{{< note >}}
Client libraries and ORM frameworks tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if PostgreSQL is supported, it's **not guaranteed** that the same integration will work out-of-the-box.
{{</ note >}}

### Client libraries {#libraries-and-drivers}

| Language | Support level                   | Tested drivers                                                  | Notes                                                 |
| -------- | ------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------- |
| Go       | {{< supportLevel production >}} | [`pgx`](https://github.com/jackc/pgx)                           | See the [Go cheatsheet](/integrations/golang/).       |
| Java     | {{< supportLevel production >}} | [PostgreSQL JDBC driver](https://jdbc.postgresql.org/)          | See the [Java cheatsheet](/integrations/java-jdbc/).  |
| Node.js  | {{< supportLevel production >}} | [`node-postgres`](https://node-postgres.com/)                   | See the [Node.js cheatsheet](/integrations/node-js/). |
| PHP      | {{< supportLevel production >}} | [`pdo_pgsql`](https://www.php.net/manual/en/ref.pgsql.php)      | See the [PHP cheatsheet](/integrations/php/).         |
| Python   | {{< supportLevel production >}} | [`psycopg2`](https://pypi.org/project/psycopg2/)                | See the [Python cheatsheet](/integrations/python/).   |
| Ruby     | {{< supportLevel production >}} | [`pg` gem](https://rubygems.org/gems/pg/)                       | See the [Ruby cheatsheet](/integrations/ruby/).       |
| Rust     | {{< supportLevel production >}} | [`postgres-openssl`](https://crates.io/crates/postgres-openssl) | See the [Rust cheatsheet](/integrations/rust/).       |

### ORM frameworks

| Framework          | Support level                    | Notes                                                              |             |
| ------------------ | -------------------------------- | ------------------------------------------------------------------ | ----------- |
| Rails ActiveRecord | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Prisma             | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |

👋 _Is there another client library or ORM framework you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

## Other tools

As a rule of thumb: if a tool has support for PostgreSQL, it _might_ work out-of-the-box with Materialize using the native **PostgreSQL connector**. This will depend on the complexity of the queries each tool runs under the hood, and the configuration settings, system tables and features involved.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize and join efforts with each community to make the integrations Just Work™️.

### Data integration

| Service       | Support level                    | Notes                                                                              |             |
| ------------- | -------------------------------- | ---------------------------------------------------------------------------------- | ----------- |
| Fivetran      | {{< supportLevel beta >}} | See the [Fivetran guide](/integrations/fivetran/) for a step-by-step breakdown of the integration |             |
| Stitch        | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest.                 | [](#notify) |
| Meltano       | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest.                 | [](#notify) |
| Airbyte       | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest.                 | [](#notify) |
| Striim Cloud  | {{< supportLevel beta >}}        | See the [Striim Cloud guide](/integrations/striim/) for a step-by-step breakdown of the integration         |             |

### Business Intelligence (BI)

| Service            | Support level              | Notes                                                                                                                                                                                                                                                                                               |             |
| ------------------ | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| Metabase           | {{< supportLevel beta >}}  | Connect using the [PostgreSQL database driver](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html). See the [Metabase integration page](/integrations/metabase/) for more details.                                                                                 | [](#notify) |
| Superset           | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://superset.apache.org/docs/databases/postgres/).                                                                                                                                                                                               | [](#notify) |
| Preset             | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://superset.apache.org/docs/databases/postgres/).                                                                                                                                                                                               | [](#notify) |
| Looker             | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://cloud.google.com/looker/docs/db-config-postgresql). See the [Looker integration page](/integrations/looker/) for more details.                                                                                                               | [](#notify) |
| Google Data Studio | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://support.google.com/datastudio/answer/7288010?hl#how-to-connect-to-postgresql&zippy=%2Cin-this-article).                                                                                                                                      | [](#notify) |
| Tableau            | {{< supportLevel alpha >}} | Connect using the [JDBC driver for PostgreSQL](https://help.tableau.com/current/pro/desktop/en-us/examples_postgresql.htm). See the [Tableau integration page](/integrations/tableau/) for more details.                                                                                            | [](#notify) |
| Microsoft Power BI | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://learn.microsoft.com/en-us/power-query/connectors/postgresql). See the [Power BI integration page](/integrations/power-bi/) for more details.                                                                                                 | [](#notify) |

### Headless BI

| Service | Support level              | Notes                                                                           |             |
| ------- | -------------------------- | ------------------------------------------------------------------------------- | ----------- |
| Cube.js | {{< supportLevel alpha >}} | Connect using the [Materialize driver](https://cube.dev/docs/config/databases). See the [Cube guide](/integrations/cube) for a step-by-step breakdown of the integration. | [](#notify) |

### Reverse ETL

| Service   | Support level                    | Notes                                                                               |             |
| --------- | -------------------------------- | ----------------------------------------------------------------------------------- | ----------- |
| Census    | {{< supportLevel alpha >}}       | Connect using the [Materialize source](https://docs.getcensus.com/sources/materialize). See the [Census integration page](/manage/reverse-etl/census/) for more details.                 | |
| Hightouch | {{< supportLevel in-progress >}} | Connect using the [Materialize source](https://hightouch.com/integrations/sources/materialize). | [](#notify) |

### Data collaboration

| Service | Support level              | Notes                                                                                                                                |             |
| ------- | -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------- |
| Deepnote   | {{< supportLevel production >}} | Connect using the [Materialize connection](https://deepnote.com/docs/materialize). See the [Deepnote integration page](https://materialize.com/docs/serve-results/deepnote/).                            |
| Hex     | {{< supportLevel beta >}}  | Connect using the [Materialize connection](https://learn.hex.tech/docs/connect-to-data/data-connections/overview). |                 |
| Retool  | {{< supportLevel alpha >}} | Connect using the [PostgreSQL integration](https://retool.com/integrations/postgresql).                                              | [](#notify) |

👋 _Is there another tool you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests&labels=A-integration)._

<div id="subscribe_dialog">
  <form name="notify">
    <input name="email" type="email" placeholder="Email Address" required="required"/>
    <input type="submit" class="notify_button btn-ghost" value="Confirm" />
  </form>
  <div class="disclaimer">
    <em>Subscribe to receive an email when support status is upgraded. No spam!</em>
  </div>
</div>

<style>
    td { min-width: 125px;}
</style>

<script>
$(function() {
    analytics.on('page', function() {
        $('a[href="#notify"]').replaceWith('<button class="notify_button btn-ghost" data-js="subscribe_open" title="Get notified when support is upgraded.">Notify Me</button>')

        var removeForms = function() {
            $('.subscribe_dialog_active').removeClass('subscribe_dialog_active');
        };

        $("[data-js='subscribe_open']").on("click", function() {
            !$(this).hasClass('success') &&
                $(this).parent()
                .addClass('subscribe_dialog_active')
                .append($('#subscribe_dialog'));
            return false;
        });

        $("#subscribe_dialog").submit(function(e) {
            var email = $(this).find('[name="email"]').val();
            $(this).siblings("[data-js='subscribe_open']")
                .addClass('success')
                .attr('title', 'Subscribed')
                .text('✅');
            removeForms();
            window.analytics && window.analytics.track("Integration Status Subscribed", {
                email: email,
                subject: $(this).parents("tr").find('td').first().text()
            });
            e.preventDefault();
        });

        $("body").on("click", function(e) {
            if (!$(e.target).closest('#subscribe_dialog').length) {
                removeForms();
            }
        });
    });
});
</script>
