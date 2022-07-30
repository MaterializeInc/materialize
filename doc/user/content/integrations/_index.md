---
title: Tools and integrations
description: "Get details about third-party tools and integrations supported by Materialize"
disable_list: true
aliases:
  - /third-party/supported-tools/
  - /third-party/
  - /third-party/postgres-cloud/
  - /guides/postgres-cloud/
  - /guides/
---

[//]: # "TODO(morsapaes) Once #8396 lands, link the page here"

Materialize is **wire-compatible** with PostgreSQL, which means it integrates with most client libraries, ORM frameworks and other third-party tools that support PostgreSQL. This page describes the status, level of support, and usage notes for commonly used and requested Materialize integrations and tools.

{{< note >}}
If there's a tool that you'd like to use with Materialize but is not listed, let us know by submitting a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)!

For listed tools that are not yet production-ready, you can register your interest by clicking the **Notify Me** button. This helps us prioritize integration work, and follow up with you once the support level for the tool has evolved.
{{</ note >}}

| Support level | Description |
| ------------- | ------- |
| {{< supportLevel production >}} <a name="production"></a> | We are committed to prioritizing bugs in the interaction between these tools and Materialize. |
| {{< supportLevel beta >}} <a name="beta"></a> | There may be small performance issues and minor missing features, but Materialize supports the major use cases for this tool. We can't guarantee  [bug reports or feature requests](https://github.com/MaterializeInc/materialize/issues/new) will be prioritized. |
| {{< supportLevel alpha >}} <a name="alpha"></a> | Some of our community members have made this integration work, but we haven’t tested it internally and can’t vouch for its stability. |
| {{< supportLevel in-progress >}} <a name="in-progress"></a> | **There are known issues** preventing the integration from working, but we are actively developing features that unblock the integration. |
| {{< supportLevel researching >}} <a name="researching"></a> | **There are known issues** preventing the integration from working, and we are gathering user feedback and gauging interest in supporting these integrations. |

## Message brokers

### Kafka

Kafka is supported as a [**source**](/overview/key-concepts/#sources), with features like **upserts** and **Debezium** CDC.

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Apache Kafka | {{< supportLevel production >}} | See the [source](/sql/create-source/kafka/) documentation for more details. |  |
| Confluent Cloud Kafka | {{< supportLevel production >}} | Use [`SASL/PLAIN` authentication](/sql/create-connection/#kafka-sasl), to securely connect to a Confluent Cloud cluster. See the [source](/sql/create-source/kafka/) documentation for more details. |  |
| AWS MSK (Managed Streaming for Kafka) | {{< supportLevel production >}} | See the [source documentation](/sql/create-source/kafka/) for more details, and the [AWS MSK guide](/integrations/aws-msk/) for a step-by-step breakdown of the integration.  |  |
| Heroku Kafka | {{< supportLevel alpha >}} | Use [`SSL` authentication](/sql/create-connection/#kafka-ssl) and the Heroku-provided provided keys and certificates for security, and the `KAFKA_URL` as the broker address (replacing `kafka+ssl://` with `ssl://`). | [](#notify) |

### Redpanda

Being Kafka API-compatible, Redpanda is supported as a [**source**](/overview/key-concepts/#sources) at the same level and with the same features as Kafka.

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Redpanda | {{< supportLevel beta >}} | See the [source](/sql/create-source/kafka/) and documentation for more details. | [](#notify) |
| Redpanda Cloud | {{< supportLevel beta >}} | Use [`SASL` authentication](/sql/create-connection/#kafka-sasl) to securely connect to Redpanda Cloud clusters. See the [Redpanda documentation](https://docs.redpanda.com/docs/security/acls/#acls) for more details. | [](#notify) |

### Kinesis Data Streams

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| AWS Kinesis Data Streams | {{< supportLevel in-progress >}} | Subscribe via “Notify Me” to register interest. | [](#notify) |

### Other message brokers

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| GCP Pub/Sub | {{< supportLevel researching >}} | Integration requires implementing a new connector {{% gh 2678 %}}. | [](#notify) |
| Azure Event Hubs | {{< supportLevel researching >}} | Integration requires implementing a new connector {{% gh 2851 %}}. <br> Event Hubs provides a [Kafka-compatible endpoint](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview) that may enable interoperability with Materialize, but this hasn't been officially tested. | [](#notify) |
| Apache Pulsar | {{< supportLevel researching >}} | Integration requires implementing a new connector {{% gh 3517 %}}. <br> Pulsar provides a [Kafka-compatible wrapper](https://pulsar.apache.org/docs/en/adaptors-kafka/) that may enable interoperability with Materialize, but this hasn't been officially tested. | [](#notify) |

👋 _Is there another message broker you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Databases

Materialize can efficiently maintain real-time materialized views on top of **Change Data Capture (CDC)** data originating from a database, either by directly consuming its replication stream or via [Debezium](/integrations/debezium/).

### PostgreSQL

Postgres is supported as a [**source**](/overview/key-concepts/#sources), both through the [direct PostgreSQL source](/sql/create-source/postgres/) and through [Debezium](/integrations/debezium/) (via Kafka or Redpanda). Using a PostgreSQL instance as a source requires enabling **logical replication**.

| Service | Support level| Notes |  |
| --- | --- | --- | --- |
| PostgreSQL _(direct)_ | {{< supportLevel beta >}} | See the [source documentation](/sql/create-source/postgres/) for more details, and the [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source) for a step-by-step breakdown of the integration. | [](#notify) |
| PostgreSQL _(via Debezium)_ | {{< supportLevel production >}} | See the [PostgreSQL CDC guide](/integrations/cdc-postgres/#kafka--debezium) for a step-by-step breakdown of the integration. | [](#notify) |
| Amazon RDS for PostgreSQL | {{< supportLevel beta >}} | See the [Amazon RDS CDC guide](/integrations/aws-rds/) for a step-by-step breakdown of the integration. | [](#notify) |
| GCP Cloud SQL for PostgreSQL | {{< supportLevel beta >}} | See the [GCP Cloud SQL CDC guide](/integrations/gcp-cloud-sql/) for a step-by-step breakdown of the integration. | [](#notify) |
| Amazon Aurora | {{< supportLevel beta >}} | See the [Amazon Aurora CDC guide](/integrations/aws-aurora/) for a step-by-step breakdown of the integration. | [](#notify) |
| Amazon Aurora Serverless | {{< supportLevel researching >}} | Aurora serverless v1 does [not support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) logical replication, which is required for PostgreSQL CDC. | [](#notify) |
| Heroku Postgres | {{< supportLevel researching >}} | Heroku Postgres does not support logical replication, which is required for PostgreSQL CDC. Heroku provides [Heroku Data Connectors](https://devcenter.heroku.com/articles/heroku-data-connectors) that may enable interoperability with Materialize, but this hasn't been officially tested. | [](#notify) |
| Azure Database for PostgreSQL | {{< supportLevel beta >}} | See the [Azure Database CDC guide](/integrations/azure-postgres/) for a step-by-step breakdown of the integration. | [](#notify) |
| DigitalOcean Managed PostgreSQL | {{< supportLevel beta >}} | See the [DO Managed PostgreSQL guide](/integrations/digitalocean-postgres/) for a step-by-step breakdown of the integration. | [](#notify) |
| CrunchyBridge Postgres| {{< supportLevel beta >}} | Logical replication is enabled by default. See the [PostgreSQL CDC guide](/integrations/cdc-postgres/#direct-postgres-source) for a step-by-step breakdown of the integration. | [](#notify) ||

### MySQL

MySQL is indirectly supported as a [**source**](/overview/key-concepts/#sources) through [Debezium](/integrations/debezium/) (via Kafka or Redpanda). Using a MySQL instance as a source requires enabling **row-based replication**.

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| MySQL _(direct)_ | {{< supportLevel researching >}} | Not supported yet {{% gh 8078 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| MySQL _(via Debezium)_ | {{< supportLevel production >}} | See the [MySQL CDC guide](/integrations/cdc-mysql/) for a step-by-step breakdown of the integration. | |

### Other databases

{{< note >}}
OLAP databases like Snowflake, BigQuery or Redshift aren't a good fit as a data source in Materialize, as these are typically _consumers_ and not _producers_ of change events.
{{</ note >}}

Debezium has an extensive ecosystem of connectors, but each database has its own quirks. Materialize expects a specific message structure that includes the row data `before` and `after` the change event, which is **not guaranteed** for every connector.

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| MongoDB _(via Debezium)_ | {{< supportLevel researching >}} | Not supported yet {{% gh 7289 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| SQL Server _(via Debezium)_ | {{< supportLevel alpha >}} | Supported with [known limitations](https://materializecommunity.slack.com/archives/C0157GZ7UKF/p1646142131506359) unrelated to Materialize. See the [Debezium documentation](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html) for details on how to configure a SQL Server instance for [CDC](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html#sqlserver-overview). | [](#notify) |

👋 _Is there another database you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Object storage services

### S3

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Amazon S3 | {{< supportLevel in-progress >}} | Subscribe via “Notify Me” to register interest. | [](#notify) |
| MinIO Object Storage | {{< supportLevel researching >}} | Not supported yet {{% gh 6568 %}}. <br> MinIO provides a [S3-compatible API](https://min.io/product/s3-compatibility) that may enable interoperability with Materialize, but this hasn't been officially tested. | [](#notify) |

### Other object storage services

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| GCP Cloud Storage | {{< supportLevel researching >}} | Integration requires implementing a new connector {{% gh 10349 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Azure Blob Storage | {{< supportLevel researching >}} | Integration requires implementing a new connector. Subscribe via "Notify Me" to register interest. | [](#notify) |

👋 _Is there another object storage tool you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Database management tools

### dbt

Materialize integrates with dbt through the [`dbt-materialize`](https://github.com/MaterializeInc/materialize/tree/main/misc/dbt-materialize) adapter. The adapter implements custom materializations and macros to **build**, **run** and **version-control** models that transform streaming data in real time.

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| dbt Core | {{< supportLevel beta >}} | See the [dbt documentation](https://docs.getdbt.com/reference/warehouse-profiles/materialize-profile) for more details, and the [dbt + Materialize guide](/integrations/dbt/) for a step-by-step breakdown of the integration. | [](#notify) |
| dbt Cloud | {{< supportLevel in-progress >}} | Not supported yet. We are working with the dbt community to bring native Materialize support to dbt Cloud soon. | [](#notify) |

### SQL clients

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| DBeaver | {{< supportLevel production >}} | Connect using the [PostgreSQL database driver](https://hevodata.com/learn/dbeaver-postgresql/#a5). |  |
| DataGrip IDE | {{< supportLevel in-progress >}} | Not supported yet {{% gh 9720 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| pgAdmin | {{< supportLevel in-progress >}} | Not supported yet {{% gh 5874 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| TablePlus | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://tableplus.com/blog/2019/09/jdbc-connection-strings.html). | [](#notify) |

👋 _Is there another SQL client you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._


## Client libraries and ORMs

Applications can use any common language-specific PostgreSQL drivers and PostgreSQL-compatible ORMs to interact with Materialize and **create relations**, **execute queries** and **stream out results**.

{{< note >}}
Client libraries and ORM frameworks tend to run complex introspection queries that may use configuration settings, system tables or features not yet implemented in Materialize. This means that even if PostgreSQL is supported, it's **not guaranteed** that the same integration will work out-of-the-box.
{{</ note >}}

### Client libraries {#libraries-and-drivers}

| Language | Support level | Tested drivers | Notes |
| --- | --- | --- | --- |
| Go | {{< supportLevel production >}} | [`pgx`](https://github.com/jackc/pgx) | See the [Go cheatsheet](/integrations/golang/). |
| Java | {{< supportLevel production >}} | [PostgreSQL JDBC driver](https://jdbc.postgresql.org/) | See the [Java cheatsheet](/integrations/java-jdbc/). |
| Node.js | {{< supportLevel production >}} | [`node-postgres`](https://node-postgres.com/) | See the [Node.js cheatsheet](/integrations/node-js/). |
| PHP | {{< supportLevel production >}} | [`pdo_pgsql`](https://www.php.net/manual/en/ref.pgsql.php) | See the [PHP cheatsheet](/integrations/php/). |
| Python | {{< supportLevel production >}} |[`psycopg2`](https://pypi.org/project/psycopg2/) | See the [Python cheatsheet](/integrations/python/). |
| Ruby | {{< supportLevel production >}} | [`pg` gem](https://rubygems.org/gems/pg/) | See the [Ruby cheatsheet](/integrations/ruby/). |

### ORM frameworks

| Framework | Support level | Notes |  |
| --- | --- | --- | --- |
| Rails ActiveRecord | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Prisma | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |

👋 _Is there another client library or ORM framework you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Other tools

As a rule of thumb: if a tool has support for PostgreSQL, it _might_ work out-of-the-box with Materialize using the native **PostgreSQL connector**. This will depend on the complexity of the queries each tool runs under the hood, and the configuration settings, system tables and features involved.

The level of support for these tools will improve as we extend the coverage of `pg_catalog` in Materialize {{% gh 2157 %}} and join efforts with each community to make the integrations Just Work™️.

### Data integration

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| FiveTran | {{< supportLevel researching >}} | Not supported yet {{% gh 5188 %}}. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Stitch | {{< supportLevel researching >}}   | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Meltano | {{< supportLevel researching >}}  | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Airbyte | {{< supportLevel researching >}}  | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |

### Business Intelligence (BI)

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Metabase | {{< supportLevel beta >}} | Connect using the [PostgreSQL database driver](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql.html). See the [Metabase integration page](/integrations/metabase/) for more details. | [](#notify) |
| Superset | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://superset.apache.org/docs/databases/postgres/).  | [](#notify) |
| Preset | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://superset.apache.org/docs/databases/postgres/). | [](#notify) |
| Looker | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://superset.apache.org/docs/databases/postgres/). | [](#notify) |
| Google Data Studio | {{< supportLevel alpha >}} | Connect using the [PostgreSQL database driver](https://support.google.com/datastudio/answer/7288010?hl#how-to-connect-to-postgresql&zippy=%2Cin-this-article). | [](#notify) |
| Tableau | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Microsoft Power BI | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Mode Analytics | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |

### Headless BI

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Cube.js | {{< supportLevel alpha >}} | Connect using the [Materialize driver](https://cube.dev/docs/config/databases). | [](#notify) |

### Reverse ETL

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Census | {{< supportLevel researching >}} | Not supported yet. Subscribe via "Notify Me" to register interest. | [](#notify) |
| Hightouch | {{< supportLevel in-progress >}} | Connect using a [PostgreSQL source](https://hightouch.io/docs/sources/postgresql/). | [](#notify) |

### Data collaboration

| Service | Support level | Notes |  |
| --- | --- | --- | --- |
| Hex | {{< supportLevel alpha >}} | Connect using a [PostgreSQL connection](https://learn.hex.tech/docs/connect-to-data/data-connections/overview#add-a-new-connection). | [](#notify) |
| Retool | {{< supportLevel alpha >}} | Connect using the [PostgreSQL integration](https://retool.com/integrations/postgresql). | [](#notify) |

👋 _Is there another tool you'd like to use with Materialize? Submit a [feature request](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

<div id="subscribe_dialog">
  <form name="notify">
    <input name="email" type="email" placeholder="Email Address" required="required"/>
    <input type="submit" class="notify_button" value="Confirm" />
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
        $('a[href="#notify"]').replaceWith('<button class="notify_button" data-js="subscribe_open" title="Get notified when support is upgraded.">Notify Me</button>')

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
                .text('✔️');
            removeForms();
            window.analytics && window.analytics.identify(email);
            window.analytics && window.analytics.track("Integration Status Subscribed", {
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
