---
title: Tools and Integrations
description: "Get details about third-party tools and integrations support with Materialize"
menu:
  main:
    parent: 'third-party'
    name: Overview
weight: 1
---

The status, level of support, and usage notes for commonly used and requested Materialize integrations and tools are listed below.

_How to use the information on this page:_

| Support Level | Meaning |
| ------------- | ------- |
| {{< supportLevel production >}} <a name="production"></a> | We are committed to prioritizing bugs in the interaction between these tools and Materialize. |
| {{< supportLevel beta >}} <a name="beta"></a> | There may be small performance issues and minor missing features, but Materialize supports the major use cases for this tool. We can't guarantee  [bug reports or feature requests](https://github.com/MaterializeInc/materialize/issues/new) will be prioritized. |
| {{< supportLevel alpha >}} <a name="alpha"></a> | Some of our community members have made this integration work, but we haven’t tested it ourselves and can’t guarantee its stability. |
| {{< supportLevel in-development >}} <a name="in-development"></a> | **There are known issues** preventing the integration from working, but we are actively developing features that unblock the integration. |
| {{< supportLevel researching >}} <a name="researching"></a> | **There are known issues** preventing the integration from working, but we are gathering user feedback and gauging interest in supporting these integrations. |

## Message Brokers

### Kafka

Kafka is supported in Materialize as a [`SOURCE`](/sql/create-source/) of input data, and as a [`SINK`](/sql/create-sink/), where Materialize produces data *(in the form of change events from a Materialized view)* back out to Kafka.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Apache Kafka | {{< supportLevel production >}} | Kafka is supported in a variety of [configuration](/sql/create-source/kafka/#with-options) and [security](/sql/create-source/kafka/#authentication) options. | [More Info](/sql/create-source/kafka/) |
| Confluent Cloud Kafka | {{< supportLevel production >}} | Use SASL authentication, see [example here](/sql/create-source/kafka/#saslplain). The same config can be used to produce messages to Confluent Kafka via a [SINK](/sql/create-sink/). |  |
| AWS MSK (Managed Streaming for Kafka) | {{< supportLevel production >}} | Use SASL/SCRAM Authentication to securely connect to MSK clusters. [MSK SASL Docs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html) *(mTLS connections coming soon.)* |  |
| Redpanda | {{< supportLevel beta >}} | Repdanda works as a Kafka Source and Sink in Materialize. See [using Redpanda with Materialize](/third-party/redpanda/) for instructions and limitations. | [More Info](/third-party/redpanda/) [](#notify) |
| Heroku Kafka | {{< supportLevel alpha >}} | Use [SSL Authentication](https://materialize.com/docs/sql/create-source/kafka/#ssl) and the Heroku-provided certificates and keys for security. Use Heroku-provided `KAFKA_URL` for broker addresses (replace `kafka+ssl://` with `ssl://`). Heroku disables topic creation, [preventing SINKs from working](https://github.com/MaterializeInc/materialize/issues/8378). | [](#notify) |


### Other Message Brokers

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| AWS Kinesis Data Streams | {{< supportLevel beta >}} | Materialize can read source data via the [Kinesis Source](/sql/create-source/kinesis/), Kinesis cannot be used for output (Sinks). | [More Info](/sql/create-source/kinesis/) [](#notify) |
| PubNub | {{< supportLevel beta >}} | Materialize can read source data via the [PubNub Source](/sql/create-source/json-pubnub/), but PubNub is more queue than broker, Materialize only has access to messages sent after materialization. | [More Info](/sql/create-source/json-pubnub/) [](#notify) |
| Apache Pulsar | {{< supportLevel researching >}} | Direct integration requires development of a Pulsar source. Pulsar has a [Kafka Adaptor](https://pulsar.apache.org/docs/en/adaptors-kafka/) that may enable interoperability with Materialize, but it hasn't been officially tested. | [](#notify) |
| Azure Event Hubs | {{< supportLevel researching >}} | Direct integration requires development of an Event Hub source. Event Hubs have [various Kafka interoperability features](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview), but they haven't been officially tested with Materialize. | [](#notify) |
| GCP Cloud PubSub | {{< supportLevel researching >}} | Integration with GCP PubSub requires development of a PubSub Source connector. | [](#notify) |

_Is there another message broker you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Databases

Materialize can use change events _(creates, updates, deletes)_ as source data. These events typically come from the replication logs of databases. This requires Materialize to either connect directly to a database via a replication slot, or to use an intermediary service like [Debezium](/third-party/debezium/) to stream the events to a [message broker](#message-brokers).

### PostgreSQL

Materialize has a [direct PostgreSQL source](/sql/create-source/postgres/) that allows for direct connection to Postgres databases via a replication slot. Compatibility requires Postgres 10+ and appropriate `wal_level` and replication slot capabilities. Specifics of managed versions of PostgreSQL are documented below.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Self-managed PostgreSQL (10+) | {{< supportLevel beta >}} | Users with full control over their PostgreSQL database can [connect directly to a PostgreSQL](/sql/create-source/postgres/) _version 10+_ database via a replication slot, or indirectly via Debezium. | [More Info](/sql/create-source/postgres/) [](#notify) |
| Amazon RDS for PostgreSQL | {{< supportLevel beta >}} | The AWS user account requires the `rds_superuser` role to perform logical replication for the PostgreSQL database on Amazon RDS. | [More Info](https://materialize.com/docs/guides/postgres-cloud/#amazon-rds) [](#notify) |
| GCP Cloud SQL for PostgreSQL | {{< supportLevel beta >}} | Users must [enable `cloudsql.logical_decoding`](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication). | [More Info](https://materialize.com/docs/guides/postgres-cloud/#cloud-sql) [](#notify) |
| Amazon Aurora | {{< supportLevel beta >}} | AWS Aurora can be configured to work with Materialize by enabling logical replication via `rds.logical_replication`. | [More Info](https://materialize.com/docs/guides/postgres-cloud/#aws-aurora) [](#notify) |
| Amazon Aurora Serverless | {{< supportLevel researching >}} | Aurora serverless V1 does [not currently support](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless.html#aurora-serverless.limitations) the logical replication required to integrate. | [](#notify) |
| Heroku Postgres | {{< supportLevel researching >}} | Heroku Postgres does not open up access to logical replication. But an indirect connection may be possible via Kafka and [Heroku Data Connectors](https://devcenter.heroku.com/articles/heroku-data-connectors). | [](#notify) |

### Other Databases

Currently, it is only possible to use Materialize with other databases via an intermediary service like Debezium that can handle extracting change-data-capture events. This may change in the future.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| MySQL _(via Debezium)_ | {{< supportLevel production >}} | See the [guide to setting up CDC from MySQL with Debezium](/guides/cdc-mysql/) for more information. | [More Info](/guides/cdc-mysql/) |
| MySQL Direct | {{< supportLevel researching >}} | A direct MySQL Source does not exist yet, but we are exploring creating one. Subscribe via "Notify Me" to register interest. | [](#notify) |
| MongoDB _(via Debezium)_ | {{< supportLevel researching >}} | Debezium has a MongoDB connector, but it [lacks the metadata](https://github.com/MaterializeInc/materialize/issues/7289) required to work in Materialize. | [](#notify) |
| Snowflake, BigQuery, Redshift | {{< supportLevel not-supported >}} | OLAP DB's batch operational model are not a great fit for Materialize' event-driven model. Instead of reading from OLAP DB's, Materialize is better placed closer to where the data originates. |

_Is there another database you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Object Storage

Materialize can ingest archived events from append-only log files in object storage. It is not currently possible to sink data out to object storage.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Amazon S3 | {{< supportLevel beta >}} | The [AWS S3 Source](/sql/create-source/s3/) can be used to do one-time loads of archived events. [SQS notifications can be configured](/sql/create-source/s3/#listening-to-sqs-notifications) to prompt Materialize to ingest new events as they appear. | [More Info](/sql/create-source/s3/) [](#notify) |
| GCP Cloud Storage | {{< supportLevel researching >}} | Direct integration requires development of a Google Cloud Storage source. | [](#notify) |
| Azure Blob Storage | {{< supportLevel researching >}} | Direct integration requires development of an Azure Blob Storage source. | [](#notify) |
| MinIO Object Storage | {{< supportLevel researching >}} | MinIO Object Storage has an [S3 compatible API](https://min.io/product/s3-compatibility), but the Materialize S3 source needs to be updated with additional configuration options for MinIO to work. | [](#notify) |

_Is there another object storage tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Management Tools

Materialize is PostgreSQL compatible: Communication happens over the Postgres wire protocol and Postgres-compatible SQL is used for most DDL and DML interactions. This means many of the same management tools used on Postgres databases can be used on Materialize.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| dbt Core | {{< supportLevel beta >}} | The `dbt-materialize` adaptor enables users of dbt Core to manage Materialize Sources, Views, Indexes, and Sinks. [Full guide to dbt and Materialize here](https://materialize.com/docs/guides/dbt/). | [More Info](https://materialize.com/docs/guides/dbt/) [](#notify) |
| dbt Cloud | {{< supportLevel in-development >}} | The `dbt-materialize` adaptor is not currently available in dbt Cloud. | [](#notify) |
| DBeaver | {{< supportLevel production >}} | Use the PostgreSQL settings in DBeaver to connect to Materialize Core or Materialize Cloud *(using the provided certs.)* |  |
| DataGrip IDE | {{< supportLevel in-development >}} | DataGrip uses a number of `pg_catalog` endpoints that are not yet implemented by Materialize. For details, see the [DataGrip tracking issue](https://github.com/MaterializeInc/materialize/issues/9720) in GitHub. | [](#notify) |
| PGAdmin | {{< supportLevel in-development >}} | Upon connection, PGAdmin executes configuration and `pg_catalog` queries that are not yet implemented by Materialize. | [](#notify) |
| Table Plus | {{< supportLevel alpha >}} | Able to connect to Materialize Core and Cloud *(using provided certs)* and run queries via SQL Editor. Introspection fails due to dependence on `pg_catalog` items not yet implemented. | [](#notify) |

_Is there another DB management tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Libraries and Drivers

The following popular PostgreSQL libraries and drivers have been tested and confirmed to be working well with Materialize.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Node.js | {{< supportLevel production >}} | The [`node-postgres` library](https://node-postgres.com/) can be used to [manage](/guides/node-js/#manage-sources-views-and-indexes), [query](/guides/node-js/#query) and even [stream](/guides/node-js/#stream) data from Materialize. | [More Info](/guides/node-js/) |
| Python | {{< supportLevel production >}} | The [`psycopg2` python package](https://pypi.org/project/psycopg2/) can be used to interact with Materialize as if it were a PostgreSQL DB. | |
| Java | {{< supportLevel production >}} | The popular [PostgreSQL JDBC driver](https://jdbc.postgresql.org/) can be used to interact with Materialize as if it were a PostgreSQL DB. |  |
| Golang | {{< supportLevel production >}} | TODO: Confirm which Go drivers have been tested. |  |
| PHP | {{< supportLevel production >}} | The standard PHP [PostgreSQL Extension](https://www.php.net/manual/en/ref.pgsql.php) can be used to interact with Materialize as if it were a PostgreSQL DB. |  |

## Frameworks and ORMs

Frameworks and ORMs tend to make more advanced queries to PostgreSQL behind the scenes, using configuration settings and system table endpoints that Materialize hasn't yet implemented. As Materialize [expands `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157), support for frameworks and ORMs will improve.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Ruby on Rails | {{< supportLevel researching >}} | The Rails Active-Record ORM executes many PostgreSQL configuration queries and queries to `pg_catalog` endpoints that are not yet implemented in Materialize. | [](#notify) |
| Prisma | {{< supportLevel researching >}} | Prisma executes configuration queries and queries to `pg_catalog` endpoints that are not yet implemented in Materialize. | [](#notify) |

_Is there another framework or ORM you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Data Tools

Many tools in the modern data stack can connect to Materialize via PostgreSQL, but like ORMs, these tools sometimes make advanced configuration and system table queries to endpoints that Materialize hasn't yet implemented.

### Business Intelligence (BI) Tools

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Metabase | {{< supportLevel beta >}} | The Metabase PostgreSQL connector can be used to [connect Metabase to Materialize Core](/third-party/metabase/). | [More Info](/third-party/metabase/) [](#notify) |
| Superset | {{< supportLevel alpha >}} | Connect Superset to Materialize Core or Cloud using the Postgres connector.  | [](#notify) |
| Looker | {{< supportLevel alpha >}} | Connect Looker to Materialize Core by adding a PostgreSQL 9.5+ database connection and specifying your Materialize credentials. Connections to Materialize Cloud are currently blocked by user/password auth. | [](#notify) |
| Google Data Studio | {{< supportLevel alpha >}} | Google Data Studio can connect to Materialize Core and Cloud using the PostgreSQL connector. Data is cached hourly but can be manually refreshed. | [](#notify) |
| Tableau | {{< supportLevel researching >}} | | [](#notify) |
| Microsoft Power BI | {{< supportLevel researching >}} | | [](#notify) |
| Preset | {{< supportLevel researching >}} | | [](#notify) |
| Mode Analytics | {{< supportLevel researching >}} | | [](#notify) |
| Holistics BI | {{< supportLevel researching >}} | | [](#notify) |

### Other Data Applications and Tools

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Hex | {{< supportLevel alpha >}} | Users of Hex can connect to Materialize Core instances via the Hex PostgreSQL connector. Hex automatically introspects Materialized Views and Tables. *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| Cube.js | {{< supportLevel alpha >}} | The Cube.js PostgreSQL driver [can be edited](https://github.com/rongfengliang/cubejs-materialize-driver) to work with Materialize. A Cube.js driver for Materialize is in active development. | [](#notify) |
| Retool | {{< supportLevel alpha >}} | The Retool PostgreSQL connector can be used to connect to a Materialize Core instance, *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| Hightouch | {{< supportLevel in-development >}} | The Hightouch PostgreSQL connector can be used to connect to a Materialize Core instance, *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| FiveTran | {{< supportLevel researching >}} |  | [](#notify) |
| Stitch | {{< supportLevel researching >}} |  | [](#notify) |
| Meltano | {{< supportLevel researching >}} |  | [](#notify) |
| AirByte | {{< supportLevel researching >}} |  | [](#notify) |
| Census | {{< supportLevel researching >}} |  | [](#notify) |

_Is there another data tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

<div id="subscribe_dialog">
  <form name="notify">
    <input name="email" type="email" placeholder="Email Address" required="required"/>
    <input type="submit" class="default_button" value="Confirm" />
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
        $('a[href="#notify"]').replaceWith('<button class="default_button" data-js="subscribe_open" title="Get notified when support is upgraded.">Notify Me</button>')

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
