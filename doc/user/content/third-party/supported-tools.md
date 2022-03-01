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
| 🟢 **Production** | We are committed to prioritizing bugs in the interaction between these tools and Materialize. |
| 🟢 **Beta** | There may be small performance issues and minor missing features, but Materialize supports the major use cases for this tool. You can file bug reports or feature requests for Materialize integration with these tools [here](https://github.com/MaterializeInc/materialize) but they may not be prioritized. |
| 🟡 **Alpha** | Some of our community members have made this integration work, but we haven’t tested it ourselves and can’t guarantee its stability. |
| 🔴 **Active Development** | **There are known issues** preventing the integration from working, but we are actively developing features that unblock the integration. |
| 🔴 **Researching** | **There are known issues** preventing the integration from working, but we are gathering user feedback and gauging interest in supporting these integrations. |

## Message Brokers

### Kafka

Kafka is well-supported in Materialize. [`SOURCE`](/sql/create-source/)'s can be used to consume data from Kafka, and a [`SINK`](/sql/create-sink/) can be used to produce data *(in the form of change events from a Materialized view)* back out to Kafka.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Apache Kafka | 🟢 Production | Kafka is a well-supported source with multiple [configuration](/sql/create-source/kafka/#with-options) and [security](/sql/create-source/kafka/#authentication) options. | [More Info](/sql/create-source/kafka/) |
| Confluent Cloud Kafka | 🟢 Production | Use SASL authentication, see [example here](/sql/create-source/kafka/#saslplain). The same config can be used to produce messages to Confluent Kafka via a [SINK](/sql/create-sink/). |  |
| AWS MSK (Managed Streaming for Kafka) | 🟢 Production | Use SASL/SCRAM Authentication to securely connect to MSK clusters. [MSK SASL Docs](https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html) *(mTLS connections coming soon.)* |  |
| Redpanda Core | 🟢 Beta | Repdanda works as a Kafka Source and Sink in Materialize. See [using Redpanda with Materialize](/third-party/redpanda/) for instructions and limitations. | [](#notify) |
| Heroku Kafka | 🟡 Alpha | Test it out! | [](#notify) |

### Other Message Brokers

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| AWS Kinesis Data Streams | 🟢 Beta | Materialize can read source data via the [Kinesis Source](/sql/create-source/kinesis/), Kinesis cannot be used for output (Sinks). | [](#notify) |
| PubNub | 🟢 Beta | Materialize can read source data via the [PubNub Source](/sql/create-source/pubnub/), but PubNub is more queue than broker, Materialize only has access to messages sent after materialization. | [](#notify) |
| Apache Pulsar | 🔴 Researching | Direct integration requires development of a Pulsar source. Pulsar has a [Kafka Adaptor](https://pulsar.apache.org/docs/en/adaptors-kafka/) that may enable interoperability with Materialize, but it hasn't been officially tested. | [](#notify) |
| Azure Event Hubs | 🔴 Researching | Direct integration requires development of an Event Hub source. Event Hubs have [various Kafka interoperability features](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview), but they haven't been officially tested with Materialize. | [](#notify) |
| GCP Cloud PubSub | 🔴 Researching | Integration with GCP PubSub requires development of a PubSub Source connector. | [](#notify) |

_Is there another message broker you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Databases

Materialize works with individual writes _(creates, updates, deletes)_ as source data, found via the change or replication logs of databases. This requires Materialize to either connect directly to a database via a replication slot, or to use an intermediary service like Debezium to stream the events to a message broker.

### PostgreSQL

Materialize has a [direct PostgreSQL source](/sql/create-source/postgres/) that allows for direct connection to Postgres databases via a replication slot. Compatibility requires Postgres 10+ and appropriate `wal_level` and replication slot capabilities. Specifics of managed versions of PostgreSQL are documented below.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Self-managed PostgreSQL (10+) | 🟢 Beta | Users with full control over their PostgreSQL database can [connect directly to a PostgreSQL](/sql/create-source/postgres/) _version 10+_ database via a replication slot, or indirectly via Debezium. | [](#notify) |
| Amazon RDS for PostgreSQL | 🟢 Beta | The AWS user account requires the `rds_superuser` role to perform logical replication for the PostgreSQL database on Amazon RDS. | [](#notify) |
| GCP Cloud SQL for PostgreSQL | Tbd | TBD? | [](#notify) |
| Amazon Aurora | Tbd | TBD? | [](#notify) |
| Amazon Aurora Serverless | Tbd | TBD? | [](#notify) |
| Heroku Postgres | Tbd | TBD? | [](#notify) |
| Azure Database for PostgreSQL | Tbd | TBD? | [](#notify) |
| Supabase | Tbd | TBD? | [](#notify) |

### Other Databases

It is possible to use Materialize with other databases, but only via an intermediary service like Debezium that can handle extracting change-data-capture events and producing them to Kafka.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| MySQL | 🟢 Production | See the [guide to setting up CDC from MySQL with Debezium](/guides/cdc-mysql/) for more information. |  |
| MongoDB | 🔴 Researching | Debezium has a MongoDB connector, but it lacks the metadata required to work in Materialize. | [](#notify) |

_Is there another database you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Object Storage

Materialize can ingest archived events from append-only log files in object storage. It is not currently possible to sink data out to object storage.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Amazon S3 | 🟢 Beta | The [AWS S3 Source](/sql/create-source/text-s3/) can be used to do one-time loads of archived events. [SQS notifications can be configured](/sql/create-source/text-s3/#listening-to-sqs-notifications) to prompt Materialize to ingest new events as they appear. | [](#notify) |
| GCP Cloud Storage | 🔴 Researching | Direct integration requires development of a Google Cloud Storage source. | [](#notify) |
| Azure Blob Storage | 🔴 Researching | Direct integration requires development of an Azure Blob Storage source. | [](#notify) |
| MinIO Object Storage | 🔴 Researching | MinIO Object Storage has an [S3 compatible API](https://min.io/product/s3-compatibility), but the Materialize S3 source needs to be updated with additional configuration options for MinIO to work. | [](#notify) |

_Is there another object storage tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Management Tools

Materialize is PostgreSQL compatible: Communication happens over the Postgres wire protocol and Postgres-compatible SQL is used for most DDL and DML interactions. This means many of the same management tools used on Postgres databases can be used on Materialize.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| dbt Core | 🟢 Beta | The `dbt-materialize` adaptor enables users of dbt Core to manage Materialize Sources, Views, Indexes, and Sinks. [Full guide to dbt and Materialize here](https://materialize.com/docs/guides/dbt/). | [](#notify) |
| dbt Cloud | 🔴 Active Development | The `dbt-materialize` adaptor is not currently available in dbt Cloud. | [](#notify) |
| DBeaver | 🟢 Production | Use the PostgreSQL settings in DBeaver to connect to Materialize Core or Materialize Cloud *(using the provided certs.)* |  |
| DataGrip IDE | 🔴 Active Development | DataGrip uses a number of `pg_catalog` endpoints that are not yet implemented by Materialize. For details, see the [DataGrip tracking issue](https://github.com/MaterializeInc/materialize/issues/9720) in GitHub. | [](#notify) |
| PGAdmin | 🔴 Active Development | Upon connection, PGAdmin executes configuration and `pg_catalog` queries that are not yet implemented by Materialize. | [](#notify) |
| Table Plus | 🟡 Alpha | Able to connect to Materialize Core and Cloud *(using provided certs)* and run queries via SQL Editor. Introspection fails due to dependence on `pg_catalog` items not yet implemented. | [](#notify) |

_Is there another DB management tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Libraries and Drivers

The following popular PostgreSQL libraries and drivers have been tested and confirmed to be working well with Materialize.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Node.js | 🟢 Production | The [`node-postgres` library](https://node-postgres.com/) can be used to [manage](/guides/node-js/#manage-sources-views-and-indexes), [query](/guides/node-js/#query) and even [stream](/guides/node-js/#stream) data from Materialize. | [More Info](/guides/node-js/) |
| Python | 🟢 Production | The [`psycopg2` python package](https://pypi.org/project/psycopg2/) can be used to interact with Materialize as if it were a PostgreSQL DB. | [More Info](/guides/python/) |
| Java | 🟢 Production | The popular [PostgreSQL JDBC driver](https://jdbc.postgresql.org/) can be used to interact with Materialize as if it were a PostgreSQL DB. | [More Info](/guides/java/) |
| Golang | 🟢 Production | TODO: Confirm which Go drivers have been tested. | [More Info](/guides/golang/) |
| PHP | 🟢 Production | The standard PHP [PostgreSQL Extension](https://www.php.net/manual/en/ref.pgsql.php) can be used to interact with Materialize as if it were a PostgreSQL DB. | [More Info](/guides/php/) |

## Frameworks and ORMs

Frameworks and ORMs tend to make more advanced queries to PostgreSQL behind the scenes, using configuration settings and system table endpoints that Materialize hasn't yet implemented. As Materialize [expands `pg_catalog` support](https://github.com/MaterializeInc/materialize/issues/2157), support for frameworks and ORMs will improve.

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Laravel ORM | 🟡 Alpha | TODO: Summarize. Link to GitHub Issue. | [](#notify) |
| Adonis.JS | 🟡 Alpha | TODO: Summarize. Link to GitHub Issue. | [](#notify) |
| Postgraphile | 🟡 Alpha | TODO: Summarize. Link to GitHub Issue. | [](#notify) |
| Ruby on Rails | 🔴 Researching | The Rails Active-Record ORM executes many PostgreSQL configuration queries and queries to `pg_catalog` endpoints that are not yet implemented in Materialize. | [](#notify) |
| Django | 🔴 Researching | TODO: Investigate, create GitHub issue. | [](#notify) |
| SQLAlchemy | 🔴 Researching | TODO: Investigate, create GitHub issue. | [](#notify) |
| Prisma | 🔴 Researching | Prisma executes configuration queries and queries to `pg_catalog` endpoints that are not yet implemented in Materialize. | [](#notify) |

_Is there another framework or ORM you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

## Data Tools

Many tools in the modern data stack can connect to Materialize via PostgreSQL, but like ORMs, these tools often make advanced configuration and system table queries to endpoints that Materialize hasn't yet implemented.

### Business Intelligence (BI) Tools

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Metabase | 🟢 Beta | The Metabase PostgreSQL connector can be used to [connect Metabase to Materialize Core](/third-party/metabase/). | [](#notify) |
| Looker | 🟡 Alpha | TBD @andrioni input. | [](#notify) |
| Google Data Studio | 🟡 Alpha | Google Data Studio can connect to Materialize Core and Cloud using the PostgreSQL connector. Data is cached hourly but can be manually refreshed. | [](#notify) |
| Tableau | 🔴 Active Development | TBD @andrioni input. | [](#notify) |
| Superset | 🔴 Active Development | TBD @andrioni input. | [](#notify) |
| Microsoft Power BI | 🔴 Researching | Power BI hasn't been officially tested with Materialize. | [](#notify) |
| Preset | 🔴 Researching | Preset hasn't been officially tested with Materialize. | [](#notify) |
| Mode Analytics | 🔴 Researching | Mode hasn't been officially tested with Materialize. | [](#notify) |
| Holistics BI | 🔴 Researching | Holistics hasn't been officially tested with Materialize. | [](#notify) |

### Other Data Applications and Tools

| Service | Materialize Support | Notes |  |
| --- | --- | --- | --- |
| Hex | 🟡 Alpha | Users of Hex can connect to Materialize Core instances via the Hex PostgreSQL connector. Hex automatically introspects Materialized Views and Tables. *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| Cube.js | 🟡 Alpha | The Cube.js PostgreSQL driver [can be edited](https://github.com/rongfengliang/cubejs-materialize-driver) to work with Materialize. A Cube.js driver for Materialize is in active development. | [](#notify) |
| Retool | 🟡 Alpha | The Retool PostgreSQL connector can be used to connect to a Materialize Core instance, *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| Hightouch | 🔴 Active Development | The Hightouch PostgreSQL connector can be used to connect to a Materialize Core instance, *(Cloud connectivity is blocked by user/password auth.)* | [](#notify) |
| FiveTran | 🔴 Researching |  | [](#notify) |
| Stitch | 🔴 Researching |  | [](#notify) |
| Meltano | 🔴 Researching |  | [](#notify) |
| AirByte | 🔴 Researching |  | [](#notify) |
| Streamlit | 🔴 Researching |  | [](#notify) |
| Census | 🔴 Researching |  | [](#notify) |
| TopCoat | 🔴 Researching |  | [](#notify) |

_Is there another data tool you'd like to use with Materialize? [Open a GitHub Issue here](https://github.com/MaterializeInc/materialize/issues/new?assignees=&labels=A-integration&template=02-feature.yml)._

<div id="subscribe_dialog">
  <form>
    <input name="email" type="email" placeholder="Email Address" required="required"/>
    <input type="submit" class="default_button" value="Confirm" />
  </form>
  <div class="disclaimer">
    <em>Subscribe to receive an email when support status is upgraded. No spam!</em>
  </div>
</div>

<script>

  $(function () {
    $('a[href="#notify"]').replaceWith('<button class="default_button" data-js="subscribe_open" title="Get notified when support is upgraded.">Notify Me</button>')

    var removeForms = function(){
      $('.subscribe_dialog_active').removeClass('subscribe_dialog_active');
    };

    $("[data-js='subscribe_open']").on("click", function(){
      !$(this).hasClass('success') &&
      $(this).parent()
        .addClass('subscribe_dialog_active')
        .append($('#subscribe_dialog'));
      return false;
    });

    $("#subscribe_dialog").submit(function(e) {
      var email = $(this).find('[name="email"]').val(),
          subject = $(this).siblings("[data-js='subscribe_open']")
        .addClass('success')
        .attr('title', 'Subscribed')
        .text('✔️')
        .attr('data-subject');
      removeForms();
      window.analytics && window.analytics.identify(email);
      window.analytics && window.analytics.track("Integration Status Subscribed", { subject: subject });
      e.preventDefault();
    });

    $("body").on("click", function(e){
      if(!$(e.target).closest('#subscribe_dialog').length) {
        removeForms();
      }
    });
 });

</script>
