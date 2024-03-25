---
name: Materialize
title: Fivetran for Materialize | Configuration and documentation
description: Connect data sources to Materialize in minutes using Fivetran. Explore documentation and start syncing your applications and events.
---

# Materialize {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

[Materialize](https://materialize.com) is a data warehouse purpose-built for operational workloads where an analytical data warehouse would be too slow, and a stream processor would be too complicated.

Using SQL and common tools in the wider data ecosystem, Materialize allows you to build real-time automation, engaging customer experiences, and interactive data products that drive value for your business while reducing the cost of data freshness.

You can use Fivetran to sync data to Materialize for the following use cases:

- To sync data from SaaS applications or platforms, such as HubSpot, Shopify, or Stripe.
- To sync data from event streaming sources, such as Kinesis or Google Pub/Sub.
- To sync data from other data warehouses, such as Snowflake, Databricks, or Oracle.

For relational databases like PostgreSQL or MySQL, and event streaming sources like Apache Kafka, you should prefer to use [Materialize native sources](https://materialize.com/docs/sql/create-source/).

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to the Materialize destination and its documentation, contact the [Materialize team](mailto:support@materialize.com).

----

## Setup guide

Follow our [step-by-step Materialize setup guide](/docs/destinations/materialize/setup-guide) to connect Materialize to Fivetran.

----

## Type transformation mapping

As we extract your data, we match Fivetran data types to types that Materialize supports. If we don't support a specific data type, we automatically change that type to the closest supported data type.

The data types in Materialize follow Fivetran's [standard data type storage](/docs/destinations#datatypes).

The following table illustrates how we transform your Fivetran data types into Materialize-supported types:

| FIVETRAN DATA TYPE | MATERIALIZE DATA TYPE |
|--------------------|-----------------------|
| BOOLEAN            | BOOLEAN               |
| SHORT              | INT16                 |
| INT                | INT32                 |
| LONG               | INT64                 |
| BIGDECIMAL         | DOUBLE                |
| FLOAT              | FLOAT                 |
| DOUBLE             | DOUBLE                |
| LOCALDATE          | DATE                  |
| LOCALDATETIME      | TIMESTAMP             |
| INSTANT            | TIMESTAMP             |
| STRING             | STRING                |
| JSON               | JSONB                 |
| BINARY             | STRING                |
| XML                | Unsupported           |

----

## Schema changes

Schema changes to existing tables is not currently supported. When creating a Connector you should select the option to "Block all" schema changes.

| SCHEMA CHANGE      | SUPPORTED | NOTES                                                                                                     |
|--------------------|-----------|-----------------------------------------------------------------------------------------------------------|
| Add column         | No        | Adding a column to your source is not currently supported.                                                |
| Change column type | No        | Changing column types is not supported as it is a breaking schema change.                                 |

---

## Limitations

The Fivetran Materialize destination has the following known limitations:

- Schema changes to an existing source are not supported.
