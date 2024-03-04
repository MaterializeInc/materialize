---
name: Materialize
title: Fivetran for Materialize | Configuration and documentation
description: Connect data sources to Materialize in minutes using Fivetran. Explore documentation and start syncing your applications, databases, and events.
---

# Materialize {% badge text="Partner-Built" /%} {% badge text="Private Preview" /%}

[Materialize](https://materialize.com) is an operational data warehouse that incrementally maintain your `VIEW`s, responding to queries in
tens of milliseconds.

You can use Fivetran to sync data to Materialize for the following use cases:

- To sync data from SaaS applications or platforms, such as HubSpot, Shopify, or Stripe.
- To sync data from event streaming sources, such as Kinesis or Google Pub/Sub.
- To sync data from other data warehouse, such as Snowflake, Databricks, or Oracle.

For databases like Postgres or MySQL, and event streaming sources like Kafka, you should prefer to use [Materialize native sources](https://materialize.com/docs/sql/create-source/).

> NOTE: This destination is [partner-built](/docs/partner-built-program). For any questions related to Materialize destination and its documentation, contact [Materialize Support](mailto:support@materialize.com).

----

## Setup guide

Follow our [step-by-step Materialize setup guide](/docs/destinations/materialize/setup-guide) to connect Materialize to Fivetran.

----

## Type transformation mapping

As we extract your data, we match Fivetran data types to types that Materialize supports. If we don't support a specific data type, we automatically change that type to the closest supported data type or, in some cases, don't load that data.

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
| XML                | STRING                |
| JSON               | JSON                  |
| BINARY             | STRING                |

----

## Schema changes

| SCHEMA CHANGE      | SUPPORTED | NOTES                                                                                                     |
|--------------------|-----------|-----------------------------------------------------------------------------------------------------------|
| Add column         | No        | Adding a column to your source is not currently supported.                                                |
| Change column type | No        | Changing column types is not supported as it is a breaking schema change.                                 |

---

## Limitations

Materialize has the following limitations:

- Schema changes to an existing source are not supported.
