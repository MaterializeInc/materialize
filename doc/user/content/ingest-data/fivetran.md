---
title: "Fivetran"
description: "How to use Fivetran to sync data into Materialize"
aliases:
  - /integrations/fivetran/
---

{{< private-preview />}}

[Fivetran](https://www.fivetran.com/) is a cloud-based automated data movement platform for
extracting, loading and transforming data from a wide variety of connectors.

You can use Fivetran to sync data into Materialize for the following use cases:
- To sync data from SaaS applications or platforms, such as HubSpot, Shopify, or Stripe.
- To sync data from event streaming sources, such as Kinesis or Google Pub/Sub.
- To sync data from other data warehouses, such as Snowflake, Databricks, or Oracle.

For relational databases like PostgreSQL or MySQL, and event streaming sources like Apache Kafka,
you should prefer to use [Materialize native sources](/sql/create-source-v1/).

## Before you begin
### Terminology
Fivetran syncs data from what they call
[sources](https://fivetran.com/docs/getting-started/glossary#source) to what they call
[destinations](https://fivetran.com/docs/getting-started/glossary#destination). Users create
[connectors](https://fivetran.com/docs/getting-started/glossary#connector) to configure the data
pipelines that repeatedly sync the data from each source to the destination at a scheduled cadence.

In this setup, Materialize is the destination. The source is whichever data source you're syncing
into Materialize, such as Hubspot or Shopify.

### Prerequisites
Ensure that you have:
- An active [Fivetran](https://www.fivetran.com/) account with
[permission to add destinations and connectors](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/account-management/role-based-access-control#legacyandnewrbacmodel).
- For the Materialize user that you're using to connect to Fivetran,
[`CREATE`](/security/appendix/appendix-privileges/) privileges on the
target database in Materialize.

## Setup guide
### Step 1: Create the Materialize destination
Follow this
[Materialize-authored guide in the Fivetran docs](https://fivetran.com/docs/destinations/materialize/setup-guide#materializesetupguide) to set up Materialize as a destination in Fivetran.

### Step 2: Create the connector(s)
Follow the
[Fivetran guide on connectors](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/connectors#overview)
to set up your connector(s). Choose your newly created Materialize destination as the destination
for the connector.

Schema changes to existing tables is not currently supported. When creating a Connector you should
select the option to
["Block all" schema changes](https://fivetran.com/docs/using-fivetran/fivetran-dashboard/connectors/schema#defineschemachangehandlingsettings).

You can see the full list of available [Fivetran connectors](https://fivetran.com/docs/connectors)
in their docs.

## Other setup information
### Type transformation mapping
As we extract your data, we match Fivetran data types to types that Materialize supports. If we don't
support a specific data type, we automatically change that type to the closest supported data type.

The data types in Materialize follow Fivetran's standard data type storage.

The following table illustrates how we transform Fivetran data types into Materialize-supported
types:

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

### Sync frequency
The highest sync frequency Fivetran offers is 1 minute for Enterprise and Business Critical plans,
and 5 minutes for all other plans. The lowest sync frequency is 24 hours. You can read more about
sync scheduling in the
[Fivetran docs](https://fivetran.com/docs/core-concepts/syncoverview#syncfrequencyandscheduling).
