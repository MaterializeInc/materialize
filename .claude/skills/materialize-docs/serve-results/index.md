---
audience: developer
canonical_url: https://materialize.com/docs/serve-results/
complexity: advanced
description: Serving results from Materialize
doc_type: reference
keywords:
- SELECT/SUBSCRIBE statements
- External BI tools
- Serve results
- 'Tip:'
- Sink results
- CREATE A
- CREATE AN
- SELECT REGION
- SELECT YOU
- materialized
product_area: Sinks
status: stable
title: Serve results
---

# Serve results

## Purpose
Serving results from Materialize

If you need to understand the syntax and options for this command, you're in the right place.


Serving results from Materialize


In Materialize, indexed views and materialized views maintain up-to-date query
results. This allows Materialize to serve fresh query results with low latency.

To serve results, you can:

- [Query using `SELECT` and `SUBSCRIBE`
  statements](/serve-results/query-results/)

- [Use BI/data collaboration tools](/serve-results/bi-tools/)

- [Sink results to to external systems](/serve-results/sink/)

- [Use Foreign Data Wrapper (FDW)](/serve-results/fdw/)


**SELECT/SUBSCRIBE statements**
- [Query using `SELECT` and `SUBSCRIBE`](/serve-results/query-results/)
- [Use Foreign Data Wrapper (FDW)](/serve-results/fdw/)
**External BI tools**
- [Deepnote](/serve-results/bi-tools/deepnote/)
- [Hex](/serve-results/bi-tools/hex/)
- [Metabase](/serve-results/bi-tools/metabase/)
- [Power BI](/serve-results/bi-tools/power-bi/)
- [Tableau](/serve-results/bi-tools/tableau/)
- [Looker](/serve-results/bi-tools/looker/)

**Sink results**
- [Sinking results to Amazon S3](/serve-results/sink/s3/)
- [Sinking results to Census](/serve-results/sink/census/)
- [Sinking results to Kafka](/serve-results/sink/kafka/)
- [Sinking results to Snowflake](/serve-results/sink/snowflake/)


---

## `SELECT` and `SUBSCRIBE`


You can query results from Materialize using `SELECT` and `SUBSCRIBE` SQL
statements. Because Materialize uses the PostgreSQL wire protocol, it works
out-of-the-box with a wide range of SQL clients and tools that support
PostgreSQL.

## SELECT

You can query data in Materialize using the [`SELECT` statement](/sql/select/).
For example:

```mzsql
SELECT region.id, sum(purchase.total)
FROM mysql_simple_purchase AS purchase
JOIN mysql_simple_user AS user ON purchase.user_id = user.id
JOIN mysql_simple_region AS region ON user.region_id = region.id
GROUP BY region.id;
```text

Performing a `SELECT` on an indexed view or a materialized view is
Materialize's ideal operation. When Materialize receives such a `SELECT` query,
it quickly returns the maintained results from memory.

Materialize also quickly returns results for queries that only filter, project,
transform with scalar functions, and re-order data that is maintained by an
index.

Queries that can't simply read out from an index will create an ephemeral dataflow to compute
the results. These dataflows are bound to the active [cluster](/concepts/clusters/),
 which you can change using:

```mzsql
SET cluster = <cluster name>;
```text

Materialize will remove the dataflow as soon as it has returned the query
results to you.

For more information, see [`SELECT`](/sql/select/) reference page.  See
also the following client library guides:


- [Go](/integrations/client-libraries/golang/#query)</li>
- [Java](/integrations/client-libraries/java-jdbc/#query)</li>
- [Node.js](/integrations/client-libraries/node-js/#query)</li>
- [PHP](/integrations/client-libraries/php/#query)</li>
- [Python](/integrations/client-libraries/python/#query)</li>
- [Ruby](/integrations/client-libraries/ruby/#query)</li>
- [Rust](/integrations/client-libraries/rust/#query)</li>


## SUBSCRIBE

You can use [`SUBSCRIBE`](/sql/subscribe/) to stream query results.  For
example:

```mzsql
BEGIN;
DECLARE c CURSOR FOR SUBSCRIBE (SELECT * FROM mv_counter_sum);
FETCH 10 c WITH (timeout='1s');
FETCH 20 c WITH (timeout='1s');
COMMIT;
```text

The [`SUBSCRIBE`](/sql/subscribe/) statement is a more general form of a `SELECT` statement. While a `SELECT` statement computes a relation at a moment in time, a `SUBSCRIBE` operation computes how a relation changes over time.

You can use `SUBSCRIBE` to:

- Power event processors that react to every change to a relation or an
  arbitrary `SELECT` statement.

- Replicate the complete history of a relation while `SUBSCRIBE` is active.

> **Tip:** 
Use materialized view (instead of an indexed view) with `SUBSCRIBE`.


For more information, see [`SUBSCRIBE`](/sql/subscribe/) reference page.  See
also the following client library guides:


- [Go](/integrations/client-libraries/golang/#stream)</li>
- [Java](/integrations/client-libraries/java-jdbc/#stream)</li>
- [Node.js](/integrations/client-libraries/node-js/#stream)</li>
- [PHP](/integrations/client-libraries/php/#stream)</li>
- [Python](/integrations/client-libraries/python/#stream)</li>
- [Ruby](/integrations/client-libraries/ruby/#stream)</li>
- [Rust](/integrations/client-libraries/rust/#stream)</li>


---

## Sink results


A [sink](/concepts/sinks/) describes the external system you want Materialize to
write data to and details the encoding of that data. You can sink data from a
**materialized** view, a source, or a table.

## Sink methods

To create a sink, you can:

<!-- Dynamic table: sink_external_systems - see original docs -->

### Operational guideline

- Avoid putting sinks on the same cluster that hosts sources to allow for
[blue/green deployment](/manage/dbt/blue-green-deployments).

### Troubleshooting

For help, see [Troubleshooting
sinks](/serve-results/sink/sink-troubleshooting/).


---

## Use BI/data collaboration tools


Materialize uses the PostgreSQL wire protocol, which allows it to integrate out-of-the-box with various BI/data collaboration tools that support PostgreSQL.

To help you get started, the following guides are available:


---

## Use foreign data wrapper (FDW)


Materialize can be used as a remote server in a PostgreSQL foreign data wrapper
(FDW). This allows you to query any object in Materialize as foreign tables from
a PostgreSQL-compatible database. These objects appear as part of the local
schema, making them accessible over an existing Postgres connection without
requiring changes to application logic or tooling.


## Prerequisite

1. In Materialize, create a dedicated service account `fdw_svc_account` as an
   **Organization Member**. For details on setting up a service account, see
   [Create a service
   account](../manage/users-service-accounts/create-service-accounts/)

   > **Tip:** 
   Per the linked instructions, be sure you connect at least once with the new
   service account to finish creating the new account. You will also need the
   connection details (host, port, password) when setting up the foreign server
   and user mappings in PostgreSQL.

   

1. After you have connected at least once with the new service account to finish
   the new account creation, modify the `fdw_svc_account` role:

   1. Set the default cluster to the name of your serving cluster:

      ```mzsql
      ALTER ROLE fdw_svc_account SET CLUSTER = <serving_cluster>;
      ```text

   1. [Grant `USAGE` privileges](/sql/grant-privilege/) on the serving cluster,
      and the database and schema of your views and materialized views.

      ```mzsql
      GRANT USAGE ON CLUSTER <serving_cluster> TO fdw_svc_account;
      GRANT USAGE ON DATABASE <db_name> TO fdw_svc_account;
      GRANT USAGE ON SCHEMA <db_name.schema_name> TO fdw_svc_account;
      ```text

   1. [Grant `SELECT` privileges](/sql/grant-privilege/) to the various
      view(s)/materialized view(s):

      ```mzsql
      GRANT SELECT ON <db_name.schema_name.view_name>, <...> TO fdw_svc_account;
      ```bash


## Setup FDW in PostgreSQL

**In your PostgreSQL instance**:

1. If not installed, create a `postgres_fdw` extension in your database:

   ```mzsql
   CREATE EXTENSION postgres_fdw;
   ```text

1. Create a foreign server to your Materialize, substitute your [Materialize
   connection details](/console/connect/).

   ```mzsql
   CREATE SERVER remote_mz_server
      FOREIGN DATA WRAPPER postgres_fdw
      OPTIONS (host '<host>', dbname '<db_name>', port '6875');
   ```text

1. Create a user mapping between your PostgreSQL user and the Materialize
   `fdw_svc_account`:

   ```mzsql
   CREATE USER MAPPING FOR <postgres_user>
      SERVER remote_mz_server
      OPTIONS (user 'fdw_svc_account', password '<service_account_password>');
   ```text

1. For each view/materialized view you want to access, create the foreign table
   mapping (you can use the [data explorer](/console/data/) to get the column
   detials)

   ```mzsql
   CREATE FOREIGN TABLE <local_view_name_in_postgres> (
            <column> <type>,
            ...
        )
   SERVER remote_mz_server
   OPTIONS (schema_name '<schema>', table_name '<view_name_in_Materialize>');
   ```text

1. Once created, you can select from within PostgreSQL:

   ```mzsql
   SELECT * from <local_view_name_in_postgres>;
   ```