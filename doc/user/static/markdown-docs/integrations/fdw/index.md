# Foreign data wrapper (FDW)
Use FDW to access Materialize
Materialize can be used as a remote server in a PostgreSQL foreign data wrapper
(FDW). This allows you to query any object in Materialize as foreign tables from
a PostgreSQL-compatible database. These objects appear as part of the local
schema, making them accessible over an existing Postgres connection without
requiring changes to application logic or tooling.

## Prerequisite

1. In Materialize, create a dedicated service account `fdw_svc_account` as an
   **Organization Member**. For details on setting up a service account, see
   [Create a service
   account](https://materialize.com/docs/manage/users-service-accounts/create-service-accounts/)

   > **Tip:** Per the linked instructions, be sure you connect at least once with the new
>    service account to finish creating the new account. You will also need the
>    connection details (host, port, password) when setting up the foreign server
>    and user mappings in PostgreSQL.


1. After you have connected at least once with the new service account to finish
   the new account creation, modify the `fdw_svc_account` role:

   1. Set the default cluster to the name of your serving cluster:

      ```mzsql
      ALTER ROLE fdw_svc_account SET CLUSTER = <serving_cluster>;
      ```

   1. [Grant `USAGE` privileges](/sql/grant-privilege/) on the serving cluster,
      and the database and schema of your views and materialized views.

      ```mzsql
      GRANT USAGE ON CLUSTER <serving_cluster> TO fdw_svc_account;
      GRANT USAGE ON DATABASE <db_name> TO fdw_svc_account;
      GRANT USAGE ON SCHEMA <db_name.schema_name> TO fdw_svc_account;
      ```

   1. [Grant `SELECT` privileges](/sql/grant-privilege/) to the various
      view(s)/materialized view(s):

      ```mzsql
      GRANT SELECT ON <db_name.schema_name.view_name>, <...> TO fdw_svc_account;
      ```

## Setup FDW in PostgreSQL

**In your PostgreSQL instance**:

1. If not installed, create a `postgres_fdw` extension in your database:

   ```mzsql
   CREATE EXTENSION postgres_fdw;
   ```

1. Create a foreign server to your Materialize, substitute your [Materialize
   connection details](/console/connect/).

   ```mzsql
   CREATE SERVER remote_mz_server
      FOREIGN DATA WRAPPER postgres_fdw
      OPTIONS (host '<host>', dbname '<db_name>', port '6875');
   ```

1. Create a user mapping between your PostgreSQL user and the Materialize
   `fdw_svc_account`:

   ```mzsql
   CREATE USER MAPPING FOR <postgres_user>
      SERVER remote_mz_server
      OPTIONS (user 'fdw_svc_account', password '<service_account_password>');
   ```

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
   ```

1. Once created, you can select from within PostgreSQL:

   ```mzsql
   SELECT * from <local_view_name_in_postgres>;
   ```
