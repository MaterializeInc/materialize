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
