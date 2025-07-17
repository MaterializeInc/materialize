### 1. Create a Materialize user in SQL Server.

Create a user that Materialize will use to connect when ingesting data.

1. In `master`:

   1. Create a login `materialize` (replace `<PASSWORD>` with your own
      password):

      ```sql
      USE master;

      -- Specify additional options per your company's security policy
      CREATE LOGIN materialize WITH PASSWORD = '<PASSWORD>',
      DEFAULT_DATABASE = <DATABASE_NAME>;
      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

   1. Create a user `materialize` for the login and role `materialize_role`:

      ```sql
      USE master;
      CREATE USER materialize FOR LOGIN materialize;
      CREATE ROLE materialize_role;
      ALTER ROLE materialize_role ADD MEMBER materialize;
      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

   1. Grant permissions to the `materialize_role` to enable discovery of the
      tables to be replicated and monitoring replication progress:

      ```sql
      USE master;

      -- Required for schema discovery for replicated tables.
      GRANT SELECT ON INFORMATION_SCHEMA.KEY_COLUMN_USAGE TO materialize_role;
      GRANT SELECT ON INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;
      GRANT SELECT ON OBJECT::INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;

      -- Allows checking the minimum and maximum Log Sequence Numbers (LSN) for CDC,
      -- required for the Source to be able to track progress.
      GRANT EXECUTE ON sys.fn_cdc_get_min_lsn TO materialize_role;
      GRANT EXECUTE ON sys.fn_cdc_get_max_lsn TO materialize_role;
      GRANT EXECUTE ON sys.fn_cdc_increment_lsn TO materialize_role;

      GO -- The GO terminator may be unsupported or unnecessary for your client.
      ```

1. In the database from which which you want to ingest data, create a second
   `materialize` user with `db_datareader` role (replace `<DATABASE_NAME>` with
   your database name):

   ```sql
   USE <DATABASE_NAME>;

   -- Use the same user name as the one created in master
   CREATE USER materialize FOR LOGIN materialize;
   ALTER ROLE db_datareader ADD MEMBER materialize;
   GO -- The GO terminator may be unsupported or unnecessary for your client.
   ```

### 2. Enable Change-Data-Capture for the database.

In SQL Server, for the database from which you want to ingest data, enable
change data capture  (replace `<DATABASE_NAME>` with your database name):

```sql
USE <DATABASE_NAME>;
GO -- The GO terminator may be unsupported or unnecessary for your client.
EXEC sys.sp_cdc_enable_db;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

For guidance on enabling Change Data Capture, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql).

### 3. Enable `SNAPSHOT` transaction isolation.

Enable `SNAPSHOT` transaction isolation for the database (replace
`<DATABASE_NAME>` with your database name):

```sql
ALTER DATABASE <DATABASE_NAME> SET ALLOW_SNAPSHOT_ISOLATION ON;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql)


### 4. Enable Change-Data-Capture for the tables.

Enable Change Data Capture for each table you wish to replicate (replace
`<DATABASE_NAME>`, `<SCHEMA_NAME>`, and `<TABLE_NAME>` with the your database,
schema name, and table name):

```sql
USE <DATABASE_NAME>;

EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = 'materialize_role',
  @supports_net_changes = 0;
GO -- The GO terminator may be unsupported or unnecessary for your client.
```
