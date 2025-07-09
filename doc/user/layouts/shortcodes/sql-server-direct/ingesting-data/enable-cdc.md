### 1. Create a Materialize User for the database.

Create a user with permissions to read the table for which you will enable Change Data Capture.

```sql
  CREATE LOGIN materialize WITH PASSWORD = 'AStrongPassword',
    DEFAULT_DATABASE = <DATABASE_NAME>,
    -- Either disable password expiry OR
    -- ensure that you rotate the password after it expires
    -- to ensure that the `Source` will continue to work.
    CHECK_EXPIRATION = OFF,
    CHECK_POLICY = ON;
  GO
```

Create a materialize user with the same name as the login to allow setting the user as 'materialize' when creating the `CONNECTION`.
```sql
  CREATE USER materialize FOR LOGIN materialize;
  GO
```

Create a role for the materialize user to limit permissions to just the necessary set for the materialize user.
```sql
  CREATE ROLE materialize_role;
  GO
```

Add the materialize user to the materialize role.
```sql
  ALTER ROLE materialize_role ADD MEMBER materialize;
  GO
```

### 2. Enable Change-Data-Capture for the database.

Before creating a source in Materialize, you **must** configure your SQL Server
database for change data capture. This requires running the following stored procedures:

```sql
EXEC sys.sp_cdc_enable_db;
GO
```

For guidance on enabling Change Data Capture, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-db-transact-sql).

Note, on AWS the following command is required:

```sql
EXEC msdb.dbo.rds_cdc_enable_db '<DATABASE_NAME>';
GO
```

For further guidance see the [AWS documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.SQLServer.CommonDBATasks.CDC.html).

### 3. Enable `SNAPSHOT` transaction isolation.

In addition to enabling Change-Data-Capture you **must** also enable your
`SNAPSHOT` transaction isolation in your SQL Server database. This requires running
the following SQL:

```sql
ALTER DATABASE <DATABASE_NAME> SET ALLOW_SNAPSHOT_ISOLATION ON;
```

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL Server documentation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server).

### 4. Enable Change-Data-Capture for database tables.

You **must** enable Change Data Capture for each table you wish to replicate.

```sql
EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = '<ROLE_FROM_MZ_CONNECTION>',
  @supports_net_changes = 0;
GO
```

For guidance on enabling `SNAPSHOT` transaction isolation, see the [SQL Server documentation](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql)

### 5. Add permissions to read from the Change-Data-Capture of the database tables.

Finally, add permissions to the materialize user to enable reading from the Change Data Capture for each table.

```sql
-- Allows checking the minimum and maximum Log Sequence Numbers (LSN) for CDC.
GRANT EXECUTE ON sys.fn_cdc_get_min_lsn TO materialize_role;
GRANT EXECUTE ON sys.fn_cdc_get_max_lsn TO materialize_role;
GRANT EXECUTE ON sys.fn_cdc_increment_lsn TO materialize_role;

-- Allows cleaning up CDC change tables.
GRANT EXECUTE ON sys.sp_cdc_cleanup_change_table TO materialize_role;

-- Allows viewing the database state to check if CDC and snapshot isolation are enabled.
GRANT VIEW DATABASE STATE TO materialize_role;

-- Required for metadata about tables, schemas, and columns.
GRANT SELECT ON sys.tables TO materialize_role;
GRANT SELECT ON sys.schemas TO materialize_role;
GRANT SELECT ON sys.columns TO materialize_role;
GRANT SELECT ON sys.types TO materialize_role;
GRANT SELECT ON sys.objects TO materialize_role;

-- Required for CDC metadata.
GRANT SELECT ON cdc.change_tables TO materialize_role;
GRANT SELECT ON cdc.lsn_time_mapping TO materialize_role;

-- Required for primary key information.
GRANT SELECT ON INFORMATION_SCHEMA.KEY_COLUMN_USAGE TO materialize_role;
GRANT SELECT ON INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;

-- Required to use the SCHEMA_NAME() function. Repeat for each relevant schema.
GRANT VIEW DEFINITION ON SCHEMA::<SCHEMA_NAME> TO materialize_role;

GRANT SELECT ON cdc.<SCHEMA_NAME>_<TABLE_NAME>_CT TO materialize_role;
GRANT SELECT ON SCHEMA::cdc TO materialize_role;
GRANT SELECT ON OBJECT::INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO materialize_role;
GO
```
