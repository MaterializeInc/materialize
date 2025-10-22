To create a source from SQL Server 2016+, you must first:

- **Configure upstream SQL Server instance**
  - Create a replication user and password for Materialize to use to connect.
  - For each database you wish to replicate
    - Enable Change Data Capture for the database.
    - Enable `SNAPSHOT` isolation for the database.
    - Enable Change Data Capture for specific tables you wish to replicate.
- **Configure network security**
  - Ensure Materialize can connect to your SQL Server instance.
- **Create a connection to SQL Server in Materialize**
  - The connection setup depends on the network security configuration.

For details, see the [SQL Server integration
guides](/ingest-data/sql-server/#integration-guides).
