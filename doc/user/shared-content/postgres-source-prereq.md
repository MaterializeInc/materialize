To create a source from PostgreSQL 11+, you must first:

- **Configure upstream PostgreSQL instance**
  - Set up logical replication.
  - Create a publication.
  - Create a replication user and password for Materialize to use to connect.
- **Configure network security**
  - Ensure Materialize can connect to your PostgreSQL instance.
- **Create a connection to PostgreSQL in Materialize**
  - The connection setup depends on the network security configuration.

For details, see the [PostgreSQL integration
guides](/ingest-data/postgres/#integration-guides).
