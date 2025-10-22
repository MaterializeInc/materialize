To create a source from MySQL 5.7+, you must first:

- **Configure upstream MySQL instance**
  - Enable GTID-based binlog replication
  - Create a replication user and password for Materialize to use to connect.
- **Configure network security**
  - Ensure Materialize can connect to your MySQL instance.
- **Create a connection to MySQL in Materialize**
  - The connection setup depends on the network security configuration.

For details, see the [MySQL integration
guides](/ingest-data/mysql/#integration-guides).
