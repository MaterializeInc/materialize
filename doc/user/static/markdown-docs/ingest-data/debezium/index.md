# Debezium
How to propagate Change Data Capture (CDC) data from a database to Materialize using Debezium
You can use [Debezium](https://debezium.io/) to propagate Change Data Capture
(CDC) data to Materialize from databases that are not supported via native
connectors. For PostgreSQL and MySQL databases, we **strongly recommend** using
the native [PostgreSQL](/sql/create-source/postgres/) and [MySQL](/sql/create-source/mysql/)
sources instead.

| Database   | Natively supported? | Integration guide                                                                              |
|------------|---------------------| ---------------------------------------------------------------------------------------------- |
| PostgreSQL | ✓                   | <ul><li>[AlloyDB for PostgreSQL](/ingest-data/postgres/alloydb/)</li><li>[Amazon Aurora for PostgreSQL](/ingest-data/postgres/amazon-aurora/)</li><li>[Amazon RDS for PostgreSQL](/ingest-data/postgres/amazon-rds/)</li><li>[Azure DB for PostgreSQL](/ingest-data/postgres/azure-db/)</li><li>[Google Cloud SQL for PostgreSQL](/ingest-data/postgres/cloud-sql/)</li><li>[Neon](/ingest-data/postgres/neon/)</li><li>[Self-hosted PostgreSQL](/ingest-data/postgres/self-hosted/)</li></ul>
                                                    |
| MySQL      | ✓                   | <ul><li>[Amazon Aurora for MySQL](/ingest-data/mysql/amazon-aurora/)</li><li>[Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/)</li><li>[Azure DB for MySQL](/ingest-data/mysql/azure-db/)</li><li>[Google Cloud SQL for MySQL](/ingest-data/mysql/google-cloud-sql/)</li><li>[Self-hosted MySQL](/ingest-data/mysql/self-hosted/)</li></ul>
                                                       |
| SQL Server | ✓                   | <ul><li>[Self-hosted SQL Server](/ingest-data/sql-server/self-hosted/)</li></ul>
                                                  |
| Oracle     |                     | [Kafka + Debezium](https://debezium.io/documentation/reference/stable/connectors/oracle.html)  |
| MongoDB    |                     | [Kafka + Debezium](/ingest-data/mongodb/) |

### Using Debezium

For databases that are not yet natively supported, like Oracle, SQL Server, or
MongoDB, you can use [Debezium](https://debezium.io/) to propagate Change Data
Capture (CDC) data to Materialize.

<div class="note">
  <strong class="gutter">NOTE:</strong> Currently, Materialize only supports Avro-encoded Debezium records. If you're interested in JSON support, please reach out in the community Slack or submit a <a href="https://github.com/MaterializeInc/materialize/discussions/new?category=feature-requests">feature request</a>.
</div>


Debezium captures row-level changes resulting from `INSERT`, `UPDATE`, and
`DELETE` operations in the upstream database and publishes them as events to
Kafka (and other Kafka API-compatible brokers) using Kafka Connect-compatible
connectors. For more details on CDC support in Materialize, check the
[Kafka source](/sql/create-source/kafka/#debezium-envelope) reference
documentation.
