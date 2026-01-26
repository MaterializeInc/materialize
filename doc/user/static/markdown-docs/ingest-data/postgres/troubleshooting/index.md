# Troubleshooting

Troubleshooting guides for PostgreSQL source errors in Materialize



This section contains troubleshooting guides for specific errors you may
encounter when using PostgreSQL sources in Materialize. These guides focus on
errors that are unique to the PostgreSQL replication workflow, including issues
with replication slots, WAL management, and other CDC-specific scenarios.

For general data ingestion troubleshooting that applies to all source types, see
the main [Troubleshooting](/ingest-data/troubleshooting/) guide. For answers to
common questions about PostgreSQL sources, see the [FAQ](/ingest-data/postgres/faq/).

## Troubleshooting guides

| Guide | Description |
|-------|-------------|
| [Slot overcompacted](/ingest-data/postgres/slot-overcompacted/) | Resolve errors when PostgreSQL removes WAL data before Materialize can read it |
| [Connection Closed](/ingest-data/postgres/connection-closed/) | Resolve unexpected networking connection terminations between Materialize and PostgreSQL |
