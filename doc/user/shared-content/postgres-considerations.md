{{< include-md file="shared-content/postgres-known-limitations.md" >}}

### Replication slots

Each source ingests the raw replication stream data for all tables in the
specified publication using **a single** replication slot. To manage
replication slots:

- {{< include-md file="shared-content/postgres-wal.md" >}}

{{< include-md file="shared-content/postgres-remove-unused-replication-slots.md"
>}}
