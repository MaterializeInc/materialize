### Schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

{{% schema-changes %}}

### Publication membership

PostgreSQL's logical replication API does not provide a signal when users remove
tables from publications. Because of this, Materialize relies on periodic checks
to determine if a table has been removed from a publication, at which time it
generates an irrevocable error, preventing any values from being read from the
table.

However, it is possible to remove a table from a publication and then re-add it
before Materialize notices that the table was removed. In this case, Materialize
can no longer provide any consistency guarantees about the data we present from
the table and, unfortunately, is wholly unaware that this occurred.

To mitigate this issue, if you need to drop and re-add a table to a publication,
ensure that you remove the table/subsource from the source _before_ re-adding it
using the [`DROP SOURCE`](/sql/drop-source/) command.

### Supported types

{{< include-md file="shared-content/postgres-supported-types.md" >}}

{{< include-md file="shared-content/postgres-unsupported-type-handling.md" >}}

### Truncation

Upstream tables replicated into Materialize should not be truncated. If an
upstream table is truncated while replicated, the whole source becomes
inaccessible and will not produce any data until it is recreated. Instead of
truncating, you can use an unqualified `DELETE` to remove all rows from the
table:

```mzsql
DELETE FROM t;
```

### Inherited tables

When using [PostgreSQL table inheritance](https://www.postgresql.org/docs/current/tutorial-inheritance.html),
PostgreSQL serves data from `SELECT`s as if the inheriting tables' data is also
present in the inherited table. However, both PostgreSQL's logical replication
and `COPY` only present data written to the tables themselves, i.e. the
inheriting data is _not_ treated as part of the inherited table.

PostgreSQL sources use logical replication and `COPY` to ingest table data, so
inheriting tables' data will only be ingested as part of the inheriting table,
i.e. in Materialize, the data will not be returned when serving `SELECT`s from
the inherited table.

You can mimic PostgreSQL's `SELECT` behavior with inherited tables by creating a
materialized view that unions data from the inherited and inheriting tables
(using `UNION ALL`). However, if new tables inherit from the table, data from
the inheriting tables will not be available in the view. You will need to add
the inheriting tables via `ADD SUBSOURCE` and create a new view (materialized or
non-) that unions the new table.

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html
