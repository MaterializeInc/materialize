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
