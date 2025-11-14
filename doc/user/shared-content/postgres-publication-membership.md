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
