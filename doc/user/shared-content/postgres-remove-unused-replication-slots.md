
- If you stop using Materialize, or if either the Materialize instance or the
PostgreSQL instance crash, delete any replication slots. You can query the
`mz_internal.mz_postgres_sources` table to look up the name of the replication
slot created for each source.

- If you delete all objects that depend on a source without also dropping the
source, the upstream replication slot remains and will continue to accumulate
data so that the source can resume in the future. To avoid unbounded disk space
usage, make sure to use [`DROP SOURCE`](/sql/drop-source/) or manually delete
the replication slot.
