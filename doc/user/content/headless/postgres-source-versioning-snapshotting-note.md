---
headless: true
---
In private preview, Materialize snapshots a new PostgreSQL table while still
reading the replication stream, so existing tables keep making progress.
Without the feature enabled, existing tables are blocked until the snapshot
completes. Caveats:

- Adding or dropping a table briefly restarts the source dataflow.
- Another restart before the snapshot finishes, such as a resize or another
  table change, pauses existing tables until Materialize catches up on the
  replication stream.
- The snapshot competes with replication for cluster resources. On a
  single-worker size such as `25cc`, existing tables still stall, so use a
  larger size or resize up for the snapshot. Memory use also rises until the new
  table hydrates.

Monitor progress on the source's overview page in the Console. See the [v26.47
release notes](/releases/#v26.47-postgres-add-table-freshness).
