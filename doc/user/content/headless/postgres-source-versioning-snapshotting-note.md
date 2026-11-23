---
headless: true
---
In private preview starting in v26.47, Materialize can snapshot a new table
without blocking ingestion for existing tables. This uses more memory and CPU.
Single-worker replica sizes (`25cc` or `50cc`) might not have enough capacity
to keep both the new table and existing tables fresh. Use a source cluster size
of `100cc` or larger, or temporarily resize it up for the snapshot. See the
[release notes](/releases/#v26.47-postgres-add-table-freshness).

Without this feature, snapshotting a new table blocks ingestion for existing
tables. To speed up the snapshot, consider sizing up the cluster. You can
monitor snapshot progress on the source's overview page in the Materialize
Console.
