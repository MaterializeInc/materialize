---
headless: true
---
When you add a table to an existing source ([`CREATE TABLE ... FROM
SOURCE`](/sql/create-table/postgres/) or [`ALTER SOURCE ... ADD
SUBSOURCE`](/sql/alter-source/)), Materialize starts the snapshotting process
for the new table.

{{< private-preview >}}Ingesting existing tables while a new PostgreSQL table snapshots{{< /private-preview >}}

{{< note >}}

{{% include-headless "/headless/postgres-source-versioning-snapshotting-note" %}}

{{< /note >}}

With the feature enabled, if you drop the new table before its snapshot
finishes, the source restarts and the remaining tables resume from their own
committed position without replaying data. Materialize discards the partial
snapshot.
