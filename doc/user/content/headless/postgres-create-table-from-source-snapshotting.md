---
headless: true
---
Creating a table from a source starts the
[snapshotting](/ingest-data/#snapshotting) process.

{{% include-headless "/headless/ingestion/snapshotting-definition" %}}

You cannot query the table until its snapshot completes.

{{< private-preview >}}Ingesting existing tables while a new PostgreSQL table snapshots{{< /private-preview >}}

{{< note >}}

{{% include-headless "/headless/postgres-source-versioning-snapshotting-note" %}}

{{< /note >}}
