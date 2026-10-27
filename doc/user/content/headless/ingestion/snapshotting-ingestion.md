---
headless: true
---

<!--
Generic (syntax-agnostic) version of the "existing tables blocked" behavior.
Keep in sync with the syntax-specific version in
headless/ingestion/snapshotting-queries.md.
-->

When you create additional tables for a source that already has tables ingesting
data, ingestion for the existing tables is blocked while the new tables
snapshot. The existing tables remain queryable, but they stop advancing until
the new tables' snapshots complete.
