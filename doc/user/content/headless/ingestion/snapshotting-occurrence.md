---
headless: true
---
When snapshotting occurs depends on the syntax.

- With the legacy [`CREATE SOURCE ... FOR <ALL
  TABLES|TABLES|SCHEMAS>`](/sql/create-source/#legacy-syntax), you run a single
  statement to create both the source and the tables that ingest data.
  Snapshotting begins when you run the statement. For an existing source, the
  legacy [`ALTER SOURCE ... ADD SUBSOURCE`](/sql/alter-source/) starts the
  snapshotting for the added table.

- With the source-versioning syntax, you create the source and its tables
  separately using [`CREATE SOURCE ...`](/sql/create-source/#new-syntax) and
  [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/). Snapshotting begins when
  you run `CREATE TABLE ... FROM SOURCE`.
