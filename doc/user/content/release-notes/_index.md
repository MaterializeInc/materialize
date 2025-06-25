---
title: "Release notes"
description: "Release notes for Self-managed Materialize"
menu:
  main:
    weight: 50
    name: "Release notes"
    identifier: "release-notes"
---

## v25.2

### Support for SQL Server Source

Starting in v25.2, self-managed Materialize adds support for a native SQL Server
source. With the new source, you can replicate data directly into Materialize,
using SQL Server's built-in Change Data Capture (CDC) functionality.

For more information, see [Ingest data: SQL Server](/ingest-data/sql-server/).

### Authentication + RBAC

Starting in v25.2, password authentication and role-based access control are available in self-managed Materialize. For details, see:

- [Password authentication](/manage/authentication).
- [Role-Based Access
  Control](/manage/access-control/#role-based-access-control-rbac).

## Support for `PARTITION BY`

Starting in v25.2, self-managed Materialize adds support for a new `PARTITION
BY` option for materialized views and tables. `PARTITION BY` allows you to
control how Materializes interally groups and stores your data. This can lead to
dramatically faster query performance and rehydration times for certain
workloads.

For details, see [Partioning and filter
pushdown](/transform-data/patterns/partition-by/).

## Console Improvements

Self-managed v25.2 also includes various improvements to the Console:

- Consolidating source lag under Freshness metrics
- Copy button for queries
- Syntax error highlighting

## Improved `EXPLAIN`

The following improvements to `EXPLAIN` are now available in v25.2:

- New [`EXPLAIN`](/sql/explain-plan/) output format to highlight the most
  important parts of your query.

- New [`EXPLAIN ANALYZE`](/sql/explain-analyze/) to help you understand
  long-running queries.

For more information, see the [`EXPLAIN`](/sql/explain-plan/) and [`EXPLAIN
ANALYZE`](/sql/explain-analyze/) reference pages.

## Self-managed versioning and lifecycle

Self-managed Materialize uses a calendar versioning (calver) scheme of the form
`vYY.R.PP` where:

- `YY` indicates the year.
- `R` indicates major release.
- `PP` indicates the patch number.

For Self-managed Materialize, Materialize supports the latest 2 major releases.
