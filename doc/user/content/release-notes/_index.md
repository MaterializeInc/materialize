---
title: "v25.2 Release notes"
description: "Release notes for Self-managed Materialize"
menu:
  main:
    weight: 50
    name: "Release notes"
    identifier: "release-notes"
---
## v25.2.5

### Broader support for service accounts

v25.2.5 allows setting the service account name materialize will use in the
MZ k8s resource. Along with this we allow setting labels and annotations
on service accounts we create. This should allow us to more tightly integrate with
authorization best practices in Azure.

Along with this the AWS specific `environmentd_iam_role_arn` field on the Materialize CRD will be
depreciated. Please set the `eks.amazonaws.com/role-arn` service account annotation instead.



### Pod name Annotation propagation

Cluster and replica names will not propagate into statefulset and pod annoattions.

### Set Security Standards on [Orchestratord](https://github.com/MaterializeInc/materialize/commit/bc86e34d7d2e9022ada697ee5a5e8371a92f6234)

### Fixed correctness bug in Upsert [operator](https://github.com/MaterializeInc/materialize/pull/33283)

### Support for [WITH ORDINALITY](../sql/functions/table-functions/#WITH_ORDINALITY)

## v25.2.4

### DO NOT USE

## v25.2.3

### TLS Support for SQL Server Source

v25.2.3 bumps the `environmentd` version 0.147.4 which supports TLS connections
for SQL Server. For more information, see [Ingest data: SQL
Server](/ingest-data/sql-server/).

### Basic Authentication for HTTP interface

v25.2.3 bumps the `environmentd` version 0.147.4 which supports basic
authentication for the HTTP interface. For more information, see [Integrations
HTTP API](/integrations/http-api).

## v25.2.0

### Support for SQL Server Source

Starting in v25.2, self-managed Materialize adds support for a native SQL Server
source. With the new source, you can replicate data directly into Materialize,
using SQL Server's built-in Change Data Capture (CDC) functionality.

For more information, see [Ingest data: SQL Server](/ingest-data/sql-server/).

### Authentication + RBAC

Starting in v25.2, password authentication and role-based access control are available in self-managed Materialize. For details, see:

- [Password authentication](/manage/authentication), now in public preview.
- [Role-Based Access
  Control](/manage/access-control/#role-based-access-control-rbac).

### Support for `PARTITION BY`

Starting in v25.2, self-managed Materialize adds support for a new `PARTITION
BY` option for materialized views and tables. `PARTITION BY` allows you to
control how Materializes internally groups and stores your data. This can lead to
dramatically faster query performance and rehydration times for certain
workloads.

For details, see [Partitioning and filter
pushdown](/transform-data/patterns/partition-by/).

### Console Improvements

Self-managed v25.2 also includes various improvements to the Console:

- Consolidating source lag under Freshness metrics
- Copy button for queries
- Syntax error highlighting

### Improved `EXPLAIN`

The following improvements to `EXPLAIN` are now available in v25.2:

- New [`EXPLAIN`](/sql/explain-plan/) output format to highlight the most
  important parts of your query.

- New [`EXPLAIN ANALYZE`](/sql/explain-analyze/) to help you understand
  long-running queries.

For more information, see the [`EXPLAIN`](/sql/explain-plan/) and [`EXPLAIN
ANALYZE`](/sql/explain-analyze/) reference pages.

## Known Limitations

| Item                                    | Status      |
|-----------------------------------------|-------------|
| **License Compliance** <br> License key support to make it easier to comply with license terms. | In progress |
| **Network Policies** <br> Materialize Network policies are not yet supported. | |
| **AWS Connections** <br> AWS connections require backing cluster that hosts Materialize to be AWS EKS.  | |
| **EKS/Azure Connections** | |
| **Temporal Filtering** <br> Memory optimizations for filtering time-series data are not yet implemented. | |

## See also
