---
title: "Troubleshooting"
description: "Troubleshooting guides for queries that are slow, unresponsive, or expensive in Materialize."
disable_list: true
menu:
  main:
    name: "Troubleshoot query performance"
    identifier: transform-troubleshooting
    parent: serve-results
    weight: 90
aliases:
  - /transform-data/troubleshooting/
---

This section contains troubleshooting guides for queries that don't perform as
expected. Each guide starts from a symptom, helps you find the cause with SQL
against the system catalog, and describes how to fix it.

For problems with maintaining indexes and materialized views, such as high
memory or CPU on a cluster, see [Dataflow
troubleshooting](/transform-data/dataflow-troubleshooting/). For problems with
sources, see [Troubleshoot ingestion](/ingest-data/troubleshooting/).

## Troubleshooting guides

| Guide | Description |
|-------|-------------|
| [Slow queries](/serve-results/troubleshooting/slow-queries/) | Find which stage of a query's lifecycle takes the most time, and fix it. Includes a section for self-managed deployments. |
| [Unresponsive queries](/serve-results/troubleshooting/unresponsive-queries/) | Find why a query hangs or never returns, and cancel it. |
| [Expensive queries](/serve-results/troubleshooting/expensive-queries/) | Find queries that use a lot of CPU or memory on a cluster, and reduce their cost. |

## Query history

All three guides use the statement log, which records a sample of the SQL
statements issued against Materialize in the last **24 hours**. You can browse
it in the **Query history** tab of the [Materialize
console](/developer-tools/console/), or query it through
[`mz_internal.mz_recent_activity_log`](/sql/system-catalog/mz_internal/#mz_recent_activity_log).

To query the statement log, connect as a *superuser* or as a user granted the
[`mz_monitor` role](/security/appendix/appendix-built-in-roles/#system-catalog-roles).

Statements are sampled, so not every statement appears in the log. In
Materialize Cloud, the default and maximum sample rate for most organizations
is 99%, and Materialize may change it at any time. In self-managed deployments,
the operator sets the sample rate. See [Query
History](/self-managed-deployments/query-history/).

For a complete history of DDL statements, use
[`mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events).
