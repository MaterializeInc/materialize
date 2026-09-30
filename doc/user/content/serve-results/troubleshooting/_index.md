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

Statements are sampled and throttled, so not every statement appears in the
log. In Materialize Cloud, Materialize controls the sample rate and may change
it at any time. In self-managed deployments, the operator sets it.

### Control the sample rate on Materialize Self-Managed

The fraction of statements logged is the smaller of two values:

| Parameter | Scope | Description |
|-----------|-------|-------------|
| `statement_logging_max_sample_rate` | System | Upper bound for every session. `0` disables statement logging. |
| `statement_logging_sample_rate` | Session | Rate the session requests. New sessions start at `statement_logging_default_sample_rate`. |

A higher sample rate makes the log more complete, which helps when you need to
find a specific slow query. The tradeoff is CPU overhead on `environmentd`, which
grows with statement throughput. Separately,
`statement_logging_target_data_rate` caps the bytes written per second, so on a
busy instance some sampled statements are still dropped even at a rate of `1.0`.

To change the system-wide rates, connect as the `mz_system` user and run:

```mzsql
ALTER SYSTEM SET statement_logging_max_sample_rate = 0.5;
ALTER SYSTEM SET statement_logging_default_sample_rate = 0.5;
```

To log every statement in your own session while you debug, run:

```mzsql
SET statement_logging_sample_rate = 1.0;
```

To check the rates in effect:

```mzsql
SHOW statement_logging_max_sample_rate;
SHOW statement_logging_sample_rate;
```

`ALTER SYSTEM SET` takes effect immediately and overrides the Helm chart value.
For the Helm chart settings, storage costs, and how to revert an override, see
[Query History](/self-managed-deployments/query-history/).

For a complete history of DDL statements, use
[`mz_audit_events`](/sql/system-catalog/mz_catalog/#mz_audit_events).
