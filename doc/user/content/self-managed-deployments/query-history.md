---
title: "Query History"
description: "How query history and statement logging are configured in self-managed Materialize deployments"
menu:
  main:
    parent: "sm-deployments"
    weight: 72
---

The Materialize Console includes a **Query History** view, under its
[**Monitoring**](/console/monitoring/) section, that lists the SQL statements
recently issued to your Materialize instance along with their duration, status,
and the cluster that ran them. Query history is available in self-managed
deployments and is enabled by default.

Query history is backed by *statement logging*: Materialize records a randomly
sampled fraction of statement executions into the system catalog, most visibly
[`mz_recent_activity_log`](/reference/system-catalog/mz_internal/#mz_recent_activity_log),
which covers the last 24 hours. Sampling means the view is a representative
sample of your workload rather than a complete audit log. For a complete record
of DDL, use
[`mz_audit_events`](/reference/system-catalog/mz_catalog/#mz_audit_events)
instead.

To see query history in the Console, connect as a Materialize *superuser* or as
a user granted the [`mz_monitor`
role](/security/appendix/appendix-built-in-roles/#system-catalog-roles).

## Default sample rate

The Materialize operator Helm chart sets the
`operator.args.statementLoggingMaxSampleRate` value to `0.1`, so roughly 10% of
statement executions are sampled.

This value is lower than `environmentd`'s own default of `0.99`. Statement
logging costs CPU on `environmentd` and storage for the retained history, and
that overhead is most noticeable on the small instances typical of self-managed
deployments, so the chart samples a fraction of statements instead.

The rate that actually applies to a statement is the smaller of two parameters:

| Parameter | Scope | Meaning |
|-----------|-------|---------|
| `statement_logging_sample_rate` | Session | The rate a session asks for. A session may `SET` this to any value between 0 and 1, but only values below the cap have any effect. |
| `statement_logging_max_sample_rate` | System | A cap on the above. The chart value sets this. |

Because the system parameter is a cap, lowering it reduces logging for every
session regardless of what individual sessions request.

Sampling is not the only limit. Materialize also throttles statement logging to
a target byte rate, `statement_logging_target_data_rate`, so executions beyond
that budget are dropped even at a high sample rate. A sample rate of `1.0`
therefore does not guarantee that every statement is recorded.

{{< note >}}
Unlike Materialize Cloud, self-managed deployments log statements issued by the
`mz_system` user by default, because the chart enables
`enableInternalStatementLogging`. Internal activity, including the Console's own
catalog queries, is sampled alongside your workload and counts toward the cost
described below.
{{< /note >}}

## Tune the sample rate

There are two places to change the maximum sample rate. They interact, so read
the note on precedence below before picking one.

### Using the Helm chart

Set `operator.args.statementLoggingMaxSampleRate` when installing or upgrading
the operator:

```shell
helm upgrade my-materialize-operator materialize/materialize-operator \
  --set operator.args.statementLoggingMaxSampleRate=0.5
```

Or, in your `values.yaml`:

```yaml
operator:
  args:
    statementLoggingMaxSampleRate: 0.5
```

Setting the value to `0` disables statement logging entirely. Already-logged
statements remain visible until they age out of the 24-hour window. Setting the
value to `null` inherits `environmentd`'s default of `0.99`.

The operator passes this value to `environmentd` as the *default* for
`statement_logging_max_sample_rate`, so it only takes effect when `environmentd`
restarts. Upgrading the operator does not by itself roll out your Materialize
instances: you also need to request a rollout, as described in [modifying the
custom
resource](/self-managed-deployments/#modifying-the-custom-resource). For the
full list of chart values, see [Materialize Operator
Configuration](/self-managed-deployments/operator-configuration/).

### Using system parameters

Because the chart value is only a default, you can override it at runtime
without a rollout, either through the `system-params.json` ConfigMap described
in [Configuring System
Parameters](/self-managed-deployments/configuration-system-parameters/):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "statement_logging_max_sample_rate": 0.5
    }
```

Or with [`ALTER SYSTEM SET`](/sql/alter-system-set/), connected as the
`mz_system` user:

```mzsql
ALTER SYSTEM SET statement_logging_max_sample_rate = 0.5;
```

{{< note >}}
A value set through the ConfigMap or `ALTER SYSTEM SET` is stored in the catalog
and takes precedence over the Helm chart value, which is only a default. While
such an override is in place, editing
`operator.args.statementLoggingMaxSampleRate` has no effect.

To go back to the chart-provided value, first remove the parameter from the
ConfigMap, then run [`ALTER SYSTEM
RESET`](/sql/alter-system-reset/) for it. Removing it from the ConfigMap alone is
not enough, because the last synced value remains in the catalog. Resetting it
while it is still in the ConfigMap is also not enough, because the sync loop
reapplies it.
{{< /note >}}

To check the rate currently in effect:

```mzsql
SHOW statement_logging_max_sample_rate;
```

## Cost of raising the sample rate

Raising the sample rate gives more complete query history at the cost of:

- **CPU on `environmentd`**: every logged execution is prepared and written by
  the control plane, so the overhead scales with your statement throughput, not
  with your data volume. Instances serving many short queries pay the most.

- **Storage**: logged statements, including their SQL text, are retained in the
  system catalog and consume space in your blob storage and metadata backend.
  Although `mz_recent_activity_log` only surfaces the last 24 hours, the
  underlying statement history collections are not truncated, so their footprint
  grows for the lifetime of the instance.

Workloads dominated by a high rate of small, fast queries are the ones where a
high sample rate is most likely to be felt. If you raise the rate, do it
incrementally and watch `environmentd` CPU utilization and your storage
footprint.

## See also

- [Console monitoring](/console/monitoring/)
- [`mz_internal` statement logging
  relations](/reference/system-catalog/mz_internal/#mz_recent_activity_log)
- [`ALTER SYSTEM SET`](/sql/alter-system-set/)
- [`ALTER SYSTEM RESET`](/sql/alter-system-reset/)
- [Configuring System
  Parameters](/self-managed-deployments/configuration-system-parameters/)
- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
