---
title: "Query History"
description: "How query history and statement logging are configured in self-managed Materialize deployments"
menu:
  main:
    parent: "sm-deployments"
    weight: 72
---

The Materialize Console includes a **Query History** view, under its
[**Monitoring**](/console/monitoring/) section, that lists a sample of the SQL
statements recently issued to your Materialize instance, along with their
duration, status, and the cluster that ran them. Query history is available in
self-managed deployments and is enabled by default in Materialize operator chart
v26.40.0 and later. Earlier chart versions disabled statement logging outright,
and the Query History view stays empty on them.

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

## Configure statement logging

Two system parameters bound how much query history you collect. The Materialize
operator Helm chart ships a value for each:

| Parameter | Chart value | Default | Bounds |
|-----------|-------------|---------|--------|
| `statement_logging_max_sample_rate` | `operator.args.statementLoggingMaxSampleRate` | `0.99` | The fraction of executions considered for logging. |
| `statement_logging_target_data_rate` | `operator.args.statementLoggingTargetDataRate` | Unset, so `environmentd`'s own 2071 | The sustained bytes per second written. Must be greater than 0. |

Statement logging is therefore on by default, sampling nearly every statement up
to that byte rate.

A session can request its own rate with `SET statement_logging_sample_rate`. The
rate that applies to a statement is the smaller of the session's rate and the
system cap, so lowering the cap reduces logging for every session regardless of
what individual sessions ask for.

Sampling is not the only limit, and it is not the one that bounds volume.
Materialize also throttles statement logging to the target byte rate and drops
sampled executions that would exceed it. So a sample rate of `1.0` does not
guarantee every statement is recorded, and on a busy instance the byte rate is
what determines how much history you actually accumulate.

The two parameters are not interchangeable. Lower the data rate to hold storage
growth down. Lowering the sample rate instead makes query history less
representative without lowering the ceiling on what statement logging stores,
because the byte rate is already the binding limit.

{{< note >}}
The chart also enables `enableInternalStatementLogging`, which logs statements
run by Materialize's internal users: `mz_system`, `mz_support`, and
`mz_analytics`. This covers both Materialize's own activity, such as the
Console's catalog queries, and any session you open as one of those users, and
it counts toward the cost described below.

Statements from your own users are always subject to sampling and are unaffected
by this setting. Logging in through the Console as a normal user is sampled at
the rates above either way.
{{< /note >}}

Either parameter can be set through the Helm chart or as a system parameter, and
those two paths interact, so read the note on precedence below before picking
one.

### Using the Helm chart

Set either value when installing or upgrading the operator. For example, to
halve how fast query history grows:

```shell
helm upgrade my-materialize-operator materialize/materialize-operator \
  --set operator.args.statementLoggingTargetDataRate=1035
```

Or, in your `values.yaml`:

```yaml
operator:
  args:
    statementLoggingMaxSampleRate: 0.99
    statementLoggingTargetDataRate: 1035
```

Setting `statementLoggingMaxSampleRate` to `0` disables statement logging
entirely. Already-logged statements remain visible until they age out of the
24-hour window. Setting either value to `null` inherits `environmentd`'s own
default, `0.99` for the sample rate and 2071 bytes per second for the data rate.
The data rate must be greater than 0.

The operator passes these values to `environmentd` as the *defaults* for
`statement_logging_max_sample_rate` and
`statement_logging_target_data_rate`, so they only take effect when
`environmentd` restarts. Upgrading the operator does not by itself roll out your Materialize
instances: you also need to request a rollout, as described in [modifying the
custom
resource](/self-managed-deployments/#modifying-the-custom-resource). For the
full list of chart values, see [Materialize Operator
Configuration](/self-managed-deployments/operator-configuration/).

### Using system parameters

Because the chart values are only defaults, you can override either at runtime
without a rollout, through the `system-params.json` ConfigMap described in
[Configuring System
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
      "statement_logging_target_data_rate": 1035
    }
```

Or with [`ALTER SYSTEM SET`](/sql/alter-system-set/), connected as the
`mz_system` user:

```mzsql
ALTER SYSTEM SET statement_logging_target_data_rate = 1035;
```

{{< warning >}}
Setting `statement_logging_max_sample_rate` to `0` this way turns off statement
logging for the whole instance, and the Console's Query History view stops
recording new statements. Unlike the Helm chart value, this takes effect
immediately and without a rollout, so it is easy to disable query history
without meaning to. Use `SET statement_logging_sample_rate` in a session if you
only want to stop logging your own statements.
{{< /warning >}}

{{< note >}}
A value set through the ConfigMap or `ALTER SYSTEM SET` is stored in the catalog
and takes precedence over the Helm chart value, which is only a default. While
such an override is in place, editing the corresponding chart value has no
effect.

To go back to the chart-provided value, first remove the parameter from the
ConfigMap, then run [`ALTER SYSTEM
RESET`](/sql/alter-system-reset/) for it. Removing it from the ConfigMap alone is
not enough, because the last synced value remains in the catalog. Resetting it
while it is still in the ConfigMap is also not enough, because the sync loop
reapplies it.
{{< /note >}}

To check the values currently in effect:

```mzsql
SHOW statement_logging_max_sample_rate;
SHOW statement_logging_target_data_rate;
```

## Cost of statement logging

Statement logging has two distinct costs, and each is governed by a different
parameter:

- **CPU on `environmentd`**, governed by the sample rate. Every logged execution
  is prepared and written by the control plane, so the overhead scales with your
  statement throughput, not with your data volume. Instances serving many short
  queries pay the most.

- **Storage**, governed by the target data rate. Logged statements, including
  their SQL text, consume space in your blob storage and metadata backend.
  Although `mz_recent_activity_log` only surfaces the last 24 hours, the
  underlying statement history collections are never truncated, so their
  footprint grows for the lifetime of the instance.

That second point is the one to plan around: query history is not a fixed-size
buffer, and its growth rate is set by `statement_logging_target_data_rate`. On
an instance with limited storage, lower that parameter. Reach for the sample
rate only when you want to reduce `environmentd` CPU overhead, and expect less
representative history in exchange.

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
