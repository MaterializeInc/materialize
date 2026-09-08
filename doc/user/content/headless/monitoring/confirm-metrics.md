---
headless: true
---

1. Check that the gateway picked up the new configuration and is healthy:

   ```bash
   kubectl -n monitoring rollout status deployment/alloy-gateway
   ```

1. Query the receiving backend for recent samples of a metric you expect, such
   as `mz_dataflow_wallclock_lag_seconds`.

{{< note >}}
A backend's metric summary, schema, or column browser is cumulative, so a metric
listed there is not proof that it is arriving now. It may be left over from
before a configuration change. Query for recent samples instead.
{{< /note >}}

{{< warning >}}
The gateway shards scrape targets across its replicas. During a partial rollout a
metric can look missing simply because its target is being scraped by a pod that
has not picked up the new configuration yet. Let all gateway replicas roll out
before concluding that a metric is being filtered.
{{< /warning >}}
