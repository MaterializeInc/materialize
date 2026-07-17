{{< public-preview >}}
Cluster autoscaling
{{< /public-preview >}}

When you create an index or materialized view, or when a cluster restarts, the
cluster must [hydrate](/concepts/clusters/#consider-hydration-requirements) the
affected objects before they can serve results. Hydration reads the input data
and rebuilds in-memory state, and its speed scales with the cluster
[size](#available-sizes).

The `AUTO SCALING STRATEGY` option lets a cluster **burst to a larger size while
it has un-hydrated objects**, then automatically return to its steady size once
hydration completes. This speeds up hydration without permanently paying for a
larger cluster.

With the `ON HYDRATION` strategy, whenever the cluster has un-hydrated objects,
Materialize provisions an extra replica at the configured `HYDRATION SIZE`
alongside the steady-size replicas. The burst replica hydrates faster and can
serve results before the steady replicas finish. Once the steady replicas
hydrate, the burst replica lingers for `LINGER DURATION` and is then removed.

```mzsql
CREATE CLUSTER fast_start (
    SIZE = '100cc',
    AUTO SCALING STRATEGY = (
        ON HYDRATION (
            HYDRATION SIZE = '800cc',
            LINGER DURATION = '15s'
        )
    )
);
```

The `AUTO SCALING STRATEGY` option accepts the following:

Option | Description
-------|------------
`HYDRATION SIZE` | The [size](#available-sizes) to burst to while the cluster has un-hydrated objects. Must differ from the cluster's steady `SIZE`. Choose a larger size to speed up hydration.
`LINGER DURATION` | Optional. How long the burst replica lingers after the steady-size replicas hydrate, before it is removed. Default: `0s`.

{{< note >}}
The burst replica is an ordinary cluster replica and is billed as such for as
long as it runs. See [Usage & billing](/administration/billing/) for details.
{{< /note >}}

Provisioning the burst replica requires enough compute capacity to run it. In
Materialize Self-Managed, this means your Kubernetes cluster must have enough
spare resources (for example, available nodes) to schedule the burst replica.

The burst is best-effort and never blocks the cluster: if the burst replica
cannot be provisioned, the steady-size replicas still come up and hydrate as
usual, as long as there are enough resources for them. The burst replica is
always cleaned up, even if it was never provisioned.

To remove the autoscaling strategy from a cluster, use `ALTER CLUSTER ... RESET
(AUTO SCALING STRATEGY)` or set an empty strategy with `AUTO SCALING STRATEGY =
()`.

You can inspect the configured strategy and any in-flight burst in the
[`mz_internal.mz_cluster_auto_scaling_strategies`](/reference/system-catalog/mz_internal/#mz_cluster_auto_scaling_strategies)
catalog view.
