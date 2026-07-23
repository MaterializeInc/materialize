---
headless: true
---
{{% public-preview %}}
Cluster autoscaling
{{% /public-preview %}}

When you create an index or materialized view, or when a cluster restarts, the
cluster must [hydrate](/concepts/clusters/#consider-hydration-requirements) the
affected objects before they can serve results. Hydration reads the input data
and rebuilds in-memory state, and its speed scales with the cluster
[size](#available-sizes).

The `AUTO SCALING STRATEGY (ON HYDRATION)` option lets a cluster **automatically provision an
extra burst replica at a larger size while it has un-hydrated objects**. This
speeds up hydration without manually scaling the cluster up before hydration and
back down afterward. The steady-size replicas continue hydrating in parallel with the burst replica. 
Once one of the steady-size replicas catches up with the burst, the burst replica lingers for `LINGER DURATION`, and the burst replica is then removed.
The burst replica is an ordinary cluster replica, billed only for the time it is
provisioned. See [Usage & billing](/administration/billing/) for details.

The burst can also speed up deployments that hydrate new objects, especially
[blue/green deployments](/manage/blue-green/), where a new cluster must hydrate
before the cutover.

`AUTO SCALING STRATEGY (ON HYDRATION)` is only available on **managed clusters**. It is not
supported on unmanaged clusters, and it cannot be combined with a cluster
`SCHEDULE` other than the default `MANUAL`.

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

You can specify the following options:

Option | Description
-------|------------
`HYDRATION SIZE` | The [size](#available-sizes) of the burst replica provisioned while the cluster has un-hydrated objects. Must differ from the cluster's steady `SIZE`. Choose a larger size to speed up hydration.
`LINGER DURATION` | Optional. How long the burst replica lingers after a steady-size replica catches up, before it is removed. Default: `0s`.

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
