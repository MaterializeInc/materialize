---
title: "Bin packing"
description: "Pack Materialize clusters onto fewer nodes in a Self-Managed Materialize deployment."
menu:
  main:
    parent: "deployment-guidelines"
    weight: 45
---

Each replica of a Materialize cluster runs as a `clusterd` pod. How these pods
are placed on nodes determines how many nodes your deployment needs. This page
covers three ways to keep that number down:

- [Match cluster sizes to node sizes](#match-cluster-sizes-to-node-sizes), so a
  small cluster doesn't hold a large node.
- [Use `MostAllocated` scheduling](#use-mostallocated-scheduling), so new pods
  fill existing nodes before new nodes are added.
- [Move a replica off a node](#move-a-replica-off-a-node), when a single pod is
  keeping an otherwise idle node running.

## Why Materialize pods aren't consolidated

Node autoscalers save cost by evicting pods from underused nodes and removing
the nodes. Materialize opts its pods out of this: every `environmentd` and
`clusterd` pod carries these annotations:

```yaml
cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
karpenter.sh/do-not-disrupt: "true"
```

A replica restarted by an eviction has to rehydrate, and it can't serve
queries until it has. To avoid that, the cluster autoscaler (including GKE's)
and Karpenter (including AKS Node Auto Provisioning) never remove a node while
a Materialize pod is running on it.

As a result, a Materialize pod stays on the node it was first scheduled to
until it's replaced: by a Materialize rollout, by dropping or resizing its
replica, or by [moving it by hand](#move-a-replica-off-a-node). Where pods land
when they're scheduled decides how well they're packed, and the node
autoscaler can't fix it afterwards.

## Match cluster sizes to node sizes

A pod keeps its node running no matter how little of the node it uses. If a
`25cc` replica is scheduled onto an otherwise empty node sized for `1600cc`
replicas, that node can't be removed until the replica moves.

To prevent this, run separate node pools for small and large cluster sizes, and
use per-size node selectors so each size only schedules onto its pool.

### 1. Create a node pool per size tier

Create one node pool, or one Karpenter `NodePool`, per tier, each with a VM type
suited to the sizes in that tier. Give the nodes in each pool a label that
identifies the tier, for example `materialize.cloud/node-class: small` and
`materialize.cloud/node-class: large`. The label key is up to you.

Keep everything else the Materialize pods need on every pool, such as the
labels and taints for swap or the local-disk scratch filesystem.

The largest size in a tier must fit on one node of that tier's pool, after
leaving room for the daemonsets running on every node.

### 2. Add node selectors to the cluster sizes

In the Materialize operator Helm chart, add `selectors` to each size in
[`operator.clusters.sizes`](/self-managed-deployments/appendix/appendix-cluster-sizes/#custom-cluster-sizes).
Helm merges these into the default size definitions, so you only need to set
`selectors`:

```yaml
operator:
  clusters:
    sizes:
      25cc:
        selectors:
          materialize.cloud/node-class: small
      50cc:
        selectors:
          materialize.cloud/node-class: small
      # ... the other small sizes ...
      800cc:
        selectors:
          materialize.cloud/node-class: large
      # ... the other large sizes ...
```

These selectors are added to the `clusterd` pods of every replica of that size,
together with `clusterd.nodeSelector`. Built-in clusters, such as
`mz_catalog_server`, use small sizes, so they land in the small tier.

### 3. Roll out each Materialize instance

Upgrade the operator release with the new values, then [roll out each
Materialize instance](/self-managed-deployments/upgrading/#rollout-configuration)
so existing replicas are rescheduled with the new selectors. New replicas pick
them up when they're created.

### Avoid overly granular tiers

Two or three tiers are usually enough. Don't create a pool for every size:

- **Capacity:** a node selector is a hard requirement. If a pool can't add a
  node, because its VM type is out of stock in the zone, or you've reached your
  quota, its pods stay `Pending`. They won't fall back to another pool.
- **Packing:** each extra pool has its own partly used nodes. Pools with only
  a few replicas pack worse than one shared pool.
- **Replicas need separate nodes:** Materialize schedules replicas of the same
  cluster onto different nodes. A pool that serves a single size needs at least
  as many nodes as the largest replication factor of the clusters using that
  size.

## Use `MostAllocated` scheduling

When a pod can be placed on several nodes, the Kubernetes scheduler scores the
candidates and picks the best one. By default, the `NodeResourcesFit` plugin
uses the `LeastAllocated` strategy, which prefers the node with the most free
resources. That spreads pods across nodes: every node ends up partly used, and
none of them can be removed while it runs a Materialize pod.

The `MostAllocated` strategy prefers the node with the least free resources
that still fits the pod. New replicas fill existing nodes before the autoscaler
adds new ones, and nodes freed up by dropped or resized replicas tend to empty
completely, so they can be removed. Switching to `MostAllocated` has been shown
to noticeably improve bin packing for Materialize.

Both strategies score nodes by the resources pods _request_, not by what they
actually use.

How you enable `MostAllocated` depends on your platform:

{{< tabs >}}
{{< tab "EKS" >}}

Set the scoring strategy in the EKS cluster's scheduler configuration. In
Terraform, this requires a version of the AWS provider that supports the
`kube_scheduler_config` block:

```hcl
resource "aws_eks_cluster" "example" {
  # ...
  kube_scheduler_config {
    node_resources_fit {
      scoring_strategy {
        type = "MostAllocated"
      }
    }
  }
}
```

This changes the default scheduler for every pod in the cluster.

{{< /tab >}}
{{< tab "GKE" >}}

GKE doesn't expose the scheduler configuration directly. Instead, set the
cluster's autoscaling profile to `OPTIMIZE_UTILIZATION`, which makes the
scheduler prefer the nodes with the most allocated resources:

```hcl
resource "google_container_cluster" "example" {
  # ...
  cluster_autoscaling {
    autoscaling_profile = "OPTIMIZE_UTILIZATION"
  }
}
```

Or, with `gcloud`:

```bash
gcloud container clusters update <cluster-name> \
  --autoscaling-profile optimize-utilization
```

The profile applies to the whole cluster, and it also makes the cluster
autoscaler remove underused nodes sooner. That doesn't affect nodes running
Materialize pods, which [aren't removed](#why-materialize-pods-arent-consolidated),
but other pods, for example on the system node pool, may be moved more often.

{{< /tab >}}
{{< tab "Other platforms" >}}

If your platform doesn't let you change the default scheduler's scoring
strategy, you can run a second scheduler for the Materialize pods:

1. Deploy `kube-scheduler` as a separate scheduler, with a
   [scheduler profile](https://kubernetes.io/docs/reference/scheduling/config/#multiple-profiles)
   that sets the `NodeResourcesFit` scoring strategy to `MostAllocated`.

2. Set `schedulerName` in the Materialize operator Helm chart to the name of
   that profile. The operator then uses it for the `environmentd`, `clusterd`,
   `balancerd`, and `console` pods.

You're responsible for running and upgrading this scheduler. If it's
unavailable, new Materialize pods stay `Pending`.

{{< /tab >}}
{{< /tabs >}}

## Move a replica off a node

Sometimes a single replica keeps a large node running while the rest of the
node sits idle. If other nodes have room for the replica's pod, you can move
it without downtime: add a temporary replica, wait for it to hydrate, then move
the original pod.

### 1. Find the replica on the node

List the pods on the node:

```bash
kubectl get pods -A -o wide --field-selector spec.nodeName=<node-name>
```

`clusterd` pod names include the cluster ID and the replica ID. For example,
`mz9j7stg3ws3-cluster-u5-replica-u12-gen-2-0` runs replica `u12` of cluster
`u5`. Look up their names:

```mzsql
SELECT c.name AS cluster, r.name AS replica, c.replication_factor
FROM mz_catalog.mz_cluster_replicas AS r
JOIN mz_catalog.mz_clusters AS c ON c.id = r.cluster_id
WHERE r.id = 'u12';
```

### 2. Cordon the node

Cordon the node so no pods, including the ones you create in the next steps,
schedule onto it:

```bash
kubectl cordon <node-name>
```

### 3. Add a temporary replica

Increase the cluster's replication factor by one. For a cluster with a
replication factor of `1`:

```mzsql
ALTER CLUSTER <cluster-name> SET (REPLICATION FACTOR = 2);
```

The new replica, `r2`, is scheduled onto another node. If no node has room, the
node autoscaler adds one.

### 4. Wait for the new replica to hydrate

Compare the hydration status of the replicas:

```mzsql
SELECT
  r.name AS replica,
  count(*) AS objects,
  count(*) FILTER (WHERE h.hydrated) AS hydrated
FROM mz_internal.mz_hydration_statuses AS h
JOIN mz_catalog.mz_cluster_replicas AS r ON r.id = h.replica_id
JOIN mz_catalog.mz_clusters AS c ON c.id = r.cluster_id
WHERE c.name = '<cluster-name>'
GROUP BY r.name;
```

The new replica is hydrated once its `objects` and `hydrated` counts equal the
original replica's.

### 5. Delete the original pod

Delete the original replica's pod:

```bash
kubectl delete pod -n <materialize-instance-namespace> <pod-name>
```

Kubernetes recreates the pod on another node, since the original node is
cordoned. The new replica serves queries while the recreated pod rehydrates.
Wait for it to hydrate, using the query from the previous step.

### 6. Remove the temporary replica

Set the replication factor back to its original value:

```mzsql
ALTER CLUSTER <cluster-name> SET (REPLICATION FACTOR = 1);
```

Reducing the replication factor drops the most recently created replicas
first, so this drops the temporary replica `r2`.

### 7. Let the node be removed

Repeat these steps for any other `clusterd` pods on the node. Once no
Materialize pods are left on it, the node autoscaler removes it. If you want to
keep the node instead, uncordon it:

```bash
kubectl uncordon <node-name>
```

Some pods need different handling:

- **Clusters with a replication factor of `2` or more:** skip steps 3, 4, and
  6. The other replicas serve queries while the deleted pod rehydrates.
- **`environmentd`:** you can't move it by deleting its pod without downtime.
  Use a forced rollout instead, as described in [Resize node
  pools](/self-managed-deployments/deployment-guidelines/resize-node-pools/).
- **`balancerd` and `console`:** these can be evicted, so the node autoscaler
  moves them on its own.
- **Unmanaged clusters:** instead of changing the replication factor, create
  the temporary replica with [`CREATE CLUSTER REPLICA`](/sql/create-cluster-replica/)
  and drop it with [`DROP CLUSTER REPLICA`](/sql/drop-cluster-replica/).

## See also

- [Cluster sizes](/self-managed-deployments/appendix/appendix-cluster-sizes/)
- [Resize node pools](/self-managed-deployments/deployment-guidelines/resize-node-pools/)
- [Upgrading](/self-managed-deployments/upgrading/)
