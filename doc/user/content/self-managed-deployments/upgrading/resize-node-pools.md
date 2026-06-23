---
title: "Resize node pools"
description: "Safely change the VM type of a node pool in a Self-Managed Materialize deployment."
menu:
  main:
    parent: "upgrading"
    weight: 25
---

When you need a larger (or smaller) VM type for a node pool that Materialize
runs on, the change cannot be applied in place. The underlying Kubernetes node
group resources on all three clouds treat the VM type as **immutable**:

- GKE: `google_container_node_pool.node_config.machine_type`
- AKS: `azurerm_kubernetes_cluster_node_pool.vm_size`
- EKS: `aws_eks_node_group.instance_types`

Changing the value triggers Terraform to plan a `destroy + create`. The destroy
step fails if the pool still has Materialize pods running on it, because nothing
in the Terraform graph migrates the workloads to a replacement pool first.

The supported pattern is to **add a second pool, trigger a Materialize rollout
so the new generation of pods lands on it, then drop the old pool**.

{{< note >}}
This guide applies to deployments that use static node groups (the default for
the GCP and Azure modules, and for the AWS modules when Karpenter is disabled).
If you're using [Karpenter](https://karpenter.sh/) for dynamic node provisioning,
resizing is just a `NodePool` template change because Karpenter sizes nodes per
pod rather than per pool.
{{</ note >}}

## Steps

### 1. Declare a second node pool with the new VM type

Add a second node pool alongside the existing one. Give it the same labels and
taints as the existing pool so Materialize pods are eligible to schedule on it,
but a distinct name and the new VM type.

The exact shape depends on which module or resource you're using. For example,
with `terraform-google-modules/kubernetes-engine` on GCP:

```hcl
module "gke" {
  # ...
  node_pools = [
    {
      name         = "materialize"
      machine_type = "n2-highmem-8"
      # ...
    },
    {
      name         = "materialize-xl"
      machine_type = "n2-highmem-16"
      # ... same labels and taints as above ...
    },
  ]
}
```

Or, if you're using a single-pool module wrapper, instantiate it twice:

```hcl
module "materialize_nodepool" {
  # ... existing pool config ...
  machine_type = "n2-highmem-8"
}

module "materialize_nodepool_xl" {
  # ... copy the existing config, change name and machine_type ...
  machine_type = "n2-highmem-16"
}
```

The Azure and AWS equivalents change `vm_size` (Azure) or `instance_types` (AWS)
instead of `machine_type`.

Apply. Both pools now exist; the new one is empty.

### 2. Cordon the old pool so new pods schedule on the new one

```bash
# Cordon every node in the old pool so the scheduler stops placing new pods there
for node in $(kubectl get nodes -l <your-old-pool-label> -o name); do
  kubectl cordon "$node"
done
```

DaemonSets and existing pods stay in place; only new pods are kept off the
cordoned nodes.

### 3. Roll out the Materialize instance to land new pods on the new pool

Use the Materialize CR's rollout machinery to have the operator create a new
generation of `environmentd` and `clusterd` pods. With the old pool cordoned,
the new generation schedules onto the new pool's nodes.

If you're using the `materialize-instance` Terraform module, bump the
`force_rollout` (and `request_rollout`) inputs to a new UUID and apply:

```hcl
module "materialize_instance" {
  # ...
  rollout_strategy = "WaitUntilReady"  # default
  request_rollout  = "00000000-0000-0000-0000-000000000002"  # any new UUID
  force_rollout    = "00000000-0000-0000-0000-000000000002"  # any new UUID
}
```

```bash
terraform apply
```

If you're managing the Materialize CR directly, the equivalent kubectl
command is:

```bash
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$(uuidgen)\", \"forceRollout\": \"$(uuidgen)\"}}"
```

Both paths set `requestRollout` and `forceRollout` to new UUIDs, which is what
the operator watches for. `forceRollout` is needed because the spec isn't
otherwise changing (the node pool move happens at the cluster level, not in
the CR).

The default `rolloutStrategy` is `WaitUntilReady`, which creates the new
generation alongside the old, waits for it to catch up, then promotes it and
tears down the old generation. This briefly doubles the resource footprint
during the rollout (so make sure the new pool has the capacity) but is
otherwise zero-downtime. For other rollout strategies (manual promotion,
immediate-with-downtime), see
[Rollout Configuration](/self-managed-deployments/upgrading/#rollout-configuration).

Watch the rollout progress:

```bash
kubectl get materialize <instance-name> -n <materialize-instance-namespace> -w
kubectl get pods -n <materialize-instance-namespace> -o wide
```

You should see the new generation pods come up on the new pool's nodes, the
`UpToDate` condition flip to `True`, and the old generation pods get
terminated.

### 4. Remove the old pool

Once the rollout has completed, the old pool's nodes have no Materialize
workloads on them. Remove the original node pool entry (or module call) from
your Terraform configuration and apply.

The destroy step now succeeds because the pool has no running workloads.

### 5. Optional: rename the new pool back

If you want the pool to keep the original name (for example because other
Terraform or kubectl tooling references it), repeat the same pattern in
reverse: add a third pool with the original name, roll out onto it, then drop
the renamed pool. Otherwise, accept the new name and update any references.

## Why not change the VM type in place

It's tempting to update the existing pool's `machine_type` / `vm_size` /
`instance_types` and re-apply. The Terraform plan correctly shows `destroy +
create`, but the apply gets stuck on the destroy because the pool still has
running pods that nothing has moved off. You end up with an error like:

```
cannot update node types in pool
```

The pattern above avoids the wedge by bringing up a replacement pool first and
using the operator's rollout machinery to migrate the workloads, instead of
relying on `kubectl drain` or the cloud provider's pool deletion logic to do
the right thing on its own.

## See also

- [Upgrading](/self-managed-deployments/upgrading/) -- rollout configuration
  reference (`requestRollout`, `rolloutStrategy`)
- [Install on Azure](/self-managed-deployments/installation/install-on-azure/)
- [Install on GCP](/self-managed-deployments/installation/install-on-gcp/)
- [Install on AWS](/self-managed-deployments/installation/install-on-aws/)
