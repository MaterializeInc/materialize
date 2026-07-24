---
title: "Resize node pools"
description: "Safely change the VM type of a node pool in a Self-Managed Materialize deployment."
menu:
  main:
    parent: "deployment-guidelines"
    weight: 40
aliases:
  - /self-managed-deployments/upgrading/resize-node-pools/
---

When you need a larger (or smaller) VM type for the nodes that Materialize
runs on, how to proceed depends on how the nodes are managed:

- **Static node pools** (the GCP and Azure modules, and AWS node groups when
  not using Karpenter) cannot change VM type in place. The underlying cloud
  APIs do not support it, so the Terraform providers mark the VM type field
  `ForceNew` (GKE: `machine_type`, AKS: `vm_size`, EKS node groups:
  `instance_types`), and changing it plans a `destroy + create`. The destroy
  fails if the pool still has Materialize pods on it, because nothing in the
  Terraform graph migrates the workloads to a replacement pool first. The
  supported pattern is to **add a second pool, taint the old pool so no new
  pods schedule on it, trigger a Materialize rollout so the new generation of
  pods lands on the new pool, then drop the old pool**.

- **Karpenter-managed nodes** (the default for Materialize nodes in the AWS
  modules) size nodes per pod rather than per pool. Changing the VM type is a
  template change on the Karpenter `NodePool`, followed by a Materialize
  rollout to move the pods onto new-spec nodes.

## Steps

{{< tabs >}}
{{< tab "Terraform" >}}

These steps apply to the [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed).

{{< tabs >}}
{{< tab "GCP" >}}

### 1. Declare a second node pool with the new VM type

Add a new nodepool module instance alongside the existing one, keeping the old
pool unchanged. Copy the existing configuration, then change:

- The `prefix`, so the pool gets a distinct name.
- The `machine_type`.
- For a swap-enabled pool, a distinct `disk_setup_name` (e.g.
  `disk-setup-xl`). It names the disk-setup namespace and daemonset, which
  otherwise collide with the old pool's.
- The `local_ssd_count`, if the new machine type bundles a different number of
  local SSDs (`c4a-highmem-16-lssd` has 4, for example).

Keep the same labels and taints as the existing pool so Materialize pods are
eligible to schedule on it. For example:

```hcl
module "materialize_nodepool" {
  # ... existing pool config, unchanged ...
  machine_type = "c4a-highmem-8-lssd"
}

module "materialize_nodepool_xl" {
  # ... copy of the existing config, with a new prefix ...
  machine_type    = "c4a-highmem-16-lssd"
  local_ssd_count = 4
  disk_setup_name = "disk-setup-xl"
}
```

Run `terraform init` to pick up the new module instance, then apply:

```bash
terraform init
terraform apply
```

Both pools now exist. Materialize pods have not yet been scheduled on the new
pool.

### 2. Taint the old pool so no new pods schedule on it

Add a decommission taint to the old pool's `node_taints`:

```hcl
node_taints = [
  # ... existing taints ...
  {
    key    = "materialize.cloud/decommissioned"
    value  = "true"
    effect = "NO_SCHEDULE"
  }
]
```

Taints update in place (no pool replacement) on the provider versions the
modules require. Running pods are not evicted, but no new pods schedule to the
old pool, and the cluster autoscaler will not scale it up for pending pods,
since they don't tolerate the taint. Use a taint key the Materialize pods
don't tolerate (not `materialize.cloud/workload` or `kubernetes.io/arch`).

Apply:

```bash
terraform apply
```

### 3. Roll out the Materialize instance

With the old pool tainted, a forced rollout lands the new generation of pods
on the new pool.

{{% include-headless "/headless/self-managed-deployments/force-rollout-terraform" %}}

Apply:

```bash
terraform apply
```

See [Rollout behavior](#rollout-behavior) for what to expect during the
rollout. Verify the new `environmentd` and `clusterd` pods are only scheduled
onto the new pool.

### 4. Remove the old pool

Once the rollout has completed, the old pool's nodes have no Materialize
workloads on them. Remove the old nodepool module instance from your Terraform
configuration and apply:

```bash
terraform apply
```

The destroy step now succeeds because the pool has no running workloads.

### 5. Optional: rename the new pool back

If you want the pool to keep the original name (for example because other
Terraform or kubectl tooling references it), repeat these steps with a third
pool that carries the original name. Otherwise, accept the new name and update
any references.

{{< /tab >}}
{{< tab "Azure" >}}

### 1. Declare a second node pool with the new VM type

Add a new nodepool module instance alongside the existing one, keeping the old
pool unchanged. Copy the existing configuration, then change:

- The `prefix`, so the pool gets a distinct name. The AKS node pool name is
  the prefix with dashes removed, truncated to 12 characters, so the new
  prefix must differ from the old one within those characters. A suffix
  appended to a long prefix is silently truncated away and the apply fails
  because the pool name already exists.
- The `vm_size`.
- For a swap-enabled pool, a distinct `disk_setup_name` (e.g.
  `disk-setup-xl`). It names the disk-setup namespace and daemonset, which
  otherwise collide with the old pool's.

Keep the same labels and taints as the existing pool so Materialize pods are
eligible to schedule on it. For example:

```hcl
module "materialize_nodepool" {
  # ... existing pool config, unchanged ...
  prefix  = "mzpool"
  vm_size = "Standard_E4pds_v6"
}

module "materialize_nodepool_xl" {
  # ... copy of the existing config ...
  prefix          = "mzpoolxl"
  vm_size         = "Standard_E8pds_v6"
  disk_setup_name = "disk-setup-xl"
}
```

Run `terraform init` to pick up the new module instance, then apply:

```bash
terraform init
terraform apply
```

Both pools now exist. Materialize pods have not yet been scheduled on the new
pool.

### 2. Taint the old pool so no new pods schedule on it

Add a decommission taint to the old pool's `node_taints`:

```hcl
node_taints = [
  # ... existing taints ...
  {
    key    = "materialize.cloud/decommissioned"
    value  = "true"
    effect = "NO_SCHEDULE"
  }
]
```

Taints update in place (no pool replacement) on the provider versions the
modules require. Running pods are not evicted, but no new pods schedule to the
old pool, and the cluster autoscaler will not scale it up for pending pods,
since they don't tolerate the taint. Use a taint key the Materialize pods
don't tolerate (not `materialize.cloud/workload` or `kubernetes.io/arch`).

Apply:

```bash
terraform apply
```

### 3. Roll out the Materialize instance

With the old pool tainted, a forced rollout lands the new generation of pods
on the new pool.

{{% include-headless "/headless/self-managed-deployments/force-rollout-terraform" %}}

Apply:

```bash
terraform apply
```

See [Rollout behavior](#rollout-behavior) for what to expect during the
rollout. Verify the new `environmentd` and `clusterd` pods are only scheduled
onto the new pool.

### 4. Remove the old pool

Once the rollout has completed, the old pool's nodes have no Materialize
workloads on them. Remove the old nodepool module instance from your Terraform
configuration and apply:

```bash
terraform apply
```

The destroy step now succeeds because the pool has no running workloads.

### 5. Optional: rename the new pool back

If you want the pool to keep the original name (for example because other
Terraform or kubectl tooling references it), repeat these steps with a third
pool that carries the original name. Otherwise, accept the new name and update
any references.

{{< /tab >}}
{{< tab "AWS" >}}

{{< warning >}}
Prior to v6.0.0 of the Terraform modules, the nodepools had `termination_grace_period`
set to `300s`. This caused nodes to be terminated five minutes after any change to the
nodepool configuration, ignoring the `karpenter.sh/do-not-disrupt` annotation on pods.

If you are running an older version of the Terraform modules, to avoid downtime,
we recommend upgrading to at least v6.0.0, following the steps in the
[upgrade notes](https://github.com/MaterializeInc/materialize-terraform-self-managed/blob/v6.0.0/README.md#v600).

On later versions, the `termination_grace_period` is unset by default.
We recommend keeping it unset on nodepools for Materialize workloads.
{{</ warning >}}

The AWS modules provision Materialize nodes with Karpenter, so there is no
second pool to create. Karpenter provisions new-spec nodes on demand.

If you have disabled Karpenter and run Materialize on a static EKS node
group, follow the blue-green pattern from the GCP and Azure tabs instead,
changing `instance_types` on a second node group module instance.

### 1. Update the instance types

Change `instance_types` on the Materialize `karpenter-ec2nodeclass` and
`karpenter-nodepool` module instances and apply:

```hcl
module "ec2nodeclass_materialize" {
  # ...
  instance_types = ["r7gd.4xlarge"]
}

module "nodepool_materialize" {
  # ...
  instance_types = ["r7gd.4xlarge"]
}
```

```bash
terraform apply
```

Karpenter marks the existing nodes as drifted but does not drain them: the
`environmentd` and `clusterd` pods carry the `karpenter.sh/do-not-disrupt`
annotation, which blocks voluntary disruption while they run. This relies on
the nodepool's `expire_after` being `Never`, see [Karpenter node
expiry](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/#karpenter-node-expiry).

### 2. Cordon the drifted nodes

Cordon the existing Materialize nodes so the rollout's new pods cannot
schedule onto them and instead trigger Karpenter to provision nodes with the
new instance types:

```bash
# The nodepool name is the `name` input of the karpenter-nodepool module
# ("materialize" in the examples).
for node in $(kubectl get nodes -l karpenter.sh/nodepool=materialize -o name); do
  kubectl cordon "$node"
done
```

Nodes that Karpenter provisions after this point are not cordoned.

### 3. Roll out the Materialize instance

{{% include-headless "/headless/self-managed-deployments/force-rollout-terraform" %}}

Apply:

```bash
terraform apply
```

Karpenter provisions new-spec nodes for the pending pods of the new
generation. See [Rollout behavior](#rollout-behavior) for what to expect
during the rollout. Verify the new `environmentd` and `clusterd` pods are
only scheduled onto the new nodes.

### 4. Verify the old nodes are removed

Once the rollout has completed and the old generation's pods are gone, the
cordoned nodes are empty and Karpenter consolidates them away (the modules
configure `consolidationPolicy: WhenEmpty`). Confirm they disappear:

```bash
kubectl get nodes -l karpenter.sh/nodepool=materialize
```

{{< /tab >}}
{{< /tabs >}}

{{< /tab >}}
{{< tab "Legacy Terraform" >}}

The legacy Terraform modules
([terraform-aws-materialize](https://github.com/MaterializeInc/terraform-aws-materialize),
[terraform-google-materialize](https://github.com/MaterializeInc/terraform-google-materialize),
and
[terraform-azurerm-materialize](https://github.com/MaterializeInc/terraform-azurerm-materialize))
are no longer supported. Migrate to the [new Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
first, then follow the steps in the **Terraform** tab.

{{< /tab >}}
{{< tab "Manual" >}}

If you manage your infrastructure without the Materialize Terraform modules:

### 1. Create a second node pool with the new VM type

Using your cloud provider's tooling, create a second node pool with the same
labels and taints as the existing pool (so Materialize pods are eligible to
schedule on it), a distinct name, and the new VM type.

If your nodes are managed by Karpenter, skip this step and instead update the
instance requirements on the Karpenter `NodePool`. Karpenter provisions
new-spec nodes on demand once the old nodes are cordoned.

### 2. Keep new pods off the old nodes

For a static pool, add a decommission taint, such as
`materialize.cloud/decommissioned=true:NoSchedule`, to the old pool. Apply the
taint at the node pool level through your cloud provider rather than with
`kubectl taint`: node-level taints do not carry over to nodes the cluster
autoscaler adds to the pool later. Running pods are not evicted, but no new
pods schedule to the old pool, and the cluster autoscaler will not scale it
up for pending pods, since they don't tolerate the taint. Use a taint key the
Materialize pods don't tolerate (not `materialize.cloud/workload` or
`kubernetes.io/arch`).

For Karpenter-managed nodes, cordon the old nodes instead. Nodes that
Karpenter provisions afterwards are not cordoned.

### 3. Roll out the Materialize instance to land new pods on the new nodes

Use the Materialize CR's rollout machinery to have the operator create a new
generation of `environmentd` and `clusterd` pods. Because the Materialize
spec itself is unchanged (the node move happens at the Kubernetes cluster
level and not in the Materialize CR), you need to force the rollout.

{{< tabs >}}
{{< tab "Materialize CRD v1" >}}

Set `forceRollout` to a new UUID:

```bash
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"forceRollout\": \"$(uuidgen)\"}}"
```

{{< /tab >}}
{{< tab "Materialize CRD v1alpha1" >}}

Set both `requestRollout` and `forceRollout` to the same new UUID:

```bash
UUID="$(uuidgen)"
kubectl patch materialize <instance-name> \
  -n <materialize-instance-namespace> \
  --type='merge' \
  -p "{\"spec\": {\"requestRollout\": \"$UUID\", \"forceRollout\": \"$UUID\"}}"
```

{{< /tab >}}
{{< /tabs >}}

See [Rollout behavior](#rollout-behavior) for what to expect. Verify the new
`environmentd` and `clusterd` pods are only scheduled onto the new nodes.

### 4. Remove the old pool

Once the rollout has completed, the old nodes have no Materialize workloads
on them. Delete the old node pool using your cloud provider's tooling. For
Karpenter-managed nodes, empty drifted nodes are consolidated away
automatically.

{{< /tab >}}
{{< /tabs >}}

## Rollout behavior

The default rollout strategy, `WaitUntilReady`, creates the new generation
alongside the old, waits for it to catch up, then promotes it and tears down
the old generation. This briefly doubles the resource footprint during the
rollout (so make sure the new nodes have the capacity) but otherwise incurs
minimal downtime. For other rollout strategies (manual promotion,
immediate-with-downtime), see
[Rollout Configuration](/self-managed-deployments/upgrading/#rollout-configuration).

Watch the rollout progress:

```bash
kubectl get materialize <instance-name> -n <materialize-instance-namespace> -w
kubectl get pods -n <materialize-instance-namespace> -o wide
```

You should see the new generation pods come up on the new nodes, the
`UpToDate` condition flip to `True`, and the old generation pods get
terminated.

## Why not change the VM type in place

For static node pools, it's tempting to update the existing pool's
`machine_type` / `vm_size` / `instance_types` and re-apply. The Terraform
plan correctly shows `destroy + create`, but the apply gets stuck on the
destroy because the pool still has running pods that nothing has moved off.
You end up with an error like:

```
cannot update node types in pool
```

The pattern above avoids the wedge by bringing up a replacement pool first and
using the operator's rollout machinery to migrate the workloads, instead of
relying on `kubectl drain` or the cloud provider's pool deletion logic to do
the right thing on its own.

## See also

- [AWS deployment guidelines](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/)
- [Azure deployment guidelines](/self-managed-deployments/deployment-guidelines/azure-deployment-guidelines/)
- [GCP deployment guidelines](/self-managed-deployments/deployment-guidelines/gcp-deployment-guidelines/)
- [Upgrading](/self-managed-deployments/upgrading/) -- rollout configuration
  reference (`requestRollout`, `rolloutStrategy`)
