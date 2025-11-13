---
title: "Guide: Upgrading to v26 and enabling swap"
description: "Upgrade procedure when upgrading to v26 which has swap enabled by default."
menu:
  main:
    parent: "installation"
    weight: 69
---

Swap allows for infrequently accessed data to be moved from memory to disk. Enabling swap reduces the memory required to operate Materialize and improves cost efficiency. Upgrades to v26 and later have swap enabled by default.

## Upgrading to v26 with swap requires node preparation
We've added new labels to the node selectors for clusterd pods to enable smooth upgrades. As a result, your existing nodes will not match these selectors and won't be selected to run the pods. Before upgrading to v26, you must prepare your nodes by adding the required labels.

## Preparing for the upgrade using terraform
v0.6.1 of the Materialize terraform modules can handle much of the preparation work for you. If using our terraform modules, please follow the instructions provided in their respctive upgrade notes:
- [AWS](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061)
- [GCP](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061)
- [Azure](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061)

## Preparing for the upgrade without terraform

1. Label existing scratchfs/lgalloc node groups

   If using lgalloc on scratchfs volumes, you must add the additional `"materialize.cloud/scratch-fs": "true"` label to your existing node groups and nodes running Materialize workloads.

   Adding this label to the node group (or nodepool) configuration will apply the label to newly spawned nodes, but depending on your cloud provider may not apply the label to existing nodes.

   If not automatically applied, you may need to use `kubectl label` to apply the change to existing nodes.

1. Modify existing scratchfs/lgalloc disk setup daemonset selector labels

    If using our [ephemeral-storage-setup image](https://github.com/MaterializeInc/ephemeral-storage-setup-image/) as a daemonset to configure scratchfs LVM volumes for lgalloc, you must add the additional `"materialize.cloud/scratch-fs": "true"` label to multiple places:
    * `spec.selector.matchLabels`
    * `spec.template.metadata.labels`
    * (if using `nodeAffinity`) `spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms`
    * (if using `nodeSelector`) `spec.template.spec.nodeSelector`

    You **must** use at least one of `nodeAffinity` or `nodeSelector`.

    It is recommended to rename this daemonset to make it clear that it is only for the legacy scratchfs/lgalloc nodes (for example, change the name `disk-setup` to `disk-setup-scratchfs`).

1. Create a new node group for swap

    1. Create a new node group (or ec2nodeclass and nodepool if using Karpenter in AWS) using an instance type with local NVMe disks. If in GCP, the disks must be in `raw` mode.

    1. Label the node group with `"materialize.cloud/swap": "true"`.

    1. If using AWS Bottlerocket AMIs (highly recommended if running in AWS), set the following in the userdata to configure the disks for swap, and enable swap in the kubelet:

        ```toml
        [settings.oci-defaults.resource-limits.max-open-files]
        soft-limit = 1048576
        hard-limit = 1048576

        [settings.bootstrap-containers.diskstrap]
        source = "docker.io/materialize/ephemeral-storage-setup-image:v0.4.0"
        mode = "once"
        essential = "true"
        # ["swap", "--cloud-provider", "aws", "--bottlerocket-enable-swap"]
        user-data = "WyJzd2FwIiwgIi0tY2xvdWQtcHJvdmlkZXIiLCAiYXdzIiwgIi0tYm90dGxlcm9ja2V0LWVuYWJsZS1zd2FwIl0="

        [kernel.sysctl]
        "vm.swappiness" = "100"
        "vm.min_free_kbytes" = "1048576"
        "vm.watermark_scale_factor" = "100"
        ```

    1. If not using AWS or not using Bottlerocket AMIs, and your node group supports it (Azure does not as of 2025-11-05), add a startup taint. This taint will be removed after the disk is configured for swap.

        ```yaml
        taints:
          - key: startup-taint.cluster-autoscaler.kubernetes.io/disk-unconfigured
            value: "true"
            effect: NoSchedule
        ```

1. Create a new disk-setup-swap daemonset

    If using Bottlerocket AMIs in AWS, you may skip this step, as you should have configured swap using userdata previously.

    Create a new daemonset using our [ephemeral-storage-setup image](https://github.com/MaterializeInc/ephemeral-storage-setup-image/) to configure the disks for swap and to enable swap in the kubelet.

    The arguments to the init container in this daemonset need to be configured for swap. See the examples in the linked git repository for more details.

    This daemonset should run only on the new swap nodes, so we need to ensure it has the `"materialize.cloud/swap": "true"` label in several places:

    * `spec.selector.matchLabels`
    * `spec.template.metadata.labels`
    * (if using `nodeAffinity`) `spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms`
    * (if using `nodeSelector`) `spec.template.spec.nodeSelector`

    You **must** use at least one of `nodeAffinity` or `nodeSelector`.

    It is recommended to name this daemonset to clearly indicate that it is for configuring swap (ie: `disk-setup-swap`), as opposed to other disk configurations.

1. (Optional) Configure environmentd to also use swap

    Swap is enabled by default for clusterd, but not for environmentd. If you'd like to enable swap for environmentd, add `"materialize.cloud/swap": "true"` to the `environmentd.node_selector` helm value.

1. Upgrade the Materialize operator helm chart to v26

    The cluster size definitions for existing Materialize instances will not be changed at this point, but any newly created Materialize instances, or upgraded Materialize instances will pick up the new sizes.

    Do not create any new Materialize instances at versions less than v26, or perform any rollouts to existing Materialize instances to versions less than v26.

1. Upgrade existing Materialize instances to v26

    The new v26 pods should go to the new swap nodes.

    You can verify that swap is enabled and working by `exec`ing into a clusterd pod and running `cat /sys/fs/cgroup/memory.swap.max`. If you get a number greater than 0, swap is enabled and the pod is allowed to use it.

## How to disable swap
If you wish to opt out of swap and retain the old behavior, you may set `operator.clusters.swap_enabled: false` in your helm values.

1. (Optional) Delete old scratchfs/lgalloc node groups and disk-setup-scratchfs daemonset

    If you no longer have anything running on the old scratchfs/lgalloc nodes, you may delete their node group and the disk-setup-scratchfs daemonset.
