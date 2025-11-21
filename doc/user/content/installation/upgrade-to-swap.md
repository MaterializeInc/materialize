---
title: "Appendix: Upgrade to swap"
description: "Upgrade procedure for swap support if not using Materialize Terraform."
menu:
  main:
    parent: "installation"
    weight: 95
    identifier: "upgrade-to-swap"
---

{{< annotation type="Disambiguation" >}}

This page outlines the general steps for upgrading to v25.2.16 (the minimum
version needed to upgrade to v26.0) if you are <red>**not**</red> using
Materialize provided Terraforms.

If you are using Materialize-provided Terraforms, `v0.6.1` of the Terraforms
handle the preparation for you.  If using Materialize-provided Terraforms,
upgrade your Terraform version to `v0.6.1` and follow the Upgrade notes:

- [AWS Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061)

- [GCP Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061)

- [Azure Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061)

See also [General notes for upgrades](/installation/#upgrade).

{{< /annotation >}}

{{< include-md file="shared-content/self-managed/prepare-nodes-and-upgrade.md"
>}}

Starting in v25.2.16, Self-Managed Materialize enables swap by default. Swap
allows for infrequently accessed data to be moved from memory to disk. Enabling
swap reduces the memory required to operate Materialize and improves cost
efficiency.

To facilitate upgrades, Self-Managed Materialize added new labels to the node
selectors for `clusterd` pods. To upgrade, you must prepare your nodes with the
new labels. This guide provides general instructions for preparing for swap and
upgrading to v25.2.16 if you are <red>**not**</red> using the
Materialize-provided Terraforms.

## Upgrade to v25.2.16 without Materialize-provided Terraforms

{{< tip >}}
{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md"
>}}

See also [General notes for upgrades](/installation/#general-notes-for-upgrades)
{{< /tip >}}

1. Label existing scratchfs/lgalloc node groups.

   If using lgalloc on scratchfs volumes, add the additional
   `"materialize.cloud/scratch-fs": "true"` label to your existing node groups
   and nodes running Materialize workloads.


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

1. Upgrade the Materialize operator helm chart to v25.2.16.

    The cluster size definitions for existing Materialize instances will not be changed at this point, but any newly created Materialize instances, or upgraded Materialize instances will pick up the new sizes.

    Do not create any new Materialize instances at versions less than v25.2.16
   (`environmentd` 0.147.20) or perform any rollouts to existing Materialize
   instances to versions less than v25.2.16 (`environmentd` 0.147.20).

1. Upgrade existing Materialize instances to `v0.147.20`.

    The new pods should go to the new swap nodes.

    You can verify that swap is enabled and working by `exec`ing into a clusterd pod and running `cat /sys/fs/cgroup/memory.swap.max`. If you get a number greater than 0, swap is enabled and the pod is allowed to use it.

1. (Optional) Delete old scratchfs/lgalloc node groups and disk-setup-scratchfs daemonset

    If you no longer have anything running on the old scratchfs/lgalloc nodes, you may delete their node group and the disk-setup-scratchfs daemonset.

## How to disable swap

If you wish to opt out of swap and retain the old behavior, you may set
`operator.clusters.swap_enabled: false` in your Helm values.
