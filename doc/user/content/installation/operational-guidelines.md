---
title: "Operational guidelines"
description: ""
aliases:
  - /self-hosted/operational-guidelines/
menu:
  main:
    parent: "installation"
    weight: 80
---

## Recommended instance types

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

{{% self-managed/aws-recommended-instances %}}

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

*Additional documentation to come*

{{< tabs >}}
{{< tab  "AWS" >}}

*Starting in v0.3.1 of Materialize on AWS Terraform*, you can [enable disk
support] for Materialize using OpenEBS and NVMe instance storage. With this
change, the following configuration options are available:

- [`enable_disk_support`]
- [`disk_support_config`]

When enabled, the Terraform:

- Installs OpenEBS via Helm;

- Configures NVMe instance store volumes using a bootstrap script;

- Creates appropriate storage classes for Materialize.

By default, [`enable_disk_support`] is set to `true`.
In addition, the default [`node_group_instance_types`] has changed from
`"r8g.2xlarge"` to `"r7gd.2xlarge"`. See [Recommended instance types](#recommended-instance-types).

[enable disk support]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#disk-support-for-materialize

[`enable_disk_support`]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_enable_disk_support

[`disk_support_config`]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_disk_support_config

[`node_group_instance_types`]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_node_group_instance_types

{{</ tab >}}

{{</ tabs >}}

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
