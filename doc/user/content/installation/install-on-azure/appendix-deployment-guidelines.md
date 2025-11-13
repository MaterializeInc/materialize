---
title: "Appendix: Azure deployment guidelines"
description: "Azure environment setup/deployment guidelines"
menu:
  main:
    parent: "install-on-azure"
    identifier: "azure-deployment-guidelines"
    weight: 40
---

## Recommended instance types

As a general guideline, we recommend:

- Processor Type: ARM-based CPU

- Sizing: 2:1 disk-to-RAM ratio with spill-to-disk enabled.

### Recommended Azure VM Types with Local NVMe Disks

When operating on Azure in production, we recommend [Epdsv6
sizes](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/memory-optimized/epdsv6-series?tabs=sizebasic#sizes-in-series)
Azure VM Types with Local NVMe Disk:

| VM Size            | vCPUs | Memory  | Ephemeral Disk | Disk-to-RAM Ratio |
| ------------------ | ----- | ------- | -------------- | ----------------- |
| Standard_E2pds_v6  | 2     | 16 GiB  | 75 GiB         | ~4.7:1           |
| Standard_E4pds_v6  | 4     | 32 GiB  | 150 GiB        | ~4.7:1           |
| Standard_E8pds_v6  | 8     | 64 GiB  | 300 GiB        | ~4.7:1           |
| Standard_E16pds_v6 | 16    | 128 GiB | 600 GiB        | ~4.7:1           |
| Standard_E32pds_v6 | 32    | 256 GiB | 1,200 GiB      | ~4.7:1           |

{{< important >}}

These VM types provide **ephemeral** local NVMe SSD disks. Data is lost
when the VM is stopped or deleted.

{{</ important >}}

See also [Locally attached NVMe storage](#locally-attached-nvme-storage).

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

Starting in v0.6.1 of Materialize on Azure Terraform,
disk support (using swap on NVMe instance storage) may be enabled for Materialize.
With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

- The following configuration options are available:

  - [`swap_enabled`]

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061)

## Recommended Azure Blob Storage

Materialize writes **block** blobs on Azure. As a general guideline, we
recommend **Premium block blob** storage accounts.

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)

[`enable_disk_support`]: https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#disk-support-for-materialize-on-azure

[`disk_support_config`]:
    https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_disk_support_config

[`disk_setup_image`]:
    https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_disk_setup_image
