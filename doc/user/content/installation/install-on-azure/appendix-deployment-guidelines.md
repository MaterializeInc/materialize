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

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory is recommended.
- When using swap, it is recommended to use a 8:1 ratio of GiB local instance storage to GiB Ram.

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

## Locally-attached NVMe storage

Configuring swap on nodes to using locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

***New Unified Terraform***

The unified Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/simple) supports configuring swap out of the box.

***Legacy Terraform***

The Legacy Terraform provider, adds preliminary swap support in v0.6.1, via the [`swap_enabled`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_swap_enabled) variable.
With this change, the Terraform:
  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061).

{{< note >}}
If deploying `v25.2` Materialize clusters will not automatically use swap unless they are configured with a `memory_request` less than their `memory_limit`. In `v26` this will be handled automatically.
{{< /note >}}

## Recommended Azure Blob Storage

Materialize writes **block** blobs on Azure. As a general guideline, we
recommend **Premium block blob** storage accounts.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
