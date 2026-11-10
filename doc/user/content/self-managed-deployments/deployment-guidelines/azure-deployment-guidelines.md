---
title: "Azure deployment guidelines"
description: "General guidelines when deploying Self-Managed Materialize on Azure."
disable_list: true
menu:
  main:
    parent: "deployment-guidelines"
    identifier: "azure-deployment-guidelines"
    weight: 20
aliases:
  - /installation/install-on-azure/appendix-deployment-guidelines/
---

## Recommended instance types

As a general guideline, we recommend:

- ARM-based CPU.
- A 1:8 ratio of vCPU to GiB memory.
- At least a 2:1 ratio of GiB local instance storage to GiB memory when using swap.

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

{{< warning >}}

These VM types provide <red>**ephemeral**</red> local NVMe SSD disks. Data is
<red>**lost**</red> when the VM is stopped or deleted.

{{</ warning >}}

## Locally-attached NVMe storage

Configuring swap on nodes to use locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

The Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure/examples/simple) supports configuring swap out of the box.

## Recommended Azure Blob Storage

Materialize writes **block** blobs on Azure. As a general guideline, we
recommend **Premium block blob** storage accounts.

## Recommended metadata database sizing

{{< include-md file="content/headless/self-managed-deployments/metadata-database-sizing.md" >}}

### Flexible Server SKUs

For the Azure Database for PostgreSQL flexible server that backs the metadata
database, we recommend:

- The **Memory Optimized** tier (E-series), which provides the 1:8
  vCore-to-memory ratio recommended for the metadata database.
- **Zone-redundant high availability** for production.
- **Premium SSD v2** storage, which includes 3,000 IOPS and 125 MB/s at any
  size.

| Deployment size | `sku_name` | vCores / memory | Storage | Provisioned IOPS | Continuously-active objects (~60% CPU) |
|---|---|---|---|---|---|
| Entry / small production | `MO_Standard_E4ds_v5` | 4 / 32 GiB | 128 GiB | 3,000 (included) | ~4,500 |
| Recommended default | `MO_Standard_E16ds_v5` | 16 / 128 GiB | 512 GiB | 6,000 | ~18,000 |

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Upgrading guideline

{{% include-headless "/headless/self-managed-deployments/general-rules-for-upgrades" %}}

## Karpenter node expiry

The [Materialize Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed)
provision nodes with AKS [Node Auto
Provisioning](https://learn.microsoft.com/en-us/azure/aks/node-auto-provisioning)
(NAP), Azure's managed Karpenter.

We recommend setting `expire_after` to `Never` on the Materialize NodePool
since node expiry is not a voluntary disruption. With any other value,
Karpenter removes nodes that reach their configured lifetime even if they run
pods annotated with `karpenter.sh/do-not-disrupt`. This can cause downtime
unless you gracefully roll the nodes first. The `nap-nodepool` module defaults
`expire_after` to `Never`.

## Karpenter termination grace period

We recommend leaving `termination_grace_period` unset on NodePools that run
Materialize workloads. When this value is set, Karpenter terminates nodes after
the configured grace period following any change to the NodePool
configuration, even if they run pods annotated with
`karpenter.sh/do-not-disrupt`. The `nap-nodepool` module leaves
`termination_grace_period` unset by default.

## Node pool resizing

{{% include-headless "/headless/self-managed-deployments/resize-node-pool" %}}
