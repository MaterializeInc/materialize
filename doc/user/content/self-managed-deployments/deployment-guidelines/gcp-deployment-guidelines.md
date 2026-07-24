---
title: "GCP deployment guidelines"
description: "General guidelines when deploying Self-Managed Materialize on GCP."
disable_list: true
menu:
  main:
    parent: "deployment-guidelines"
    identifier: "gcp-deployment-guidelines"
    weight: 30
aliases:
  - /installation/install-on-gcp/appendix-deployment-guidelines/
---

## Recommended instance types

As a general guideline, we recommend:

- ARM-based CPU.
- A 1:8 ratio of vCPU to GiB memory.
- At least a 2:1 ratio of GiB local instance storage to GiB memory when using swap.

When operating on GCP in production, we recommend the Arm-based [C4A
high-memory series]. Both C4A and C4 offer local SSDs only on their `-lssd`
machine variants, which bundle a fixed number of Titanium SSD disks.

| Series | Examples   |
| ------ | ---------- |
| [C4A high-memory series] (recommended) | `c4a-highmem-16-lssd` or `c4a-highmem-32-lssd` |
| [C4 high-memory series] | `c4-highmem-16-lssd` or `c4-highmem-32-lssd` |

C4A is not available in every region. Where it is unavailable, use the
x86-based [C4 high-memory series] instead.

To maintain the recommended disk-to-RAM ratio for your machine type, see
[Number of local SSDs](#number-of-local-ssds) to determine the number of local
SSDs to use.

See also [Locally attached NVMe storage](#locally-attached-nvme-storage).

## Number of local SSDs

Each local SSD in GCP provides 375GB of storage. Use the appropriate number
of local SSDs to ensure your total disk space is at least twice the amount of RAM in your
machine type for optimal Materialize performance.

C4A and C4 bundle a fixed number of Titanium SSD disks in each `-lssd`
machine variant. The count is not configurable, but every high-memory `-lssd`
variant satisfies the 2:1 disk-to-RAM ratio:

| Machine Type          | RAM     | Bundled Local SSDs | Total SSD Storage |
|-----------------------|---------|--------------------|-------------------|
| `c4a-highmem-8-lssd`  | `64GB`  | 2                  | `750GB`           |
| `c4a-highmem-16-lssd` | `128GB` | 4                  | `1500GB`          |
| `c4a-highmem-32-lssd` | `256GB` | 6                  | `2250GB`          |
| `c4a-highmem-64-lssd` | `512GB` | 14                 | `5250GB`          |
| `c4-highmem-8-lssd`   | `62GB`  | 1                  | `375GB`           |
| `c4-highmem-16-lssd`  | `124GB` | 2                  | `750GB`           |
| `c4-highmem-32-lssd`  | `248GB` | 5                  | `1875GB`          |
| `c4-highmem-48-lssd`  | `372GB` | 8                  | `3000GB`          |

For other machine series, the local SSD count is configurable but may only
support predefined values. To determine the valid number of local SSDs to
attach for your machine type, see the [GCP
documentation](https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options).

[C4A high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#c4a_series

[C4 high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#c4_series


## Locally-attached NVMe storage

Configuring swap on nodes to use locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

{{< tabs level=4 >}}
{{< tab "New Terraform" >}}


The Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/gcp/examples/simple) supports configuring swap out of the box.

{{< /tab >}}
{{< tab "Legacy Terraform" >}}

The Legacy Terraform provider, adds preliminary swap support in v0.6.1, via the [`swap_enabled`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_swap_enabled) variable.
With this change, the Terraform:
  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).

{{< note >}}
If deploying `v25.2`, Materialize clusters will not automatically use swap unless they are configured with a `memory_request` less than their `memory_limit`. In `v26`, this will be handled automatically.
{{< /note >}}
{{< /tab >}}
{{< /tabs >}}

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Upgrading guideline

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md"
>}}

## Node pool resizing

{{% include-headless "/headless/self-managed-deployments/resize-node-pool" %}}
