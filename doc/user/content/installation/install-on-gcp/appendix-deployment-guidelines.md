---
title: "Appendix: GCP deployment guidelines"
description: "GCP environment setup/deployment guidelines"
menu:
  main:
    parent: "install-on-gcp"
    identifier: "gcp-deployment-guidelines"
    weight: 40
---

## Recommended instance types

As a general guideline, we recommend:

- Processor Type: ARM-based CPU

- Sizing: 2:1 disk-to-RAM ratio with spill-to-disk enabled.

When operating on GCP in production, we recommend the following machine types
that support local SSD attachment:

| Series | Examples   |
| ------ | ---------- |
| [N2 high-memory series] | `n2-highmem-16` or `n2-highmem-32` with local NVMe SSDs |
| [N2D  high-memory series] | `n2d-highmem-16` or `n2d-highmem-32` with local NVMe SSDs |

To maintain the recommended 2:1 disk-to-RAM ratio for your machine type, see
[Number of local SSDs](#number-of-local-ssds) to determine the number of local
SSDs
([`disk_support_config.local_ssd_count`](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/README.md#input_disk_support_config))
to use.

See also [Locally attached NVMe storage](#locally-attached-nvme-storage).

## Number of local SSDs

Each local NVMe SSD in GCP provides 375GB of storage. Use the appropriate number
of local SSDs
([`disk_support_config.local_ssd_count`](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/README.md#input_disk_support_config))
to ensure your total disk space is at least twice the amount of RAM in your
machine type for optimal Materialize performance.

{{< note >}}

Your machine type may only supports predefined number of local SSDs. For instance, `n2d-highmem-32` allows only the following number of local
SSDs: `4`,`8`,`16`, or `24`. To determine the valid number of Local SSDs to attach for your machine type, see the [GCP
documentation](https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options).

{{</ note >}}

For example, the following table provides a minimum local SSD count to ensure
the 2:1 disk-to-RAM ratio. Your actual
count will depend on the [your machine
type](https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options).

| Machine Type    | RAM     | Required Disk | Minimum Local SSD Count | Total SSD Storage |
|-----------------|---------|---------------|-----------------------------|-------------------|
| `n2-highmem-8`  | `64GB`  | `128GB`       | 1                           | `375GB`           |
| `n2-highmem-16` | `128GB` | `256GB`       | 1                           | `375GB`           |
| `n2-highmem-32` | `256GB` | `512GB`       | 2                           | `750GB`           |
| `n2-highmem-64` | `512GB` | `1024GB`      | 3                           | `1125GB`          |
| `n2-highmem-80` | `640GB` | `1280GB`      | 4                           | `1500GB`          |

[N2 high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#n2-high-mem

[N2D high-memory series]: https://cloud.google.com/compute/docs/general-purpose-machines#n2d_machine_types

[enables spill-to-disk]: https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#disk-support-for-materialize-on-gcp

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

Starting in v0.4.0 of Materialize on Google Cloud Provider (GCP) Terraform,
disk support (using OpenEBS and NVMe instance storage) is enabled, by default,
for Materialize. With this change, the Terraform:

- Installs OpenEBS via Helm;

- Configures NVMe instance store volumes using a bootstrap script;

- Creates appropriate storage classes for Materialize.

Associated with this change:

- The following configuration options are available:

  - [`enable_disk_support`]
  - [`disk_support_config`]

- The default [`gke_config.machine_type`] has changed from `e2-standard-4` to
`n2-highmem-8`. See [Recommended instance types](#recommended-instance-types).

[enable disk support]:
    https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#disk-support-for-materialize-on-gcp

[`enable_disk_support`]:
    https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_enable_disk_support

[`disk_support_config`]:
    https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_disk_support_config

[`gke_config.machine_type`]:
    https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_gke_config

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Storage bucket versioning

Starting in v0.3.1 of Materialize on GCP Terraform, storage bucket versioning is
disabled (i.e.,
[`storage_bucket_versioning`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_versioning)
is set to `false` by default) to facilitate cleanup of resources during testing.
When running in production, versioning should be turned on with a sufficient TTL
([`storage_bucket_version_ttl`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_version_ttl))
to meet any data-recovery requirements.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
