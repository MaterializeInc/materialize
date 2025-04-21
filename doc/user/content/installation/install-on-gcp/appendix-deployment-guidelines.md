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


## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

*Additional documentation to come*

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
