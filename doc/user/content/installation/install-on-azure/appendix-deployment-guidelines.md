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

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory (if spill-to-disk is not enabled)
- 1:16 ratio of vCPU to GiB local instance storage (if spill-to-disk is enabled)


## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

*Additional documentation to come*

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
