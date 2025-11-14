---
title: "Appendix: AWS deployment guidelines"
description: "AWS environment setup/deployment guidelines"
aliases:
  - /self-hosted/operational-guidelines/
menu:
  main:
    parent: "install-on-aws"
    identifier: "aws-deployment-guidelines"
    weight: 40
---

## Recommended instance types

As a general guideline, we recommend:

- Processor Type: ARM-based CPU

- Sizing:

  - If spill-to-disk is not enabled: 1:8 ratio of vCPU to GiB memory

  - If spill-to-disk is enabled (*Recommended*): 1:16 ratio of vCPU to GiB local
    instance storage

{{% self-managed/aws-recommended-instances %}}

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, locally-attached NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. Network-attached storage (like EBS
volumes) can significantly degrade performance and is not supported.

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
