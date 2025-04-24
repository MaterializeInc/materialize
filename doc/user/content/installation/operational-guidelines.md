---
title: "Operational guidelines"
description: ""
aliases:
  - /self-hosted/operational-guidelines/
menu:
  main:
    parent: "installation"
    weight: 80
    identifier: "sm-operational-guidelines"
---

## Recommended instance types

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory (if spill-to-disk is not enabled)
- 1:16 ratio of vCPU to GiB local instance storage (if spill-to-disk is enabled)

See also the specific cloud provider guidance:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)

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

Refer to the specific cloud provider guidelines:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/)

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
