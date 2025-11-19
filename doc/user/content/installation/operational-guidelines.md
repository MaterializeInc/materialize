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

As a general guideline, we recommend:

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory is recommend.
- When using swap, it is recommend to use a 1:16 ratio of vCPU to GiB local instance storage

See also the specific cloud provider guidance:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Locally-attached NVMe storage

Configuring swap on nodes to using locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

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
