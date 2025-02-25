---
title: "Operational guidelines"
description: ""
aliases:
  - /self-hosted/operational-guidelines/
menu:
  main:
    parent: "installation"
---

## Recommended instance types

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

When operating in AWS, we recommend:

- Using the `r7gd` and `r6gd` families of instances (and `r8gd` once available)
  when running with local disk

- Using the `r8g`, `r7g`, and `r6g` families when running without local disk

## CPU affinity

It is strongly recommended to enable the Kubernetes `static` [CPU management policy](https://kubernetes.io/docs/tasks/administer-cluster/cpu-management-policies/#static-policy).
This ensures that each worker thread of Materialize is given exclusively access to a vCPU. Our benchmarks have shown this
to substantially improve the performance of compute-bound workloads.

## Locally-attached NVMe storage

For optimal performance, Materialize requires fast, *locally-attached* NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. *Network-attached* storage (like EBS
volumes) can significantly degrade performance and is not supported.

*Additional documentation to come*


## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
