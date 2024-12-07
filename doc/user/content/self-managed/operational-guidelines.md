---
title: "Operational guidelines"
description: ""
robots: "noindex, nofollow"
---

## Recommended instance types

Materialize has been tested to work on instances with the following properties:

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

## Network policies

Enabling network policies ...

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Configuration](/self-managed/configuration/)
- [Installation](/self-managed/installation/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Upgrading](/self-managed/upgrading/)
