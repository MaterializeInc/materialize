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

## Custom Cluster Sizes

When installing the materialize helm chart you may specify a list of cluster sizes.
```yaml
operator:
  clusters:
    sizes:
    ...
```
These cluster sizes will be used for internal clusters, such as the `system_cluster` as well as 
user clusters. For that reason we recommend that you at minimum keep the 25-200cc cluster sizes.
If you wish to have have cluster sizes
| Field   | type | Description |
| workers | int  | The number of timely workers in your cluster replica. |
| scale   | int  | The number of processes or pods to use in a cluster replica. |
| cpu_exclusive  | bool | Whether the workers should attempt to pin to a particular cpu core. |
| cpu_limit      | float | The k8s limit for CPU for a replica pod in cores. |
| memory_limit   | float | The k8s limit for memory for a replica pod in bytes. |
| disk_limit     | float | The size of the nvme persistent volume to provision for a replica pod in bytes. |
| credits_per_hour | string | This is a cloud attribute that should be set to "0.00" in self-managed. |

### Recommendations for cluster sizes:
**workers**
* We recommend using 1 worker per CPU core while maintaining a minimum of 1 worker.

**scale**
* Scale is used to scale out replicas horrizontally. Each pod will be
  provisioned using the settings defined in the size definition. This should
  only be greater than one when a replica needs to take on limits that are
  greater than the maximum limits permitted on a single node.

**cpu_exclusive**
* set this to true if and only if a whole number is used for cpu_limit and the cpu management policy is set to static on the k8s cluster.

**cpu_limit**
* K8s will only allow CPU Affinity for pods taking a whole number of cores (not hyperthreads). When possible use a whole number.

Ratios:
**CPU : Memory**
* For most workloads, we find find a 1:8 ratio of cores to Gib to work well, but this can be quite workload dependent.

**Memory : Disk**
* Materialize attempts to keep actively used data in memory. In order to allow
  for larger workloads data can spill to disk at some cost for performance.
  Our current recommendation is a 1:2 ratio of memory to disk.


## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
