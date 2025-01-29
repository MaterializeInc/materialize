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


## Locally-attached NVMe storage (OpenEBS)

For optimal performance, Materialize requires fast, *locally-attached* NVMe
storage. Having a locally-attached storage allows Materialize to spill to disk
when operating on datasets larger than main memory as well as allows for a more
graceful degradation rather than OOMing. *Network-attached* storage (like EBS
volumes) can significantly degrade performance and is not supported.

For locally-attached NVMe storage, we recommend using OpenEBS with LVM Local PV
for managing local volumes. While other storage solutions may work, we have
tested and recommend OpenEBS for optimal performance.

For locally-attached NVMe storage,

1. Install OpenEBS to your running Kubernetes cluster.

   ```bash
   # Add OpenEBS to Helm
   helm repo add openebs https://openebs.github.io/openebs
   helm repo update

   # Install only the Local PV Storage Engines
   helm install openebs --namespace openebs openebs/openebs \
     --set engines.replicated.mayastor.enabled=false \
     --create-namespace
   ```

   Verify the installation:
   ```bash
   kubectl get pods -n openebs -l role=openebs-lvm
   ```
1. Configure the logical volume manager (LVM)

   Logical Volume Manager (LVM) setup varies by environment. Below is our tested
   and recommended configuration for AWS EC2 with Bottlerocket AMI.


   Tested configurations:

   |                                              |      |
   |----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
   | **Instance types**                           | **r6g**, **r7g** families  <br> **Note:** LVM setup may work on other instance types with local storage  (like i3.xlarge, i4i.xlarge, r5d.xlarge), but we have not extensively tested  these configurations. |
   | **AMI**                                      | AWS Bottlerocket |
   | **Instance store volumes**                   | Required |

   To setup:

   1. Use Bottlerocket bootstrap container for LVM configuration.
   1. Configure volume group name as `instance-store-vg`

   {{< tip >}}

   If you are using the recommended Bottlerocket AMI with the Terraform module,
   the LVM configuration is automatically handled by the EKS module using the
   provided user data script.

   {{< /tip >}}

   To verify the LVM setup, run the following:

   ```bash
   kubectl debug -it node/<node-name> --image=amazonlinux:2
   chroot /host
   lvs
   ```

   You should see a volume group named `instance-store-vg`.

1. Include the storage configuration in the configuration file for the
   Materialize operator:

   {{< tabs >}}
   {{< tab "OpenEBS" >}}

   If using OpenEBS, set up the storage class as follows:
   ```yaml
   storage:
     storageClass:
       create: true
       name: "openebs-lvm-instance-store-ext4"
       provisioner: "local.csi.openebs.io"
       parameters:
         storage: "lvm"
         fsType: "ext4"
         volgroup: "instance-store-vg"
   ```
   {{< /tab >}}
   {{< tab "Other Storage" >}}
   While OpenEBS is our recommended solution, you can use any storage   provisioner that meets your performance requirements by overriding the   provisioner and parameters values.

   For example, to use a different storage provider:

   ```yaml
   storage:
     storageClass:
       create: true
       name: "your-storage-class"
       provisioner: "your.storage.provisioner"
       parameters:
         # Parameters specific to your chosen storage provisioner
   ```
   {{< /tab >}}
   {{< /tabs >}}

## Network policies

Enabling network policies ...

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
