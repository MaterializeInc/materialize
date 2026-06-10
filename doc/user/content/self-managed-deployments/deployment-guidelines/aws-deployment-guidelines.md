---
title: "AWS deployment guidelines"
description: "General guidelines when deploying Self-Managed Materialize on AWS."
disable_list: true
menu:
  main:
    parent: "deployment-guidelines"
    identifier: "aws-deployment-guidelines"
    weight: 10
aliases:
  - /installation/install-on-aws/appendix-deployment-guidelines/
---

{{% self-managed/materialize-components-sentence %}}

## Recommended instance types

As a general guideline, we recommend:

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory.
- A 8:1 ratio of GiB local instance storage to GiB memory when using swap.

{{% self-managed/aws-recommended-instances %}}

## Locally-attached NVMe storage

Configuring swap on nodes to use locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

{{< tabs >}}
{{< tab "New Terraform" >}}

#### New Terraform

The new Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/simple) supports configuring swap out of the box.

{{< /tab >}}
{{< tab "Legacy Terraform" >}}
#### Legacy Terraform

The Legacy Terraform provider adds preliminary swap support in v0.6.1, via the [`swap_enabled`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_swap_enabled) variable.
With this change, the Terraform:
  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061).

{{< note >}}
If deploying `v25.2`, Materialize clusters will not automatically use swap unless they are configured with a `memory_request` less than their `memory_limit`. In `v26`, this will be handled automatically.
{{< /note >}}

{{< /tab >}}
{{< /tabs >}}

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Upgrading guideline

{{< include-md file="shared-content/self-managed/general-rules-for-upgrades.md"
>}}
