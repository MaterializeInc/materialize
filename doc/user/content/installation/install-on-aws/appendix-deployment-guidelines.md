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

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory is recommended.
- When using swap, it is recommended to use a 8:1 ratio of GiB local instance storage to GiB Ram.

{{% self-managed/aws-recommended-instances %}}

## Locally-attached NVMe storage

Configuring swap on nodes to using locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support

***New Unified Terraform***

The unified Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/simple) supports configuring swap out of the box.

***Legacy Terraform***

The Legacy Terraform provider, adds preliminary swap support in v0.6.1, via the [`swap_enabled`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_swap_enabled) variable.
With this change, the Terraform:
  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061).

{{< note >}}
If deploying `v25.2` Materialize clusters will not automatically use swap unless they are configured with a `memory_request` less than their `memory_limit`. In `v26` this will be handled automatically.
{{< /note >}}

## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## See also

- [Configuration](/installation/configuration/)
- [Installation](/installation/)
- [Troubleshooting](/installation/troubleshooting/)
