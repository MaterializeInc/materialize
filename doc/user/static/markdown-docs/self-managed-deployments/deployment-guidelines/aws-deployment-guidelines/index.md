# AWS deployment guidelines

General guidelines when deploying Self-Managed Materialize on AWS.



Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.


## Recommended instance types

As a general guideline, we recommend:

- ARM-based CPU
- A 1:8 ratio of vCPU to GiB memory.
- A 8:1 ratio of GiB local instance storage to GiB memory when using swap.

When operating in AWS, we recommend the following instances:

| EC2 Instances  |
| ---------------|
| `r8g`, `r7g`, and `r6g` families when running without local disk. |
| `r7gd` and `r6gd` families (and `r8gd` once available) when running with local disk.  *Recommended for production.* |

Starting in v0.3.1, the Materialize on AWS Terraform uses `["r7gd.2xlarge"]` as
the default [`node_group_instance_types`].

[`node_group_instance_types`]:
    https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_node_group_instance_types


## Locally-attached NVMe storage

Configuring swap on nodes to use locally-attached NVMe storage allows
Materialize to spill to disk when operating on datasets larger than main memory.
This setup can provide significant cost savings and provides a more graceful
degradation rather than OOMing. Network-attached storage (like EBS volumes) can
significantly degrade performance and is not supported.

### Swap support


**New Terraform:**

#### New Terraform

The new Materialize [Terraform module](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws/examples/simple) supports configuring swap out of the box.


**Legacy Terraform:**
#### Legacy Terraform

The Legacy Terraform provider adds preliminary swap support in v0.6.1, via the [`swap_enabled`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_swap_enabled) variable.
With this change, the Terraform:
  - Creates a node group for Materialize.
  - Configures NVMe instance store volumes as swap using a daemonset.
  - Enables swap at the Kubelet.

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061).

> **Note:** If deploying `v25.2`, Materialize clusters will not automatically use swap unless they are configured with a `memory_request` less than their `memory_limit`. In `v26`, this will be handled automatically.
>





## TLS

When running with TLS in production, run with certificates from an official
Certificate Authority (CA) rather than self-signed certificates.

## Upgrading guideline

<p>Whe upgrading:</p>
<ul>
<li>
<p><strong>Always</strong> check the <a href="/self-managed-deployments/upgrading/#version-specific-upgrade-notes" >version specific upgrade
notes</a>.</p>
</li>
<li>
<p><strong>Always</strong> upgrade the operator <strong>first</strong> and ensure version compatibility
between the operator and the Materialize instance you are upgrading to.</p>
</li>
<li>
<p><strong>Always</strong> upgrade your Materialize instances <strong>after</strong> upgrading the operator
to ensure compatibility.</p>
</li>
</ul>
