---
audience: developer
canonical_url: https://materialize.com/docs/installation/install-on-aws/upgrade-on-aws/
complexity: intermediate
description: Procedure to upgrade your Materialize operator and instances running
  on AWS
doc_type: reference
keywords:
- UPDATE YOUR
- v27
- Upgrade on AWS (Terraform)
- v26
- 'Disambiguation:'
- not
- 'Important:'
product_area: Deployment
status: stable
title: Upgrade on AWS (Terraform)
---

# Upgrade on AWS (Terraform)

## Purpose
Procedure to upgrade your Materialize operator and instances running on AWS

If you need to understand the syntax and options for this command, you're in the right place.


Procedure to upgrade your Materialize operator and instances running on AWS


> **Disambiguation:** - To upgrade to `v26.0` using Materialize-provided Terraforms, upgrade your
Terraform version to `v0.6.1` or higher, [AWS Terraform v0.6.1 Upgrade
Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061)
.

- To upgrade to `v26.0` if <red>**not**</red> using a Materialize-provided
Terraforms, you must prepare your nodes by adding the required labels. For
detailed instructions, see [Prepare for swap and upgrade to
v26.0](/installation/upgrade-to-swap/).

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running on  AWS Elastic Kubernetes Service (EKS).

The tutorial assumes you have installed Materialize on AWS Elastic Kubernetes
Service (EKS) using the instructions on [Install on
AWS](/installation/install-on-aws/) (either from the examples/simple directory
or the root).

## Version compatibility

> **Important:** 

When performing major version upgrades, you can upgrade only one major version
at a time. For example, upgrades from **v26**.1.0 to **v27**.2.0 is permitted
but **v26**.1.0 to **v28**.0.0 is not. Skipping major versions or downgrading is
not supported. To upgrade from v25.2 to v26.0, you must [upgrade first to v25.2.16+](../self-managed/v25.2/release-notes/#v25216).


#### Materialize on AWS Terraform Releases


When upgrading, you may need or want to update your fork of the Terraform module
to upgrade.

<!-- Dynamic table: self_managed/aws_terraform_versions - see original docs -->


## Prerequisites

> **Important:** 

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.


### Terraform

If you don't have Terraform installed, [install
Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform).

### AWS CLI

If you do not have the AWS CLI installed, install. For details, see the [AWS
documentation](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).

### kubectl

If you do not have `kubectl`, install. See the [Amazon EKS: install `kubectl`
documentation](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
for details.

### Helm 3.2.0+

If you do not have Helm 3.2.0+, install. For details, see the [Helm
documentation](https://helm.sh/docs/intro/install/).

### License key

Starting in v26.0, Materialize requires a license key. If your existing
deployment does not have a license key configured, contact [Materialize support](../support/).


## Procedure

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `examples/simple` directory:

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```text

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   > **Tip:** 
   <!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See self-managed installation documentation --> --> -->

   See [Materialize on AWS releases](/installation/appendix-terraforms/#materialize-on-aws-terraform-module) for notable changes.

   


1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the name of your EKS cluster. Your cluster
       name has the form `{namespace}-{environment}-eks`; e.g.,
       `my-demo-dev-eks`.

   - `<your-region>` with the region of your EKS cluster. The
     simple example uses `us-east-1`.

   ```bash
   aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   ```text

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

<!-- Unresolved shortcode: {{% self-managed/versions/upgrade/upgrade-steps-cl... -->