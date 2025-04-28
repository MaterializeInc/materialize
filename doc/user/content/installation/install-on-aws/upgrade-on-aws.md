---
title: "Upgrade on AWS"
description: "Procedure to upgrade your Materialize operator and instances running on AWS"
menu:
  main:
    parent: "install-on-aws"
    identifier: "upgrade-on-aws"
    weight: 10
---

To upgrade your Materialize instances, upgrade the Materialize operator first
and then the Materialize instances. The following tutorial upgrades your
Materialize deployment running on  AWS Elastic Kubernetes Service (EKS).

The tutorial assumes you have installed Materialize on AWS Elastic Kubernetes
Service (EKS) using the instructions on [Install on
AWS](/installation/install-on-aws/) (either from the examples/simple directory
or the root).

## Version compatibility


{{< tabs >}}

{{< tab "Helm chart releases" >}}

The following table presents the versions compatibility for the operator and the
applications:

{{< yaml-table data="self_managed/self_managed_operator_compatibility" >}}

{{</ tab >}}

{{< tab "Materialize on AWS Terraform Releases" >}}

When upgrading, you may need or want to update your fork of the Terraform module
to upgrade.

{{< yaml-table data="self_managed/aws_terraform_versions" >}}

{{</ tab >}}
{{</ tabs >}}

## Prerequisites

{{< important >}}

The following procedure performs an in-place upgrade, which incurs downtime.

To perform a rolling upgrade(where both the old and new Materialize instances
are running before the the old instances are removed), you can specify
`inPlaceRollout` to false. When performing a rolling upgrade, ensure you have
enough resources to support having both the old and new Materialize instances
running.

{{</ important >}}

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

## Procedure

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `examples/simple` directory:

   ```bash
   cd terraform-aws-materialize/examples/simple
   ```

1. Optional. You may need to update your fork of the Terraform module to
   upgrade.

   {{< tip >}}
   {{% self-managed/aws-terraform-upgrade-notes %}}

   See [Materialize on AWS releases](/installation/appendix-terraforms/#materialize-on-aws-terraform-module) for notable changes.

   {{</ tip >}}


1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the name of your EKS cluster. Your cluster
       name has the form `{namespace}-{environment}-eks`; e.g.,
       `my-demo-dev-eks`.

   - `<your-region>` with the region of your EKS cluster. The
     simple example uses `us-east-1`.

   ```bash
   aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick
   reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

{{% self-managed/versions/upgrade/upgrade-steps-cloud %}}
