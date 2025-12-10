---
title: "Upgrade on AWS"
description: "Upgrade Materialize on AWS using the Unified Terraform module."
menu:
  main:
    parent: "upgrading"
    weight: 20
---

The following tutorial upgrades your Materialize deployment running on AWS
Elastic Kubernetes Service (EKS). The tutorial assumes you have installed the
example on [Install on
AWS](/self-managed-deployments/installation/install-on-aws/).

## Upgrade guidelines

{{% include-from-yaml data="self_managed/upgrades"
name="upgrades-general-rules" %}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="upgrade-major-version-restriction" >}}
{{< /note >}}

{{< note >}}
{{< include-from-yaml data="self_managed/upgrades"
name="downgrade-restriction" >}}
{{< /note >}}

## Prerequisites

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

## Upgrade process

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new Materialize instances are running before the old instances are removed. When performing a rolling upgrade, ensure you have enough resources to support having both the old and new Materialize instances running.

{{</ important >}}

### Step 1: Set up

1. Open a Terminal window.

1. Configure AWS CLI with your AWS credentials. For details, see the [AWS
   documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).

1. Go to the Terraform directory for your Materialize deployment. For example,
   if you deployed from the `aws/examples/simple` directory:

   ```bash
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

1. Ensure your AWS CLI is configured with the appropriate profile, substitute
   `<your-aws-profile>` with the profile to use:

   ```bash
   # Set your AWS profile for the session
   export AWS_PROFILE=<your-aws-profile>
   ```

1. Configure `kubectl` to connect to your EKS cluster, replacing:

   - `<your-eks-cluster-name>` with the your cluster name; i.e., the
     `eks_cluster_name` in the Terraform output. For the
     sample example, your cluster name has the form `{prefix_name}-eks`; e.g.,
     `simple-demo-eks`.

   - `<your-region>` with the region of your cluster. Your region can be
     found in your `terraform.tfvars` file; e.g., `us-east-1`.

   ```bash
   # aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   aws eks update-kubeconfig --name $(terraform output -raw eks_cluster_name) --region <your-region>
   ```

   To verify that you have configured correctly, run the following command:

   ```bash
   kubectl get nodes
   ```

   For help with `kubectl` commands, see [kubectl Quick reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### Step 2: Update the Helm Chart

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-update-helm-chart" %}}

### Step 3: Upgrade the Materialize Operator

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-materialize-operator" %}}

### Step 4: Upgrading Materialize Instances

{{< important >}}

{{% include-from-yaml data="self_managed/upgrades" name="upgrade-order-rule" %}}

{{</ important >}}

{{% include-from-yaml data="self_managed/upgrades"
name="upgrade-materialize-instance" %}}

## See also

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)
- [Troubleshooting](/self-managed-deployments/troubleshooting/)
