---
title: "Install on AWS"
description: "Install Materialize on AWS using the Unified Terraform module."
aliases:
  - /self-hosted/install-on-aws/
  - /self-managed/v25.1/installation/install-on-aws/
  - /installation/install-on-aws/
disable_list: true
menu:
  main:
    parent: "installation"
    identifier: "install-on-aws"
    weight: 20
---

Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on AWS.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

{{% self-managed/materialize-components-sentence %}} The example on this page
deploys a complete Materialize environment on AWS using the modular Terraform
setup from this repository.


{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}


## What Gets Created

This example provisions the following infrastructure:

### Networking

| Resource | Description |
|----------|-------------|
| VPC | 10.0.0.0/16 with DNS hostnames and support enabled |
| Subnets | 3 private subnets (10.0.1.0/24, 10.0.2.0/24, 10.0.3.0/24) and 3 public subnets (10.0.101.0/24, 10.0.102.0/24, 10.0.103.0/24) across availability zones us-east-1a, us-east-1b, us-east-1c |
| NAT Gateway | Single NAT Gateway for all private subnets |
| Internet Gateway | For public subnet connectivity |

### Compute

| Resource | Description |
|----------|-------------|
| EKS Cluster | Version 1.32 with CloudWatch logging (API, audit) |
| Base Node Group | 2 nodes (t4g.medium) for Karpenter and CoreDNS |
| Karpenter | Auto-scaling controller with two node classes: Generic nodepool (t4g.xlarge instances for general workloads) and Materialize nodepool (r7gd.2xlarge instances with swap enabled and dedicated taints to run materialize instance workloads) |

### Database

| Resource | Description |
|----------|-------------|
| RDS PostgreSQL | Version 15, db.t3.large instance |
| Storage | 50GB allocated, autoscaling up to 100GB |
| Deployment | Single-AZ (non-production configuration) |
| Backups | 7-day retention |
| Security | Dedicated security group with access from EKS cluster and nodes |

### Storage

| Resource | Description |
|----------|-------------|
| S3 Bucket | Dedicated bucket for Materialize persistence |
| Encryption | Disabled (for testing; enable in production) |
| Versioning | Disabled (for testing; enable in production) |
| IAM Role | IRSA role for Kubernetes service account access |

### Kubernetes Add-ons

| Resource | Description |
|----------|-------------|
| AWS Load Balancer Controller | For managing Network Load Balancers |
| cert-manager | Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal |
| Self-signed ClusterIssuer | Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication. |

### Materialize

| Resource | Description |
|----------|-------------|
| Operator | Materialize Kubernetes operator in the `materialize` namespace |
| Instance | Single Materialize instance in the `materialize-environment` namespace |
| Network Load Balancer | Dedicated internal NLB for Materialize access {{< yaml-table data="self_managed/default_ports" >}} |


## Prerequisites

### AWS Account Requirements

An active AWS account with appropriate permissions to create:
- EKS clusters
- RDS instances
- S3 buckets
- VPCs and networking resources
- IAM roles and policies

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)
- [kubectl](https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)


### License Key

{{< yaml-table data="self_managed/license_key" >}}

## Getting started: Simple example

{{< warning >}}

{{< self-managed/terraform-disclaimer >}}

{{< /warning >}}

### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `aws/examples/simple` directory.

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

### Step 2: Configure Terraform Variables

1. Create a `terraform.tfvars` file with the following variables:

   - `name_prefix`: Prefix for all resource names (e.g., `simple-demo`)
   - `aws_region`: AWS region for deployment (e.g., `us-east-1`)
   - `aws_profile`: AWS CLI profile to use
   - `license_key`: Materialize license key
   - `tags`: Map of tags to apply to resources

   ```hcl
   name_prefix = "simple-demo"
   aws_region  = "us-east-1"
   aws_profile = "your-aws-profile"
   license_key = "your-materialize-license-key"
   tags = {
     environment = "demo"
   }
   ```

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
    and modules:

    ```bash
    terraform init
    ```

1. Apply the Terraform configuration to create the infrastructure.

    - To deploy with the default **internal NLB** for Materialize access:

        ```bash
        terraform apply
        ```

    - To deploy with <red>**public NLB**</red> for Materialize access:

        ```bash
        terraform apply -var="internal=false"
        ```

    If you are satisfied with the planned changes, type `yes` when prompted
    to proceed.


1. From the output, you will need the following fields to connect using the
   Materialize Console and PostgreSQL-compatible clients/drivers:
   - `nlb_dns_name`
   - `external_login_password_mz_system`.

   ```bash
   terraform output -raw <field_name>
   ```

   {{< tip >}}
   Your shell may show an ending marker (such as `%`) because the
   output did not end with a newline. Do not include the marker when using the value.
   {{< /tip >}}


1. Configure `kubectl` to connect to your cluster using your:
   - `eks_cluster_name`. Your cluster name can be found in the Terraform output.
     For the sample example, your cluster name has the form `{prefix_name}-eks`;
     e.g., `simple-demo-eks`.

   - `region`. The region specified in your `terraform.tfvars` file; e.g.,
     `us-east-1`

   ```bash
   # aws eks update-kubeconfig --name <your-eks-cluster-name> --region <your-region>
   aws eks update-kubeconfig --name $(terraform output -raw eks_cluster_name) --region us-east-1
   ```

### Step 4. Optional. Verify the deployment.

1. Check the status of your deployment:
   {{% include-from-yaml data="self_managed/installation"
   name="installation-verify-status" %}}

### Step 5: Connect to Materialize

Using the `dns_name` and `external_login_password_mz_system` from the Terraform
output, you can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:

{{< yaml-table data="self_managed/default_ports" >}}

{{< note >}}

If using an **internal Network Load Balancer (NLB)** for your Materialize
access, you can connect from inside the same VPC or from networks that are
privately connected to it.

{{< /note >}}

#### Connect to the Materialize Console

1. To connect to the Materialize Console, open a browser to
    `https://<dns_name>:8080`, substituting your `<dns_name>`.

   {{< tip >}}

   {{% include-from-yaml data="self_managed/installation"
   name="install-uses-self-signed-cluster-issuer" %}}

   {{< /tip >}}

1. Log in as `mz_system`, using `external_login_password_mz_system` as the
   password.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

#### Connect using `psql`

1. To connect using `psql`, in the connection string, specify:

   - `mz_system` as the user
   - Your `<dns_name>` as the host
   - `6875` as the port:

   ```sh
   psql postgres://mz_system@<dns_name>:6875/materialize
   ```

   When prompted for the password, enter the
   `external_login_password_mz_system` value.

1. Create new users and log out.

   In general, other than the initial login to create new users for new
   deployments, avoid using `mz_system` since `mz_system` also used by the
   Materialize Operator for upgrades and maintenance tasks.

   For more information on authentication and authorization for Self-Managed
   Materialize, see:

   - [Authentication](/security/self-managed/authentication/)
   - [Access Control](/security/self-managed/access-control/)

1. Login as one of the created user.

## Customizing Your Deployment

{{< tip >}}
To reduce cost in your demo environment, you can tweak subnet CIDRs
and instance types in `main.tf`.
{{< /tip >}}

You can customize each Terraform module independently.

- For details on the Terraform modules, see both the [top
level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
and [AWS
specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/aws) READMEs.

- For details on recommended instance sizing and configuration, see the [AWS
deployment
guide](/self-managed-deployments/deployment-guidelines/aws-deployment-guidelines/).

See also:

- [Materialize Operator
  Configuration](/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/self-managed-deployments/materialize-crd-field-descriptions/)


## Cleanup

{{% self-managed/cleanup-cloud %}}


## See Also


- [Troubleshooting](/self-managed-deployments/troubleshooting/)
