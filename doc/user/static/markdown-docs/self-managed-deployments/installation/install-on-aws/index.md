# Install on AWS
Install Materialize on AWS using the new Terraform module.
Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run on AWS.
The module is intended to provide a simple set of examples on how to deploy
Materialize. It can be used as is or modules can be taken from the example and
integrated with existing DevOps tooling.

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster; PostgreSQL as
a metadata database; blob storage; and a license key.
 The example on this page
deploys a complete Materialize environment on AWS using the modular Terraform
setup from this repository.


> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.



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
| Network Load Balancer | Dedicated NLB for access to Materialize
| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |
 |


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


| License key type | Deployment type | Action |
| --- | --- | --- |
| Community | New deployments | <p>To get a license key:</p> <ul> <li>If you have a Cloud account, visit the <a href="https://console.materialize.com/license/" ><strong>License</strong> page in the Materialize Console</a>.</li> <li>If you do not have a Cloud account, visit <a href="https://materialize.com/self-managed/community-license/" >https://materialize.com/self-managed/community-license/</a>.</li> </ul> |
| Community | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |
| Enterprise | New deployments | Visit <a href="https://materialize.com/self-managed/enterprise-license/" >https://materialize.com/self-managed/enterprise-license/</a> to purchase an Enterprise license. |
| Enterprise | Existing deployments | Contact <a href="https://materialize.com/docs/support/" >Materialize support</a>. |


## Getting started: Simple example

> **Warning:** The Terraform modules used in this tutorial are intended for
> evaluation/demonstration purposes and for serving as a template when building
> your own production deployment. The modules should not be directly relied upon
> for production deployments: **future releases of the modules will contain
> breaking changes.** Instead, to use as a starting point for your own production
> deployment, either:
> - Fork the repo and pin to a specific version; or
> - Use the code as a reference when developing your own deployment.


### Step 1: Set Up the Environment

1. Open a terminal window.

1. Clone the Materialize Terraform repository and go to the
   `aws/examples/simple` directory.

   ```bash
   git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
   cd materialize-terraform-self-managed/aws/examples/simple
   ```

1. Ensure your AWS CLI is configured with the appropriate profile, substitute
   `<your-aws-profile>` with the profile to use:

   ```bash
   # Set your AWS profile for the session
   export AWS_PROFILE=<your-aws-profile>
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
   # internal_load_balancer = false   # default = true (internal load balancer). You can set to false = public load balancer.
   # ingress_cidr_blocks = ["x.x.x.x/n", ...]
   # k8s_apiserver_authorized_networks  = ["x.x.x.x/n", ...]
   ```

   <p><strong>Optional variables</strong>:</p>
   <ul>
   <li><code>internal_load_balancer</code>: Flag that determines whether the load balancer
   is internal (default) or public.</li>
   <li><code>ingress_cidr_blocks</code>: List of CIDR blocks allowed to reach the load
   balancer if the load balancer is public (<code>internal_load_balancer: false</code>).
   If unset, defaults to <code>[&quot;0.0.0.0/0&quot;]</code> (i.e., <red><strong>all</strong></red> IPv4
   addresses on the internet). <strong>Only applied when the load balancer is public</strong>.</li>
   <li><code>k8s_apiserver_authorized_networks</code>: List of CIDR
   blocks allowed to access your cluster endpoint. If unset, defaults to
   <code>[&quot;0.0.0.0/0&quot;]</code> (<red><strong>all</strong></red> IPv4 addresses on the internet).</li>
   </ul>
   > **Note:** Refer to your organization's security practices to set these values accordingly.

### Step 3: Apply the Terraform

1. Initialize the Terraform directory to download the required providers
    and modules:

    ```bash
    terraform init
    ```

1. Apply the Terraform configuration to create the infrastructure.

   ```bash
   terraform apply
   ```

   If you are satisfied with the planned changes, type `yes` when prompted to
   proceed.

   > **Tip:** If you previously logged in to Amazon ECR Public, a cached auth token may cause 403 errors even when pulling public images. To remove the token, run:
>    ```bash
>    docker logout public.ecr.aws
>    ```
>    Then, re-apply the Terraform configuration.


1. From the output, you will need the following fields to connect using the
   Materialize Console and PostgreSQL-compatible clients/drivers:
   - `nlb_dns_name`
   - `external_login_password_mz_system`.

   ```bash
   terraform output -raw <field_name>
   ```

   > **Tip:** Your shell may show an ending marker (such as `%`) because the
>    output did not end with a newline. Do not include the marker when using the value.



1. Configure `kubectl` to connect to your cluster, replacing:

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

### Step 4. Optional. Verify the deployment.

1. Check the status of your deployment:
   **Operator:**
   To check the status of the Materialize operator, which runs in the `materialize` namespace:
   ```bash
   kubectl -n materialize get all
   ```

   **Materialize instance:**
   To check the status of the Materialize instance, which runs in the `materialize-environment` namespace:
   ```bash
   kubectl -n materialize-environment get all
   ```


   <p>If you run into an error during deployment, refer to the
   <a href="/self-managed-deployments/troubleshooting/" >Troubleshooting</a>.</p>

### Step 5: Connect to Materialize

Using the `nlb_dns_name` and `external_login_password_mz_system` from the Terraform
output, you can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:


| Port | Description |
| --- | --- |
| 6875 | For SQL connections to the database |
| 6876 | For HTTP(S) connections to the database |
| 8080 | For HTTP(S) connections to Materialize Console |



#### Connect to the Materialize Console

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



1. To connect to the Materialize Console, open a browser to
    `https://<nlb_dns_name>:8080`, substituting your `<nlb_dns_name>`.

   From the terminal, you can type:

   ```sh
   open "https://$(terraform output -raw  nlb_dns_name):8080/materialize"
   ```

   > **Tip:** The example uses a self-signed ClusterIssuer. As such, you may encounter a
>    warning with regards to the certificate. In production, run with
>    certificates from an official Certificate Authority (CA) rather than
>    self-signed certificates.


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

> **Note:** - **If using a public NLB:** Both SQL and Console are available via the
> public NLB. You can connect directly using the NLB's DNS name from anywhere
> on the internet (subject to your `ingress_cidr_blocks` configuration).
> - **If using a private (internal) NLB:** You can connect from inside the same VPC or from networks that are privately connected to it. Alternatively, use Kubernetes port-forwarding for both SQL and Console.



1. To connect using `psql`, in the connection string, specify:

   - `mz_system` as the user
   - Your `<nlb_dns_name>` as the host
   - `6875` as the port:

   ```sh
   psql "postgres://mz_system@$(terraform output -raw  nlb_dns_name):6875/materialize"
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

> **Tip:** To reduce cost in your demo environment, you can tweak subnet CIDRs
> and instance types in `main.tf`.


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


To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the Terraform
directory:

```bash
terraform destroy
```

When prompted to proceed, type `yes` to confirm the deletion.



## See Also


- [Troubleshooting](/self-managed-deployments/troubleshooting/)
