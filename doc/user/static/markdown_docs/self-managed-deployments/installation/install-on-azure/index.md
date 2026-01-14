<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [Self-Managed
Deployments](/docs/self-managed-deployments/)
 /  [Installation](/docs/self-managed-deployments/installation/)

</div>

# Install on Azure

Materialize provides a set of modular [Terraform
modules](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
that can be used to deploy all services required for Materialize to run
on Azure. The module is intended to provide a simple set of examples on
how to deploy Materialize. It can be used as is or modules can be taken
from the example and integrated with existing DevOps tooling.

Self-managed Materialize requires: a Kubernetes (v1.31+) cluster;
PostgreSQL as a metadata database; blob storage; and a license key. The
example on this page deploys a complete Materialize environment on Azure
using the modular Terraform setup from this repository.

<div class="warning">

**WARNING!**

The Terraform modules used in this tutorial are intended for
evaluation/demonstration purposes and for serving as a template when
building your own production deployment. The modules should not be
directly relied upon for production deployments: **future releases of
the modules will contain breaking changes.** Instead, to use as a
starting point for your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

</div>

## What Gets Created

This example provisions the following infrastructure:

### Resource Group

| Resource       | Description                                 |
|----------------|---------------------------------------------|
| Resource Group | New resource group to contain all resources |

### Networking

| Resource | Description |
|----|----|
| Virtual Network | 20.0.0.0/16 address space |
| AKS Subnet | 20.0.0.0/20 with NAT Gateway association and service endpoints for Storage and SQL |
| PostgreSQL Subnet | 20.0.16.0/24 delegated to PostgreSQL Flexible Server |
| NAT Gateway | Standard SKU with static public IP for outbound connectivity |
| Private DNS Zone | For PostgreSQL private endpoint resolution with VNet link |

### Compute

| Resource | Description |
|----|----|
| AKS Cluster | Version 1.32 with Cilium networking (network plugin: azure, data plane: cilium, policy: cilium) |
| Default Node Pool | Standard_D4pds_v6 VMs, autoscaling 2-5 nodes, labeled for generic workloads |
| Materialize Node Pool | Standard_E4pds_v6 VMs with 100GB disk, autoscaling 2-5 nodes, swap enabled, dedicated taints for Materialize workloads |
| Managed Identities | AKS cluster identity (used by AKS control plane to provision Azure resources like load balancers and network interfaces) and Workload identity (used by Materialize pods for secure, passwordless authentication to Azure Storage) |

### Database

| Resource | Description |
|----|----|
| Azure PostgreSQL Flexible Server | Version 15 |
| SKU | GP_Standard_D2s_v3 (2 vCores, 4GB memory) |
| Storage | 32GB with 7-day backup retention |
| Network Access | Public Network Access is disabled, Private access only (no public endpoint) |
| Database | `materialize` database pre-created |

### Storage

| Resource | Description |
|----|----|
| Storage Account | Premium BlockBlobStorage with LRS replication for Materialize persistence |
| Container | `materialize` blob container |
| Access Control | Workload Identity federation for Kubernetes service account (passwordless authentication via OIDC) |
| Network Access | Currently allows **all traffic**(production deployments should restrict to AKS subnet only traffic) |

### Kubernetes Add-ons

| Resource | Description |
|----|----|
| cert-manager | Certificate management controller for Kubernetes that automates TLS certificate provisioning and renewal |
| Self-signed ClusterIssuer | Provides self-signed TLS certificates for Materialize instance internal communication (balancerd, console). Used by the Materialize instance for secure inter-component communication. |

### Materialize

<table>
<colgroup>
<col style="width: 50%" />
<col style="width: 50%" />
</colgroup>
<thead>
<tr>
<th>Resource</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>Operator</td>
<td>Materialize Kubernetes operator in the <code>materialize</code>
namespace</td>
</tr>
<tr>
<td>Instance</td>
<td>Single Materialize instance in the
<code>materialize-environment</code> namespace</td>
</tr>
<tr>
<td>Load Balancers</td>
<td>Azure Load Balancers for access to Materialize
<table>
<thead>
<tr>
<th>Port</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>6875</td>
<td>For SQL connections to the database</td>
</tr>
<tr>
<td>6876</td>
<td>For HTTP(S) connections to the database</td>
</tr>
<tr>
<td>8080</td>
<td>For HTTP(S) connections to Materialize Console</td>
</tr>
</tbody>
</table></td>
</tr>
</tbody>
</table>

## Prerequisites

### Azure Account Requirements

An active Azure subscription with appropriate permissions to create:

- AKS clusters
- Azure PostgreSQL Flexible Server instances
- Storage accounts
- Virtual networks and networking resources
- Managed identities and role assignments

### Required Tools

- [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform)
- [Azure
  CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- [Helm 3.2.0+](https://helm.sh/docs/intro/install/)

### License Key

<table>
<colgroup>
<col style="width: 33%" />
<col style="width: 33%" />
<col style="width: 33%" />
</colgroup>
<thead>
<tr>
<th>License key type</th>
<th>Deployment type</th>
<th>Action</th>
</tr>
</thead>
<tbody>
<tr>
<td>Community</td>
<td>New deployments</td>
<td><p>To get a license key:</p>
<ul>
<li>If you have a Cloud account, visit the <a
href="https://console.materialize.com/license/"><strong>License</strong>
page in the Materialize Console</a>.</li>
<li>If you do not have a Cloud account, visit <a
href="https://materialize.com/self-managed/community-license/">https://materialize.com/self-managed/community-license/</a>.</li>
</ul></td>
</tr>
<tr>
<td>Community</td>
<td>Existing deployments</td>
<td>Contact <a href="https://materialize.com/docs/support/">Materialize
support</a>.</td>
</tr>
<tr>
<td>Enterprise</td>
<td>New deployments</td>
<td>Visit <a
href="https://materialize.com/self-managed/enterprise-license/">https://materialize.com/self-managed/enterprise-license/</a>
to purchase an Enterprise license.</td>
</tr>
<tr>
<td>Enterprise</td>
<td>Existing deployments</td>
<td>Contact <a href="https://materialize.com/docs/support/">Materialize
support</a>.</td>
</tr>
</tbody>
</table>

## Getting started: Simple example

<div class="warning">

**WARNING!**

The Terraform modules used in this tutorial are intended for
evaluation/demonstration purposes and for serving as a template when
building your own production deployment. The modules should not be
directly relied upon for production deployments: **future releases of
the modules will contain breaking changes.** Instead, to use as a
starting point for your own production deployment, either:

- Fork the repo and pin to a specific version; or

- Use the code as a reference when developing your own deployment.

</div>

### Step 1: Set Up the Environment

1.  Open a terminal window.

2.  Clone the Materialize Terraform repository and go to the
    `azure/examples/simple` directory.

    <div class="highlight">

    ``` chroma
    git clone https://github.com/MaterializeInc/materialize-terraform-self-managed.git
    cd materialize-terraform-self-managed/azure/examples/simple
    ```

    </div>

3.  Authenticate with Azure.

    <div class="highlight">

    ``` chroma
    az login
    ```

    </div>

    The command opens a browser window to sign in to Azure. Sign in.

4.  Select the subscription and tenant to use. After you have signed in,
    back in the terminal, your tenant and subscription information is
    displayed.

    ```
    Retrieving tenants and subscriptions for the selection...

    [Tenant and subscription selection]

    No     Subscription name    Subscription ID                       Tenant
    -----  -------------------  ------------------------------------  ----------------
    [1]*   ...                  ...                                   ...

    The default is marked with an *; the default tenant is '<Tenant>' and
    subscription is '<Subscription Name>' (<Subscription ID>).
    ```

    Select the subscription and tenant.

### Step 2: Configure Terraform Variables

1.  Create a `terraform.tfvars` file with the following variables:

    - `subscription_id`: Azure subscription ID
    - `resource_group_name`: Name for the resource group to create (e.g.
      `mz-demo-rg`)
    - `name_prefix`: Prefix for all resource names (e.g., `simple-demo`)
    - `location`: Azure region for deployment (e.g., `westus2`)
    - `license_key`: Materialize license key
    - `tags`: Map of tags to apply to resources

    <div class="highlight">

    ``` chroma
    subscription_id     = "your-subscription-id"
    resource_group_name = "mz-demo-rg"
    name_prefix         = "simple-demo"
    location            = "westus2"
    license_key         = "your-materialize-license-key"
    tags = {
      environment = "demo"
    }
    # internal_load_balancer = false   # default = true (internal load balancer). You can set to false = public load balancer.
    # ingress_cidr_blocks = ["x.x.x.x/n", ...]
    # k8s_apiserver_authorized_networks  = ["x.x.x.x/n", ...]
    ```

    </div>

    **Optional variables**:

    - `internal_load_balancer`: Flag that determines whether the load
      balancer is internal (default) or public.
    - `ingress_cidr_blocks`: List of CIDR blocks allowed to reach the
      load balancer if the load balancer is public
      (`internal_load_balancer: false`). If unset, defaults to
      `["0.0.0.0/0"]` (i.e., **all** IPv4 addresses on the internet).
      **Only applied when the load balancer is public**.
    - `k8s_apiserver_authorized_networks`: List of CIDR blocks allowed
      to access your cluster endpoint. If unset, defaults to
      `["0.0.0.0/0"]` (**all** IPv4 addresses on the internet).

    <div class="note">

    **NOTE:** Refer to your organization’s security practices to set
    these values accordingly.

    </div>

### Step 3: Apply the Terraform

1.  Initialize the Terraform directory to download the required
    providers and modules:

    <div class="highlight">

    ``` chroma
    terraform init
    ```

    </div>

2.  Apply the Terraform configuration to create the infrastructure.

    <div class="highlight">

    ``` chroma
    terraform apply
    ```

    </div>

    If you are satisfied with the planned changes, type `yes` when
    prompted to proceed.

3.  From the output, you will need the following field(s) to connect:

    - `console_load_balancer_ip` for the Materialize Console
    - `balancerd_load_balancer_ip` to connect PostgreSQL-compatible
      clients/drivers.
    - `external_login_password_mz_system`.

    <div class="highlight">

    ``` chroma
    terraform output -raw <field_name>
    ```

    </div>

    <div class="tip">

    **💡 Tip:** Your shell may show an ending marker (such as `%`)
    because the output did not end with a newline. Do not include the
    marker when using the value.

    </div>

4.  Configure `kubectl` to connect to your cluster, replacing:

    - `<your-resource-group-name>` with your resource group name; i.e.,
      the `resource_group_name` in the Terraform output or in the
      `terraform.tfvars` file.

    - `<your-aks-cluster-name>` with your cluster name; i.e., the
      `aks_cluster_name` in the Terraform output. For the sample
      example, your cluster name has the form `{prefix_name}-aks`; e.g.,
      `simple-demo-aks`.

    <div class="highlight">

    ``` chroma
    # az aks get-credentials --resource-group <your-resource-group-name> --name <your-aks-cluster-name>
    az aks get-credentials --resource-group $(terraform output -raw resource_group_name) --name $(terraform output -raw aks_cluster_name)
    ```

    </div>

### Step 4. Optional. Verify the deployment.

1.  Check the status of your deployment:

    <div class="code-tabs">

    <div class="tab-content">

    <div id="tab-operator" class="tab-pane" title="Operator">

    To check the status of the Materialize operator, which runs in the
    `materialize` namespace:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize get all
    ```

    </div>

    </div>

    <div id="tab-materialize-instance" class="tab-pane"
    title="Materialize instance">

    To check the status of the Materialize instance, which runs in the
    `materialize-environment` namespace:

    <div class="highlight">

    ``` chroma
    kubectl -n materialize-environment get all
    ```

    </div>

    </div>

    </div>

    </div>

    If you run into an error during deployment, refer to the
    [Troubleshooting](/docs/self-managed-deployments/troubleshooting/).

### Step 5: Connect to Materialize

You can connect to Materialize via the Materialize Console or
PostgreSQL-compatible tools/drivers using the following ports:

| Port | Description                                    |
|------|------------------------------------------------|
| 6875 | For SQL connections to the database            |
| 6876 | For HTTP(S) connections to the database        |
| 8080 | For HTTP(S) connections to Materialize Console |

#### Connect using the Materialize Console

<div class="note">

**NOTE:**

- **If using a public NLB:** Both SQL and Console are available via the
  public NLB. You can connect directly using the NLB’s DNS name from
  anywhere on the internet (subject to your `ingress_cidr_blocks`
  configuration).

- **If using a private (internal) NLB:** You can connect from inside the
  same VPC or from networks that are privately connected to it.
  Alternatively, use Kubernetes port-forwarding for both SQL and
  Console.

</div>

Using the `console_load_balancer_ip` and
`external_login_password_mz_system` from the Terraform output, you can
connect to Materialize via the Materialize Console.

1.  To connect to the Materialize Console, open a browser to
    `https://<console_load_balancer_ip>:8080`, substituting your
    `<console_load_balancer_ip>`.

    From the terminal, you can type:

    <div class="highlight">

    ``` chroma
    open "https://$(terraform output -raw  console_load_balancer_ip):8080/materialize"
    ```

    </div>

    <div class="tip">

    **💡 Tip:** The example uses a self-signed ClusterIssuer. As such,
    you may encounter a warning with regards to the certificate. In
    production, run with certificates from an official Certificate
    Authority (CA) rather than self-signed certificates.

    </div>

2.  Log in as `mz_system`, using `external_login_password_mz_system` as
    the password.

3.  Create new users and log out.

    In general, other than the initial login to create new users for new
    deployments, avoid using `mz_system` since `mz_system` also used by
    the Materialize Operator for upgrades and maintenance tasks.

    For more information on authentication and authorization for
    Self-Managed Materialize, see:

    - [Authentication](/docs/security/self-managed/authentication/)
    - [Access Control](/docs/security/self-managed/access-control/)

4.  Login as one of the created user.

#### Connect using `psql`

<div class="note">

**NOTE:**

- **If using a public NLB:** Both SQL and Console are available via the
  public NLB. You can connect directly using the NLB’s DNS name from
  anywhere on the internet (subject to your `ingress_cidr_blocks`
  configuration).

- **If using a private (internal) NLB:** You can connect from inside the
  same VPC or from networks that are privately connected to it.
  Alternatively, use Kubernetes port-forwarding for both SQL and
  Console.

</div>

Using the `balancerd_load_balancer_ip` and
`external_login_password_mz_system` from the Terraform output, you can
connect to Materialize via PostgreSQL-compatible clients/drivers, such
as `psql`.

1.  To connect using `psql`, in the connection string, specify:

    - `mz_system` as the user
    - `balancerd_load_balancer_ip` as the host
    - `6875` as the port:

    <div class="highlight">

    ``` chroma
    psql "postgres://mz_system@$(terraform output -raw balancerd_load_balancer_ip):6875/materialize"
    ```

    </div>

    When prompted for the password, enter the
    `external_login_password_mz_system` value.

2.  Create new users and log out.

    In general, other than the initial login to create new users for new
    deployments, avoid using `mz_system` since `mz_system` also used by
    the Materialize Operator for upgrades and maintenance tasks.

    For more information on authentication and authorization for
    Self-Managed Materialize, see:

    - [Authentication](/docs/security/self-managed/authentication/)
    - [Access Control](/docs/security/self-managed/access-control/)

3.  Login as one of the created user.

## Customizing Your Deployment

<div class="tip">

**💡 Tip:** To reduce cost in your demo environment, you can tweak VM
sizes and database tiers in `main.tf`.

</div>

You can customize each Terraform module independently.

- For details on the Terraform modules, see both the [top
  level](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main)
  and [Azure
  specific](https://github.com/MaterializeInc/materialize-terraform-self-managed/tree/main/azure)
  modules.

- For details on recommended instance sizing and configuration, see the
  [Azure deployment
  guide](/docs/self-managed-deployments/deployment-guidelines/azure-deployment-guidelines/).

<div class="note">

**NOTE:** Autoscaling: Uses Azure’s native cluster autoscaler that
integrates directly with Azure Virtual Machine Scale Sets for automated
node scaling.

</div>

See also:

- [Materialize Operator
  Configuration](/docs/self-managed-deployments/operator-configuration/)
- [Materialize CRD Field
  Descriptions](/docs/self-managed-deployments/materialize-crd-field-descriptions/)

## Cleanup

To delete the whole sample infrastructure and deployment (including the
Materialize operator and Materialize instances and data), run from the
Terraform directory:

<div class="highlight">

``` chroma
terraform destroy
```

</div>

When prompted to proceed, type `yes` to confirm the deletion.

## See Also

- [Materialize Operator
  Configuration](/docs/installation/configuration/)
- [Troubleshooting](/docs/installation/troubleshooting/)

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/self-managed-deployments/installation/install-on-azure.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2026 Materialize Inc.

</div>
