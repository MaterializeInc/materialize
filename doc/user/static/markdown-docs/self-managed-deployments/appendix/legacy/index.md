# Legacy Terraform: Releases and configurations



## Table of contents



---

## Legacy Terraform Releases



## Legacy Terraform Modules


| Sample Module | Description |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-helm-materialize" >terraform-helm-materialize (Legacy)</a> | A sample Terraform module for installing the Materialize Helm chart into a Kubernetes cluster. |
| <a href="https://github.com/MaterializeInc/terraform-aws-materialize" >Materialize on AWS (Legacy)</a> | A sample Terraform module for deploying Materialize on AWS Cloud Platform with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-aws-legacy/" >Install on AWS (Legacy)</a> for an example usage. |
| <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize" >Materialize on Azure (Legacy)</a> | A sample Terraform module for deploying Materialize on Azure with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-azure-legacy/" >Install on Azure</a> for an example usage. |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize" >Materialize on GCP (Legacy)</a> | A sample Terraform module for deploying Materialize on Google Cloud Platform (GCP) with all required infrastructure components. See <a href="/self-managed-deployments/installation/legacy/install-on-gcp-legacy/" >Install on GCP</a> for an example usage. |


## Materialize on AWS Terraform module (Legacy) {#materialize-on-aws-terraform-module}


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-aws-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |


If upgrading from a deployment that was set up using an earlier version of the
Terraform modules, additional considerations may apply when using an updated Terraform modules to your existing deployments.


See also [Upgrade Notes](
https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#upgrade-notes)
for release-specific upgrade notes.


## Materialize on Azure Terraform module (Legacy){#materialize-on-azure-terraform-module}


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-azurerm-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |


If upgrading from a deployment that was set up using an earlier version of the
Terraform modules, additional considerations may apply when using an updated
Terraform modules to your existing deployments.


## Materialize on GCP Terraform module (Legacy) {#materialize-on-gcp-terraform-module}


| Terraform version | Notable changes |
| --- | --- |
| <a href="https://github.com/MaterializeInc/terraform-google-materialize/releases/tag/v0.6.4" >v0.6.4</a> | <ul> <li>Released as part of v26.0.0.</li> <li>Uses <code>terraform-helm-materialize</code> version <code>v0.1.35</code>.</li> </ul>  |


If upgrading from a deployment that was set up using an earlier version of the
Terraform modules, additional considerations may apply when using an updated
Terraform modules to your existing deployments.



---

## Required configuration (Legacy AWS Terraform)


## Required variables

The following variables are required when using the [Materialize on AWS
Terraform modules](https://github.com/MaterializeInc/terraform-aws-materialize):


| Variable |
| --- |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">namespace</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">environment</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">database_password</span> <span class="o">=</span>  <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |


For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf`
file](https://github.com/MaterializeInc/terraform-aws-materialize/blob/main/variables.tf).

## Required providers

Starting in [Materialize on AWS Terraform module
v0.3.0](https://github.com/MaterializeInc/terraform-aws-materialize), you need
to declare the following providers:

```hcl
provider "aws" {
  region = "us-east-1"  # or some other region
  # Specify additional AWS provider configuration as needed
}

# Required for EKS authentication
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
    command     = "aws"
  }
}

# Required for Materialize Operator installation
provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      args        = ["eks", "get-token", "--cluster-name", module.eks.cluster_name]
      command     = "aws"
    }
  }
}
```

## Swap support

Starting in v0.6.1 of Materialize on AWS Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration option is available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-aws-materialize?tab=readme-ov-file#v061).


---

## Required configuration (Legacy Azure Terraform)


When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following configurations are required. [^1]

## Required variables

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following variables must be set: [^1]


| Variable |
| --- |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">resource_group_name</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">database_config</span> <span class="o">=</span> { </span></span><span class="line"><span class="cl"><span class="n">  password</span>            <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span><span class="c1">  # required </span></span></span><span class="line"><span class="cl"><span class="c1">  # sku_name          = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # postgres_version  = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # username          = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # db_name           = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl">} </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">network_config</span> <span class="o">=</span> {<span class="c1">     # required starting in v0.2.0 </span></span></span><span class="line"><span class="cl"><span class="n">  vnet_address_space</span>   <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  subnet_cidr</span>          <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  postgres_subnet_cidr</span> <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  service_cidr</span>         <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  docker_bridge_cidr</span>   <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl">} </span></span></code></pre></div> |


For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/variables.tf).

## Resource group

When using the root `main.tf` file from the [Materialize on Azure Terraform
module](https://github.com/MaterializeInc/terraform-azurerm-materialize), an
Azure Resource Group `azurerm_resource_group` is required.[^1] You can either
create a new resource group or use an existing resource group:

- **To create a new resource group**, declare the resource group to create in
  your configuration:

  ```hcl
  resource "azurerm_resource_group" "materialize" {
    name     = var.resource_group_name
    location = var.location                    # Defaults to eastus2
    tags     = var.tags                        # Optional
  }
  ```

- **To use an existing resource group**, set the [`resource_group_name`
  variable](https://github.com/MaterializeInc/terraform-azurerm-materialize/blob/main/variables.tf)
  to that group's name.

## Required providers

When using the root `main.tf` file from the [Materialize on Azure Terraform
module
v0.2.0+](https://github.com/MaterializeInc/terraform-azurerm-materialize), the
following provider declarations are required: [^1]

```hcl
provider "azurerm" {
  # Set the Azure subscription ID here or use the ARM_SUBSCRIPTION_ID environment variable
  # subscription_id = "XXXXXXXXXXXXXXXXXXX"

  # Specify addition Azure provider configuration as needed

  features { }
}


provider "kubernetes" {
  host                   = module.aks.cluster_endpoint
  client_certificate     = base64decode(module.aks.kube_config[0].client_certificate)
  client_key             = base64decode(module.aks.kube_config[0].client_key)
  cluster_ca_certificate = base64decode(module.aks.kube_config[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = module.aks.cluster_endpoint
    client_certificate     = base64decode(module.aks.kube_config[0].client_certificate)
    client_key             = base64decode(module.aks.kube_config[0].client_key)
    cluster_ca_certificate = base64decode(module.aks.kube_config[0].cluster_ca_certificate)
  }
}
```

## Swap support

Starting in v0.6.1 of Materialize on Azure Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration option is available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-azurerm-materialize?tab=readme-ov-file#v061).

[^1]: If using the `examples/simple/main.tf`, the example configuration handles
them for you.


---

## Required configuration (Legacy GCP Terraform)


## Required variables

The following variables are required when using the [Materialize on Google Cloud
Provider Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize).


| Variable |
| --- |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">project_id</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">prefix</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">database_config</span> <span class="o">=</span> { </span></span><span class="line"><span class="cl"><span class="n">  password</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span><span class="c1">  # required </span></span></span><span class="line"><span class="cl"><span class="c1">  # tier     = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # version  = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # username = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # db_name  = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl">} </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">network_config</span> <span class="o">=</span> {<span class="c1">     # required starting in v0.3.0 </span></span></span><span class="line"><span class="cl"><span class="n">  subnet_cidr</span>   <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span><span class="line"><span class="cl"><span class="n">  pods_cidr</span>     <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span><span class="line"><span class="cl"><span class="n">  services_cidr</span> <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span> </span></span><span class="line"><span class="cl">} </span></span></code></pre></div> |


For a list of all variables, see the
[README.md](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#inputs)
or the [`variables.tf` file](https://github.com/MaterializeInc/terraform-google-materialize/blob/main/variables.tf).

## Required providers and data source declaration

To use [Materialize on Google Cloud Terraform
module](https://github.com/MaterializeInc/terraform-google-materialize) v0.2.0+,
you need to declare:

- The following providers:

  ```hcl
  provider "google" {
    project = var.project_id
    region  = var.region
    # Specify additional Google provider configuration as needed
  }

  # Required for GKE authentication
  provider "kubernetes" {
    host                   = "https://${module.gke.cluster_endpoint}"
    token                  = data.google_client_config.current.access_token
    cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
  }

  provider "helm" {
    kubernetes {
      host                   = "https://${module.gke.cluster_endpoint}"
      token                  = data.google_client_config.current.access_token
      cluster_ca_certificate = base64decode(module.gke.cluster_ca_certificate)
    }
  }
  ```

- The following data source:

  ```hcl
  data "google_client_config" "current" {}
  ```

## Swap support

Starting in v0.6.1 of Materialize on Google Cloud Provider (GCP) Terraform,
disk support (using swap on NVMe instance storage) may be enabled for
Materialize. With this change, the Terraform:

- Creates a node group for Materialize.
- Configures NVMe instance store volumes as swap using a daemonset.
- Enables swap at the Kubelet.

For swap support, the following configuration options are available:

- [`swap_enabled`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_swap_enabled)

See [Upgrade Notes](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#v061).

## Storage bucket versioning

Starting in v0.3.1 of Materialize on GCP Terraform, storage bucket versioning is
disabled (i.e.,
[`storage_bucket_versioning`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_versioning)
is set to `false` by default) to facilitate cleanup of resources during testing.
When running in production, versioning should be turned on with a sufficient TTL
([`storage_bucket_version_ttl`](https://github.com/MaterializeInc/terraform-google-materialize?tab=readme-ov-file#input_storage_bucket_version_ttl))
to meet any data-recovery requirements.
