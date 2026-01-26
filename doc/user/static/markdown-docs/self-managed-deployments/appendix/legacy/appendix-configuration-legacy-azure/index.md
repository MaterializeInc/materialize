# Required configuration (Legacy Azure Terraform)

Required configuration for Materialize on Azure Terraform.



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
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">database_config</span> <span class="o">=</span> { </span></span><span class="line"><span class="cl"><span class="n">  password</span>            <span class="o">=</span> <span class="err">&lt;</span><span class="k">string</span><span class="err">&gt;</span><span class="c1">  # required </span></span></span><span class="line"><span class="cl"><span class="c1">  # sku_name          = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # postgres_version  = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # username          = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1">  # db_name           = &lt;string&gt;  # optional </span></span></span><span class="line"><span class="cl"><span class="c1"></span>} </span></span></code></pre></div> |
| <div class="highlight"><pre tabindex="0" class="chroma"><code class="language-hcl" data-lang="hcl"><span class="line"><span class="cl"><span class="n">network_config</span> <span class="o">=</span> {<span class="c1">     # required starting in v0.2.0 </span></span></span><span class="line"><span class="cl"><span class="c1"></span><span class="n">  vnet_address_space</span>   <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  subnet_cidr</span>          <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  postgres_subnet_cidr</span> <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  service_cidr</span>         <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl"><span class="n">  docker_bridge_cidr</span>   <span class="o">=</span> <span class="k">string</span> </span></span><span class="line"><span class="cl">} </span></span></code></pre></div> |


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
