# Legacy Terraform Releases

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
