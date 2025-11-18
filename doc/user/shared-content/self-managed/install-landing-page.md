You can install Self-Managed Materialize on a Kubernetes cluster running locally
or on a cloud provider. Self-Managed Materialize requires:

{{% self-managed/materialize-components-list %}}

## License key

Starting in v26.0, Materialize requires a license key.

{{< yaml-table data="self_managed/license_key" >}}

## Install

### Installation guides

The following installation guides are available:

|               | Notes  |
| ------------- | -------|
| [Install locally on kind](/installation/install-on-local-kind/) |
| [Deploy Materialize to AWS Elastic Kubernetes Service (EKS)](/installation/install-on-aws/) | Uses Materialize provided Terraform |
| [Deploy Materialize to Azure Kubernetes Service (AKS)](/installation/install-on-azure/) | Uses Materialize provided Terraform |
| [Deploy Materialize to Google Kubernetes Engine (GKE)](/installation/install-on-gcp/) | Uses Materialize provided Terraform |

See also:
- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)
- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)
- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)
