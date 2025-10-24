You can install self-managed Materialize on a Kubernetes cluster running locally
or on a cloud provider. Self-managed Materialize requires:

{{% self-managed/materialize-components-list %}}

## Install locally

{{< multilinkbox >}}
{{< linkbox title="Using Docker/kind" icon="materialize">}}
[Install locally on kind](/installation/install-on-local-kind/)
{{</ linkbox >}}
{{< linkbox  title="Using Docker/minikube" icon="materialize">}}
[Install locally on minikube](/installation/install-on-local-minikube/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Install on cloud provider

{{< multilinkbox >}}

{{< linkbox title="Install on AWS" icon="materialize">}}

[Deploy Materialize to AWS Elastic Kubernetes Service (EKS)](/installation/install-on-aws/)

{{</ linkbox >}}

{{< linkbox title="Install on Azure" icon="materialize">}}

[Deploy Materialize to Azure Kubernetes Service (AKS)](/installation/install-on-azure/)

{{</ linkbox >}}

{{< linkbox icon="materialize" title="Install on GCP" >}}

[Deploy Materialize to Google Kubernetes Engine (GKE)](/installation/install-on-gcp/)
{{</ linkbox >}}

{{</ multilinkbox >}}

See also:

- [AWS Deployment
  guidelines](/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)

- [GCP Deployment
  guidelines](/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)

- [Azure Deployment
  guidelines](/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)
