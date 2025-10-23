---
title: "Install self-managed Materialize"
description: "Install self-managed Materialize."
disable_list: true
menu:
  main:
    parent: get-started
    identifier: "install"
    weight: 17

---

You can install self-managed Materialize on a Kubernetes cluster running locally
or on a cloud provider. Self-managed Materialize requires:

{{% self-managed/materialize-components-list %}}

## Install locally

{{< multilinkbox >}}
{{< linkbox title="Using Docker/kind" icon="materialize">}}
[Install locally on kind](/self-managed/installation/install-on-local-kind/)
{{</ linkbox >}}
{{< linkbox  title="Using Docker/minikube" icon="materialize">}}
[Install locally on minikube](/self-managed/installation/install-on-local-minikube/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Install on cloud provider

{{< multilinkbox >}}

{{< linkbox title="[Install on AWS](/self-managed/installation/install-on-aws/)" icon="materialize">}}

[Deploy Materialize to AWS Elastic Kubernetes Service (EKS)](/self-managed/installation/install-on-aws/)

{{</ linkbox >}}


{{< linkbox title="[Install on Azure](/self-managed/installation/install-on-azure/)" icon="materialize">}}

[Deploy Materialize to Azure Kubernetes Service (AKS)](/self-managed/installation/install-on-azure/)

{{</ linkbox >}}

{{< linkbox icon="materialize" title="[Install on GCP](/self-managed/installation/install-on-gcp/)" >}}

[Deploy Materialize to Google Kubernetes Engine (GKE)](/self-managed/installation/install-on-gcp/)
{{</ linkbox >}}

{{</ multilinkbox >}}

See also:

- [AWS Deployment
  guidelines](/self-managed/installation/install-on-aws/appendix-deployment-guidelines/#recommended-instance-types)

- [GCP Deployment
  guidelines](/self-managed/installation/install-on-gcp/appendix-deployment-guidelines/#recommended-instance-types)

- [Azure Deployment
  guidelines](/self-managed/installation/install-on-azure/appendix-deployment-guidelines/#recommended-instance-types)

{{< callout >}}

{{< self-managed/also-available >}}

{{</ callout >}}
