---
title: "Installation"
description: "Installation guides for self-managed Materialize."
disable_list: true

menu:
  main:
    identifier: "installation"
    weight: 5

---

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

{{< linkbox title="[Install on AWS](/installation/install-on-aws/)" icon="materialize">}}

[Deploy Materialize to AWS Elastic Kubernetes Service (EKS)](/installation/install-on-aws/)

{{</ linkbox >}}

{{< linkbox title="[Install on Azure](/installation/install-on-azure/)" icon="materialize">}}

[Deploy Materialize to Azure Kubernetes Service (AKS)](/installation/install-on-azure/)

{{</ linkbox >}}

{{< linkbox icon="materialize" title="[Install on GCP](/installation/install-on-gcp/)" >}}

[Deploy Materialize to Google Kubernetes Engine (GKE)](/installation/install-on-gcp/)
{{</ linkbox >}}

{{</ multilinkbox >}}

{{< callout >}}
{{< self-managed/also-available >}}
{{</ callout >}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
