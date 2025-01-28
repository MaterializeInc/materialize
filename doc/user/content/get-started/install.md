---
title: "Install self-managed Materialize"
description: "Install self-managed Materialize."
disable_list: true
menu:
  main:
    parent: get-started
    identifier: "install"
    weight: 10

---

You can install self-managed Materialize on a Kubernetes cluster running locally
or on a cloud provider.

For self-managed Materialize, Materialize provides:

{{% self-managed/self-managed-details %}}

{{< callout >}}

{{< self-managed/also-available >}}

{{</ callout >}}

## Install locally

{{< multilinkbox >}}
{{< linkbox title="Using Docker/kind" >}}
[Install locally on kind](/installation/install-on-local-kind/)
{{</ linkbox >}}
{{< linkbox  title="Using Docker/minikube" >}}
[Install locally on minikube](/installation/install-on-local-minikube/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Install on cloud provider

{{< multilinkbox >}}
{{< linkbox title="AWS" >}}
[Install on AWS](/installation/install-on-aws/)
{{</ linkbox >}}
{{< linkbox title="GCP" >}}
[Install on GCP](/installation/install-on-gcp/)
{{</ linkbox >}}
{{</ multilinkbox >}}
