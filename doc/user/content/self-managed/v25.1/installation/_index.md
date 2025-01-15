---
title: "Installation"
description: "Installation guides for self-managed Materialize."
disable_list: true
aliases:
  - /self-managed/installation/
suppress_breadcrumb: true
---

You can install self-managed Materialize on a Kubernetes cluster running locally
or on a cloud provider.

## Install locally

{{< multilinkbox >}}
{{< linkbox title="Using Docker/kind" >}}
[Install locally on kind](/self-managed/installation/install-on-local-kind/)
{{</ linkbox >}}
{{< linkbox  title="Using Docker/minikube" >}}
[Install locally on minikube](/self-managed/installation/install-on-local-minikube/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## Install on cloud provider

{{< multilinkbox >}}
{{< linkbox title="AWS" >}}
[Install on AWS](/self-managed/installation/install-on-aws/)
{{</ linkbox >}}
{{< linkbox title="GCP" >}}
[Install on GCP](/self-managed/installation/install-on-gcp/)
{{</ linkbox >}}
{{</ multilinkbox >}}

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Materialize Operator Configuration](/self-managed/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Upgrading](/self-managed/upgrading/)
