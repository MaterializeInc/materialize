---
title: "Self-managed Materialize"
description: ""
aliases:
  - /self-hosted/
  - /cloud_releases/
disable_list: true
---

Materialize is a real-time data integration platform that enables you to use SQL
to transform, deliver, and act on fast changing data.

With self-managed Materialize, you can deploy and operate Materialize in your
Kubernetes environment. For self-managed Materialize, Materialize provides:

{{% self-managed/self-managed-details %}}

{{< callout >}}

{{< self-managed/also-available >}}

{{</ callout >}}

## Requirements

The self-managed Materialize requires the following:

{{% self-managed/materialize-components-list %}}

See the [Installation guide](/installation/) for more information.

## Recommended instance types

- ARM-based CPU
- 1:8 ratio of vCPU to GiB memory
- 1:16 ratio of vCPU to GiB local instance storage (if enabling spill-to-disk)

For specific cloud provider recommendations, see the [Installation guide for the
cloud provider](/installation/) as well as the [operational guidelines](/installation/operational-guidelines/).

## Installation

For instructions on installing Materialize on your Kubernetes cluster, see:

- [Install locally on kind](/installation/install-on-local-kind/)
- [Install locally on
  minikube](/installation/install-on-local-minikube/)
- [Install on AWS](/installation/install-on-aws/)
- [Install on GCP](/installation/install-on-gcp/)
- [Install on Azure](/installation/install-on-azure/)
