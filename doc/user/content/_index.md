---
title: "Self-managed Materialize"
description: ""
aliases:
  - /self-hosted/
  - /cloud_releases/
disable_list: true
---

With self-managed Materialize, you can deploy and operate Materialize in your
Kubernetes environment. For self-managed Materialize, Materialize provides:

- A Materialize Operator that manages your Materialize running in your
  Kubernetes environment.

- Materialize packaged as a containerized application that can be deployed in a
  Kubernetes cluster.

{{< callout >}}

### Also available!

- Materialize is also available as a managed cloud service. You can sign up for
  a [free
  trial](https://materialize.com/register/?utm_campaign=General&utm_source=documentation).
  For more information, see the [Materialize
  Cloud](https://materialize.com/docs) documentation.

- You can also run Materialize locally using the [Materialize Emulator Docker
  image](/get-started/install-materialize-emulator/).

{{</ callout >}}

## Requirements

The self-managed Materialize requires the following:

{{% self-managed/materialize-components-list %}}

See the [Installation guide](/installation/) for more information.

## Recommended instance types

Materialize has been tested to work on instances with the following properties:

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
