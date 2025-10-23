---
title: "Self-managed Materialize"
description: ""
aliases:
  - /self-hosted/
menus:
  main:
    weight: 3
    identifier: 'self-managed'
disable_list: true
---

Materialize is a real-time data integration platform that enables you to use SQL
to transform, deliver, and act on fast changing data.

## Self-managed Materialize

With self-managed Materialize, you can deploy and operate Materialize in your
Kubernetes environment.

{{% self-managed/self-managed-editions-table %}}

{{< callout >}}

{{< self-managed/also-available >}}

- [Materialize MCP Server](https://materialize.com/blog/materialize-turns-views-into-tools-for-agents/).
  The Materialize MCP Server bridges SQL and AI by transforming indexed views into well-typed tools
  that agents can call directly. It lets you expose your freshest, most complex logic as operational
  data products automatically and reliably. For more information, see the
  [Materialize MCP Server](/integrations/llm/) documentation.

{{</ callout >}}

## Requirements

The self-managed Materialize requires the following:

{{% self-managed/materialize-components-list %}}

See the [Installation guide](./installation/) for more information.

## Installation

For instructions on installing Materialize on your Kubernetes cluster, see:

- [Install locally on kind](./installation/install-on-local-kind/)
- [Install locally on
  minikube](./installation/install-on-local-minikube/)
- [Install on AWS](./installation/install-on-aws/)
- [Install on GCP](./installation/install-on-gcp/)
- [Install on Azure](./installation/install-on-azure/)
