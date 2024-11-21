---
title: "Install on GCP (Placeholder Stub)"
description: ""
---

The following tutorial deploys Materialize onto GCP.

{{< important >}}

For testing purposes only. For testing purposes only.  For testing purposes only. ....

{{< /important >}}

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, refer to the [Helm
documentation](https://helm.sh/docs/intro/install/).

### Kubernetes

Materialize supports [Kubernetes 1.19+](https://kubernetes.io/docs/setup/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

### Materialize repo

The following instructions assume that you are installing from the [Materialize
repo](https://github.com/MaterializeInc/materialize).

{{< important >}}

{{% self-managed/git-checkout-branch %}}

{{< /important >}}

## Configuration for your GCP account

1. Go to the Materialize repo directory.

1. Edit the `misc/helm-charts/operator/values.yaml` with your GCP account
   information.

## See also

- [Materialize Kubernetes Operator Helm Chart](/self-managed/)
- [Materialize Operator Configuration](/self-managed/configuration/)
- [Troubleshooting](/self-managed/troubleshooting/)
- [Operational guidelines](/self-managed/operational-guidelines/)
- [Installation](/self-managed/installation/)
- [Upgrading](/self-managed/upgrading/)
