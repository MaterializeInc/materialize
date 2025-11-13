---
title: "Upgrade on minikube"
description: "Upgrade Materialize running locally on a minikube cluster."
menu:
  main:
    parent: "install-on-local-minikube"
---

To upgrade your Materialize instances, first choose a new operator version and upgrade the Materialize operator. Then, upgrade your Materialize instances to the same version. The following tutorial upgrades your
Materialize deployment running locally on a
[`minikube`](https://minikube.sigs.k8s.io/docs/start/) cluster.

The tutorial assumes you have installed Materialize on `minikube` using the
instructions on [Install locally on minikube](/installation/install-on-local-minikube/).

{{< include-md file="shared-content/self-managed/version-compatibility-upgrade-banner.md" >}}

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

### License key

{{< include-md file="shared-content/license-key-required.md" >}}

## Upgrade


{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new
Materialize instances are running before the the old instance are removed.
When performing a rolling upgrade, ensure you have enough resources to support
having both the old and new Materialize instances running.

{{</ important >}}

{{% self-managed/versions/upgrade/upgrade-steps-local-minikube %}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
