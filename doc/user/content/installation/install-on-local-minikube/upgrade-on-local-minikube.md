---
title: "Upgrade on minikube"
description: "Upgrade Materialize running locally on a minikube cluster."
menu:
  main:
    parent: "install-on-local-minikube"
---

To upgrade your Materialize instances, upgrade the Materialize operator first
and then the Materialize instances. The following tutorial upgrades your
Materialize deployment running locally on a
[`minikube`](https://minikube.sigs.k8s.io/docs/start/) cluster.

The tutorial assumes you have installed Materialize on `minikube` using the
instructions on [Install locally on minikube](/installation/install-on-local-minikube/).

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl` documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Upgrade

{{% self-managed/versions/upgrade/upgrade-steps-local-minikube %}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Operational guidelines](/installation/operational-guidelines/)
- [Installation](/installation/)
