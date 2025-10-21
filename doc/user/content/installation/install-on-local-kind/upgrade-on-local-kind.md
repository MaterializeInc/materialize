---
title: "Upgrade on kind"
description: "Upgrade Materialize running locally on a kind cluster."
menu:
  main:
    parent: "install-on-local-kind"
    identifier: "upgrade-on-local-kind"
---

To upgrade your Materialize instances, upgrade the Materialize operator first
and then the Materialize instances. The following tutorial upgrades your
Materialize deployment running locally on a [`kind`](https://kind.sigs.k8s.io/)
cluster.

The tutorial assumes you have installed Materialize on `kind` using the
instructions on [Install locally on kind](/installation/install-on-local-kind/).

## Version compatibility

When updating, you need to specify the Materialize Operator version,
`orchestratord` version, and the `environmentd` versions. The following table
presents the versions compatibility for the operator and the applications:

{{< yaml-table data="self_managed/self_managed_operator_compatibility" >}}

## Prerequisites

### Helm 3.2.0+

If you don't have Helm version 3.2.0+ installed, install. For details, see the
[Helm documentation](https://helm.sh/docs/intro/install/).

### `kubectl`

This tutorial uses `kubectl`. To install, refer to the [`kubectl`
documentationq](https://kubernetes.io/docs/tasks/tools/).

For help with `kubectl` commands, see [kubectl Quick
reference](https://kubernetes.io/docs/reference/kubectl/quick-reference/).

## Upgrade

{{< important >}}

The following procedure performs a rolling upgrade, where both the old and new 
Materialize instances are running before the the old instance are removed. 
When performing a rolling upgrade, ensure you have enough resources to support 
having both the old and new Materialize instances running.

{{</ important >}}

{{% self-managed/versions/upgrade/upgrade-steps-local-kind %}}

## See also

- [Materialize Operator Configuration](/installation/configuration/)
- [Troubleshooting](/installation/troubleshooting/)
- [Installation](/installation/)
