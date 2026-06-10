---
title: "mz-debug self-managed"
description: Use mz-debug to debug Self-Managed Materialize Kubernetes environments.
menu:
  main:
    parent: mz-debug
    weight: 10
---

`mz-debug self-managed` debugs Kubernetes-based Materialize deployments. It
collects:

- Logs and resource information from pods, daemonsets, and other Kubernetes
    resources.

- Snapshots of system catalog tables from your Materialize instance.

By default, the tool will automatically port-forward to collect system catalog information. You can disable this by specifying your own connection URL via `--mz-connection-url`.

## Requirements

`mz-debug` requires [`kubectl`](https://kubernetes.io/docs/tasks/tools/)
v1.32.3+. Install [kubectl](https://kubernetes.io/docs/tasks/tools/) if you do
not have it installed.

## Syntax

```console
mz-debug self-managed [OPTIONS]
```

## Options

## `mz-debug self-managed` options

{{< yaml-table data="mz-debug/self_managed_options" >}}

## `mz-debug` global options

{{< yaml-table data="mz-debug/mz_debug_option" >}}

## Output

The `mz-debug` outputs its log file (`tracing.log`) and the generated debug
files into a directory named `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/` as well as zips
the directory and its contents `mz_debug_YYYY-MM-DD-HH-TMM-SSZ.zip`.

The generated debug files are in two main categories: [Kubernetes resource
files](#kubernetes-resource-files) and [system catalog
files](#system-catalog-files).

### Kubernetes resource files

Under `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/`, the following Kubernetes resource debug
files are generated:

{{< yaml-table data="mz-debug/kubernetes_resource_files" >}}

Each resource type directory also contains a `describe.txt` file with the output of `kubectl describe` for that resource type.

{{% integrations/mz-debug/system-catalog-files %}}

{{% integrations/mz-debug/prometheus-files %}}

{{% integrations/mz-debug/memory-profiles %}}



## Prerequisite: Get the Materialize instance name

To use `mz-debug`, you need to specify the <a href="#k8s-namespace">Kubernetes namespace (`--k8s-namespace`)</a> and the <a href="#mz-instance-name">Materialize instance name (`--mz-instance-name`)</a>. To retrieve the Materialize instance name, you can use kubectl. For example, the following retrieves the name of the Materialize instance(s) running in the Kubernetes namespace `materialize-environment`:
```
kubectl --namespace materialize-environment get materializes.materialize.cloud
```
The command should return the NAME of the Materialize instance(s) in the namespace:
```
NAME
12345678-1234-1234-1234-123456789012
```

## Examples

### Debug a Materialize instance running in a namespace

The following example uses `mz-debug` to collect debug information for the Materialize instance (`12345678-1234-1234-1234-123456789012` obtained in the Prerequisite) running in the Kubernetes namespace `materialize-environment`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012
```

### Include information from additional kubernetes namespaces

When debugging a Materialize instance, you can also include information from other namespaces via <a href="#additional-k8s-namespace">`--additional-k8s-namespace`</a>. The following example collects debug information for the Materialize instance running in the Kubernetes namespace `materialize-environment` as well as debug information for the namespace `materialize`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012 \
--additional-k8s-namespace materialize
```
