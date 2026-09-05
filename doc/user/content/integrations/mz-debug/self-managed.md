---
title: "mz-debug self-managed"
description: Use mz-debug to debug Self-Managed Materialize Kubernetes environments.
menu:
  main:
    parent: mz-debug
    weight: 10
---

`mz-debug self-managed` downloads diagnostics for a Kubernetes-based
Materialize deployment from the **debug collector** that the Materialize
operator runs alongside each instance. A snapshot contains:

- Kubernetes resources, their `describe` output, and pod logs from the
  instance's namespace.
- Heap profiles and Prometheus metrics of `environmentd` and every `clusterd`
  process.
- CPU profiles of `environmentd` and every `clusterd` process (on request).
- Snapshots of system catalog tables from your Materialize instance.

## How it works

The collector runs inside the cluster as a Deployment owned by a
`MaterializeDebug` resource. It takes a snapshot of the instance on a fixed
interval (30 minutes by default) and keeps the most recent snapshots (12 by
default, capped at 2 GiB) in a ring buffer, so that diagnostics from *before*
an incident are available when you investigate it. Periodic snapshots include
only the pod logs since the previous snapshot; concatenating the retained
snapshots reconstructs the log history.

When you run `mz-debug self-managed`, it:

1. Finds the instance's `MaterializeDebug` resource.
2. Port-forwards to the collector's Service.
3. Asks the collector for a fresh snapshot, which by default includes every
   category above, CPU profiles included, and waits for it.
4. Downloads the snapshot as `mz_debug_<instance>_<snapshot id>.zip`.

Because the collector runs inside the cluster, `mz-debug` itself only needs
`kubectl` access to read the `MaterializeDebug` resource and port-forward to
one Service.

## Requirements

- Materialize operator with the debug collector enabled. Set
  `debugCollector.enabled: true` in the operator's Helm values to run a
  collector for every instance, or create a `MaterializeDebug` resource for
  an instance by hand (see [Enable the collector for a single
  instance](#enable-the-collector-for-a-single-instance)).

  If the operator predates the debug collector, `mz-debug` reports that the
  cluster has no `MaterializeDebug` kind. Upgrade the operator, or use the
  `mz-debug` release matching the operator's version.

- [`kubectl`](https://kubernetes.io/docs/tasks/tools/) v1.32.3+ with access to
  the cluster.

## Syntax

```console
mz-debug self-managed [OPTIONS]
```

## Options

## `mz-debug self-managed` options

{{< yaml-table data="mz-debug/self_managed_options" >}}

## `mz-debug` global options

The global options select what the fresh snapshot collects. They have no
effect with `--no-fresh-snapshot`.

{{< yaml-table data="mz-debug/mz_debug_option" >}}

## Output

`mz-debug self-managed` saves each downloaded snapshot as
`mz_debug_<instance>_<snapshot id>.zip` in the current directory. The snapshot
id encodes when the snapshot was taken and whether it was periodic or requested
(`on-demand`), for example `2026-08-27T10-30-00Z-on-demand`.

Extracting a zip produces a directory `mz_debug_<snapshot id>/` holding a
`snapshot.json` manifest (instance, time, categories collected) and the debug
files, which are in two main categories: [Kubernetes resource
files](#kubernetes-resource-files) and [system catalog
files](#system-catalog-files).

### Kubernetes resource files

Under `mz_debug_<snapshot id>/`, the following Kubernetes resource debug files
are generated:

{{< yaml-table data="mz-debug/kubernetes_resource_files" >}}

Each resource type directory also contains a `describe.txt` file with a
summary of every object of that type, in the format of `kubectl describe`.

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

The operator names an instance's `MaterializeDebug` resource after the
instance. To check that a collector is running for it:

```
kubectl --namespace materialize-environment get materializedebugs.materialize.cloud
```
```
NAME                                   MATERIALIZE                            READY
12345678-1234-1234-1234-123456789012   12345678-1234-1234-1234-123456789012   True
```

## Examples

### Debug a Materialize instance running in a namespace

The following example downloads a fresh snapshot of the Materialize instance
(`12345678-1234-1234-1234-123456789012` obtained in the Prerequisite) running
in the Kubernetes namespace `materialize-environment`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012
```

### Download the collector's history

To see how an instance got into its current state, download every snapshot the
collector has retained rather than only a fresh one:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012 \
--all-snapshots
```

### Skip the fresh snapshot

If the instance is too unhealthy to snapshot, take what the collector already
has. This returns immediately with the latest retained snapshot:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012 \
--no-fresh-snapshot
```

### Enable the collector for a single instance

With `debugCollector.enabled: false` in the operator's Helm values (the
default), no collector runs. To run one for a single instance, create a
`MaterializeDebug` resource naming it. The operator derives everything else,
including the collector image, from the instance:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: MaterializeDebug
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  materializeName: 12345678-1234-1234-1234-123456789012
  # Optional. Shown with their defaults.
  snapshotInterval: 30m
  retainedSnapshots: 12
  bufferSizeLimit: 2Gi
  additionalNamespaces:
    - materialize
```

If the resource's name differs from the instance's, pass it to `mz-debug` with
`--debug-name`. See the [MaterializeDebug CRD field
descriptions](/self-managed-deployments/materialize-debug-crd-field-descriptions/)
for every field.
