---
title: "mz-debug self-managed"
description: Debug self-managed Kubernetes environments.
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

By default, the tool will automatically port-forward to collect system catalog information. You can disable this by setting `--auto-port-forward false` and specifying your own connection URL via `--mz-connection-url`.

## Requirements

`mz-debug` requires `kubectl` (Kubernetes command-line tool) v1.32.3+. Install
[kubectl](https://kubernetes.io/docs/tasks/tools/) , the official Kubernetes
command-line tool.

## Syntax

```console
$ mz-debug self-managed [OPTIONS]
```

## Options

### General Options

{{% integrations/mz-debug/global-flags %}}

### `self-managed` options

| Option                           | Description                                                                                     |
|----------------------------------|-------------------------------------------------------------------------------------------------|
| `--dump-k8s`                     | If true, dump debug information from the Kubernetes cluster. Defaults to `true`.                |
| `--k8s-namespace <NAMESPACE>`     | One or more namespaces to dump. Required if `--dump-k8s` is true.                             |
| `--k8s-context <CONTEXT>`        | The Kubernetes context to use. Defaults to the `KUBERNETES_CONTEXT` environment variable.       |
| `--k8s-dump-secret-values`       | Include secrets in the dump. Use with caution. Defaults to `false`.                            |
| `--auto-port-forward`            | Automatically port-forward the external SQL port. Defaults to `true`.                           |
| `--port-forward-local-address`   | The address to listen on for port-forwarding. Defaults to `127.0.0.1`.                         |
| `--port-forward-local-port`      | The port to listen on for port-forwarding. Defaults to `6875`.                                 |
| `--mz-connection-url <URL>`      | A [PostgreSQL connection URL](https://www.postgresql.org/docs/14/libpq-connect.html#LIBPQ-CONNSTRING) of the Materialize SQL connection. Will be constructed from port-forward settings if not provided. |

## Output files

The debug tool generates files in two main categories: Kubernetes resources and System Catalog. Files are stored under a timestamped directory: `debug-YYYY-MM-DD-HH-TMM-SSZ/` and "zipped" as the directory name.

### Kubernetes Resource Files

{{< yaml-table data="mz_debug/kubernetes_resource_files" >}}

Each resource type directory also contains a `describe.txt` file with the output of `kubectl describe` for that resource type.

{{% integrations/mz-debug/system-catalog-files %}}

## Examples

**Generate a debug zip file:**
```console
mz-debug self-managed --k8s-namespace materialize --k8s-namespace materialize-environment
```

**Include secret values (use with caution):**
```console
mz-debug self-managed --k8s-namespace materialize --k8s-dump-secret-values
```

**Debug a namespace without automatic port-forwarding:**
```console
mz-debug self-managed \
    --k8s-namespace materialize \
    --k8s-namespace materialize-environment \
    --auto-port-forward false \
    --mz-connection-url 'postgres://root@127.0.0.1:6875/materialize?sslmode=disable'
```