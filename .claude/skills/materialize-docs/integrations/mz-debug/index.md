---
audience: developer
canonical_url: https://materialize.com/docs/integrations/mz-debug/
complexity: intermediate
description: Materialize debug tool for self-managed and emulator environments.
doc_type: reference
keywords:
- mz-debug
product_area: General
status: stable
title: mz-debug
---

# mz-debug

## Purpose
Materialize debug tool for self-managed and emulator environments.

If you need to understand the syntax and options for this command, you're in the right place.


Materialize debug tool for self-managed and emulator environments.


`mz-debug` is a command-line interface tool that collects debug information for self-managed and emulator Materialize environments. By default, the tool creates a compressed file (`.zip`) containing logs and a dump of the system catalog. You can then share this file with support teams when investigating issues.

## Install `mz-debug`


This section covers install `mz-debug`.

#### macOS


```shell
ARCH=$(uname -m)
sudo echo "Preparing to extract mz-debug..."
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-apple-darwin.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1
```json

#### Linux

```shell
ARCH=$(uname -m)
sudo echo "Preparing to extract mz-debug..."
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-unknown-linux-gnu.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1


### Get version and help

To see the version of `mz-debug`, specify the `--version` flag:

```shell
mz-debug --version
```text

To see the options for running `mz-debug`,

```shell
mz-debug --help
```bash

## Next steps

To run `mz-debug`, see
- [`mz-debug self-managed`](./self-managed)
- [`mz-debug emulator`](./emulator)


---

## mz-debug emulator


`mz-debug emulator` debugs Docker-based Materialize deployments. It collects:

- Docker logs and resource information.
- Snapshots of system catalog tables from your Materialize instance.

## Requirements

- Docker installed and running. If [Docker](https://www.docker.com/) is not installed, refer to its
[official documentation](https://docs.docker.com/get-docker/) to install
- A valid Materialize SQL connection URL for your local emulator.

## Syntax

This section covers syntax.

```shell
mz-debug emulator [OPTIONS]
```bash

## Options

This section covers options.

### `mz-debug emulator` options

<!-- Dynamic table: mz-debug/emulator_options - see original docs -->

### `mz-debug` global options

<!-- Dynamic table: mz-debug/mz_debug_option - see original docs -->

## Output

The `mz-debug` outputs its log file (`tracing.log`) and the generated debug
files into a directory named `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/` as well as zips
the directory and its contents `mz_debug_YYYY-MM-DD-HH-TMM-SSZ.zip`.

The generated debug files are in two main categories: [Docker resource
files](#docker-resource-files) and [system catalog
files](#system-catalog-files).

### Docker resource files

In `mz_debug_YYYY-MM-DD-HH-TMM-SSZ/`, under the `docker/<CONTAINER-ID>`
sub-directory,  the following Docker resource debug files are generated:

<!-- Dynamic table: mz-debug/docker_resource_files - see original docs -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/system-catalog-fil --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/prometheus-files --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/memory-profiles --> --> -->

## Example

This section covers example.

### Debug a running local emulator container
```console
mz-debug emulator \
    --docker-container-id 123abc456def
```text


---

## mz-debug self-managed


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

This section covers syntax.

```console
mz-debug self-managed [OPTIONS]
```bash

## Options

This section covers options.

## `mz-debug self-managed` options

<!-- Dynamic table: mz-debug/self_managed_options - see original docs -->

## `mz-debug` global options

<!-- Dynamic table: mz-debug/mz_debug_option - see original docs -->

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

<!-- Dynamic table: mz-debug/kubernetes_resource_files - see original docs -->

Each resource type directory also contains a `describe.txt` file with the output of `kubectl describe` for that resource type.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/system-catalog-fil --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/prometheus-files --> --> -->

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: integrations/mz-debug/memory-profiles --> --> -->


## Prerequisite: Get the Materialize instance name

To use `mz-debug`, you need to specify the <a href="#k8s-namespace">Kubernetes namespace (`--k8s-namespace`)</a> and the <a href="#mz-instance-name">Materialize instance name (`--mz-instance-name`)</a>. To retrieve the Materialize instance name, you can use kubectl. For example, the following retrieves the name of the Materialize instance(s) running in the Kubernetes namespace `materialize-environment`:
```text
kubectl --namespace materialize-environment get materializes.materialize.cloud
```text
The command should return the NAME of the Materialize instance(s) in the namespace:
```text
NAME
12345678-1234-1234-1234-123456789012
```bash

## Examples

This section covers examples.

### Debug a Materialize instance running in a namespace

The following example uses `mz-debug` to collect debug information for the Materialize instance (`12345678-1234-1234-1234-123456789012` obtained in the Prerequisite) running in the Kubernetes namespace `materialize-environment`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012
```bash

### Include information from additional kubernetes namespaces

When debugging a Materialize instance, you can also include information from other namespaces via <a href="#additional-k8s-namespace">`--additional-k8s-namespace`</a>. The following example collects debug information for the Materialize instance running in the Kubernetes namespace `materialize-environment` as well as debug information for the namespace `materialize`:

```shell
mz-debug self-managed --k8s-namespace materialize-environment \
--mz-instance-name 12345678-1234-1234-1234-123456789012 \
--additional-k8s-namespace materialize
```