---
title: "mz-debug"
description: Materialize debug tool for self-managed and emulator environments.
menu:
  main:
    parent: integrations
    name: "`mz-debug` Debug tool"
    identifier: mz-debug
    weight: 7
disable_list: true
---

`mz-debug` is a command-line interface tool that collects debug information for self-managed and emulator Materialize environments. By default, the tool creates a compressed file (`.zip`) containing logs and a dump of the system catalog. You can then share this file with support teams when investigating issues.

## Install `mz-debug`

{{< tabs >}}

{{< tab "macOS" >}}

```shell
# On macOS:
ARCH=$(uname -m)
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-apple-darwin.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1
```
{{</ tab >}}
{{< tab "Ubuntu/Debian" >}}
```shell
# On Ubuntu/Debian:
curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
sudo apt update
sudo apt install materialize-cli
{{</ tab >}}
{{< tab "Docker" >}}
```shell
# Docker
docker run materialize/mz-debug
```
{{</ tab >}}
{{</ tabs >}}

## Install `kubectl`

`mz-debug` requires `kubectl` (Kubernetes command-line tool) v1.32.3+. Install
[kubectl](https://kubernetes.io/docs/tasks/tools/) , the official Kubernetes
command-line tool.

## Next steps

To run `mz-debug`, see 
- [`mz-debug self-managed`](./self-managed)
- [`mz-debug emulator`](./emulator)
