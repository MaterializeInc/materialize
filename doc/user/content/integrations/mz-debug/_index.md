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
ARCH=$(uname -m)
sudo echo "Preparing to extract mz-debug..."
curl -L "https://binaries.materialize.com/mz-debug-latest-$ARCH-apple-darwin.tar.gz" \
| sudo tar -xzC /usr/local --strip-components=1
```
{{</ tab >}}
{{< tab "Ubuntu/Debian" >}}
```shell
curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
sudo apt update
sudo apt install materialize-cli
{{</ tab >}}
{{</ tabs >}}

### Get version and help

To see the version of `mz-debug`, specify the `--version` flag:

```shell
mz-debug --version
```

To see the options for running `mz-debug`,

```shell
mz-debug --help
```

## Next steps

To run `mz-debug`, see
- [`mz-debug self-managed`](./self-managed)
- [`mz-debug emulator`](./emulator)
