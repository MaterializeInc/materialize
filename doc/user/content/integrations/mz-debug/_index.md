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

```console
$ mz-debug [OPTIONS] <SUBCOMMAND>
```

For details on the available subcommands, see:

- [Self-managed](./self-managed)
- [Emulator](./emulator)

## Installation

```shell
   # On macOS:
   curl -L https://binaries.materialize.com/mz-debug-latest-$(uname -m)-apple-darwin.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
   # On Ubuntu/Debian:
   curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
   sudo apt update
   sudo apt install materialize-cli
   # Docker
   docker run materialize/mz-debug
   ```

{{% integrations/mz-debug/global-flags %}}