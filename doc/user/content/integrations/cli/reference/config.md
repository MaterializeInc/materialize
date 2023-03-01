---
title: mz config
description: The `mz config` command manages global configuration parameters for `mz`.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz config` command manages [global configuration parameters] for `mz`.

## `get`

Get the value of a configuration parameter.

```shell
mz config get <NAME> [options...]
```

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to get.

### Examples

Get the default profile:

```shell
$ mz config get profile
```
```
acme-corp
```

## `list`, `ls`

List all configuration parameters.

```shell
mz config {list,ls} [options...]
```

See [Global parameters] for a description of the available configuration
parameters.

### Examples

```shell
$ mz config list
```
```
Name    | Value
--------|----------
profile | default
vault   | keychain
```

## `remove`, `rm`

Remove a configuration parameter.

```shell
mz config {remove,rm} <NAME> [options...]
```

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to remove.

### Examples

Remove the `vault` configuration parameter:

```shell
mz config remove vault
```

## `set`

Set a configuration parameter.

```shell
mz config set <NAME> <VALUE> [options...]
```

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to set.
`<VALUE>`           |                       | **Required.** The value to set the configuration parameter to.

### Examples

Set the `profile` configuration parameter to `hooli`:

```shell
mz config set profile hooli
```

## Global flags

{{% cli-global-args %}}

[global configuration parameters]: ../../configuration/#global-parameters
[Global parameters]: ../../configuration/#global-parameters
