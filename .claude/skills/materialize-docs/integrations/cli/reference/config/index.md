---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/config/
complexity: intermediate
description: The `mz config` command manages global configuration parameters for `mz`.
doc_type: reference
keywords:
- mz config
- Required.
product_area: General
status: stable
title: mz config
---

# mz config

## Purpose
The `mz config` command manages global configuration parameters for `mz`.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz config` command manages global configuration parameters for `mz`.


The `mz config` command manages [global configuration parameters] for `mz`.

## `get`

Get the value of a configuration parameter.

```shell
mz config get <NAME> [options...]
```text

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
```text
```text
acme-corp
```bash

## `list`, `ls`

List all configuration parameters.

```shell
mz config {list,ls} [options...]
```text

See [Global parameters] for a description of the available configuration
parameters.

### Examples

```shell
$ mz config list
```text
```text
Name    | Value
--------|----------
profile | default
vault   | keychain
```bash

## `remove`, `rm`

Remove a configuration parameter.

```shell
mz config {remove,rm} <NAME> [options...]
```text

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
```bash

## `set`

Set a configuration parameter.

```shell
mz config set <NAME> <VALUE> [options...]
```text

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

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[global configuration parameters]: ../../configuration/#global-parameters
[Global parameters]: ../../configuration/#global-parameters