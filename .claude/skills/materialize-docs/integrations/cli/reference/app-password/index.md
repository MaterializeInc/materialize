---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/app-password/
complexity: intermediate
description: The `mz app-password` command manages app passwords for your user account.
doc_type: reference
keywords:
- CREATE CI
- CREATE AN
- mz app-password
product_area: General
status: stable
title: mz app-password
---

# mz app-password

## Purpose
The `mz app-password` command manages app passwords for your user account.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz app-password` command manages app passwords for your user account.


The `mz app-password` command manages app passwords for your user account.

## `create`

Create an app password.

```shell
mz app-password create <NAME> [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | Set the name of the app password. If unspecified, `mz` automatically generates a name.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Create an app password for a continuous integration tool:

```shell
$ mz app-password create CI
```text
```text
mzp_f283gag2t3...
```bash

## `list`, `ls`

List all app passwords.

```shell
mz app-password {list,ls} [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all app passwords:

```shell
mz app-password list
```text
```text
Name        | Created at
------------|-----------------
personal    | January 21, 2022
CI          | January 23, 2022
```

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles