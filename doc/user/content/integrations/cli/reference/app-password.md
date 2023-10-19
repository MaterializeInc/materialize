---
title: mz app-password
description: The `mz app-password` command manages app passwords for your user account.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz app-password` command manages app passwords for your user account.

## `create`

Create an app password.

```shell
mz app-password create <NAME> [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | Set the name of the app password. If unspecified, `mz` automatically generates a name.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Create an app password for a continuous integration tool:

```shell
$ mz app-password create CI
```
```
mzp_f283gag2t3...
```

## `list`, `ls`

List all app passwords.

```shell
mz app-password {list,ls} [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all app passwords:

```shell
mz app-password list
```
```
Name        | Created at
------------|-----------------
personal    | January 21, 2022
CI          | January 23, 2022
```

## Global flags

{{% cli-global-args %}}

[authentication profile]: ../../configuration/#authentication-profiles
