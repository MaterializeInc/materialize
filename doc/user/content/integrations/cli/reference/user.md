---
title: mz user
description: The `mz user` command manages users in your organization.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz user` command manages users in your organization.

## `create`

Invite a user to your organization.

```shell
mz user create <EMAIL> <NAME> [options...]
```

### Arguments

Flag                  | Environment variables | Description
----------------------|-----------------------|------------
`<EMAIL>`             |                       | **Required.** Set the email address of the user.
`<NAME>`              |                       | **Required.** Set the name of the user.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Invite Franz Kafka to your organization:

```shell
mz user create franz@kafka.org "Franz Kafka"
```

## `list`, `ls`

List all users in your organization.

```shell
mz user {list,ls} [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all users in your organization:

```shell
$ mz user list
```
```
Email            | Name
-----------------|-------------
franz@kafka.org  | Franz Kafka
```

## `remove`, `rm`

Remove a user from your organization.

```shell
mz user {remove,rm} <EMAIL> [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<EMAIL>`             |                       | **Required.** The email address of the user to remove.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove Franz Kafka from your organization:

```shell
mz user remove franz@kafka.org
```

## Global arguments

{{% cli-global-args %}}

[authentication profile]: ../../configuration/#authentication-profiles
