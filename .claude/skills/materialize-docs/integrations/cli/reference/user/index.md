---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/user/
complexity: intermediate
description: The `mz user` command manages users in your organization.
doc_type: reference
keywords:
- mz user
- CREATE FRANZ
- Required.
product_area: General
status: stable
title: mz user
---

# mz user

## Purpose
The `mz user` command manages users in your organization.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz user` command manages users in your organization.


The `mz user` command manages users in your organization.

## `create`

Invite a user to your organization.

```shell
mz user create <EMAIL> <NAME> [options...]
```bash

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
```bash

## `list`, `ls`

List all users in your organization.

```shell
mz user {list,ls} [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all users in your organization:

```shell
$ mz user list
```text
```text
Email            | Name
-----------------|-------------
franz@kafka.org  | Franz Kafka
```bash

## `remove`, `rm`

Remove a user from your organization.

```shell
mz user {remove,rm} <EMAIL> [options...]
```bash

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

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles