---
title: mz sql
description: The `mz sql` command executes SQL statements in a region.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz sql` command executes SQL statements in a region.

```shell
mz sql [options...] [-- psql options...]
```

## Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

## Examples

Launch a SQL shell against the `aws/us-east-1` region:

```shell
mz sql --region=aws/us-east-1
```

Execute a single SQL query against the default region for the profile:

```shell
mz sql -- -c "SELECT * FROM mz_sources"
```

## Global flags

{{% cli-global-args %}}

[authentication profile]: ../../configuration/#authentication-profiles
