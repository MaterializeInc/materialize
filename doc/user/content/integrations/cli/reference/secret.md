---
title: mz secret
description: The `mz secret` command manages users in a region.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz secret` command manages secrets in a region.

## `create`

Create a new secret.

```shell
mz secret create <NAME> [options...]
```

The secret's value is read from the standard input stream.

By default, the command returns an error if a secret with the provided name
already exists. Pass `--force` to instead update the existing secret with the
new value, if it exists.

{{< note >}}
Using this command is preferred to executing [`CREATE SECRET`](/sql/create-secret) directly, as it avoids leaving the
secret's value in your shell history.
{{< /note >}}

### Arguments

Flag                    | Environment variables | Description
------------------------|-----------------------|------------
`<NAME>`                |                       | **Required.** The name of the secret.
`--database=<DATABASE>` |                       | The database in which to create the secret.<br>Default: `materialize`.
`--schema=<SCHEMA>`     |                       | The schema in which to create the secret.<br>Default: the first schema in the user's default `search_path`.
`--force`               |                       | Overwrite the existing value of the secret, if it exists.
`--profile=<PROFILE>`   | `MZ_PROFILE`          | Use the specified [authentication profile].


## Global arguments

{{% cli-global-args %}}

[authentication profile]: ../../configuration/#authentication-profiles
