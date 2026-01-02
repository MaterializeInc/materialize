---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/secret/
complexity: intermediate
description: The `mz secret` command manages users in a region.
doc_type: reference
keywords:
- mz secret
- CREATE THE
- UPDATE THE
- CREATE A
- CREATE SECRET
- Required.
- 'Note:'
product_area: General
status: stable
title: mz secret
---

# mz secret

## Purpose
The `mz secret` command manages users in a region.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz secret` command manages users in a region.


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

> **Note:** 
Using this command is preferred to executing [`CREATE SECRET`](/sql/create-secret) directly, as it avoids leaving the
secret's value in your shell history.


### Arguments

Flag                    | Environment variables | Description
------------------------|-----------------------|------------
`<NAME>`                |                       | **Required.** The name of the secret.
`--database=<DATABASE>` |                       | The database in which to create the secret.<br>Default: `materialize`.
`--schema=<SCHEMA>`     |                       | The schema in which to create the secret.<br>Default: the first schema in the user's default `search_path`.
`--force`               |                       | Overwrite the existing value of the secret, if it exists.
`--profile=<PROFILE>`   | `MZ_PROFILE`          | Use the specified [authentication profile].


## Global arguments

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles