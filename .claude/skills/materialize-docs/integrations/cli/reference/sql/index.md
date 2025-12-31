---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/sql/
complexity: intermediate
description: The `mz sql` command executes SQL statements in a region.
doc_type: reference
keywords:
- mz sql
product_area: Indexes
status: stable
title: mz sql
---

# mz sql

## Purpose
The `mz sql` command executes SQL statements in a region.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz sql` command executes SQL statements in a region.


The `mz sql` command executes SQL statements in a region.

```shell
mz sql [options...] [-- psql options...]
```bash

## Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

## Examples

Launch a SQL shell against the `aws/us-east-1` region:

```shell
mz sql --region=aws/us-east-1
```text

Execute a single SQL query against the default region for the profile:

```shell
mz sql -- -c "SELECT * FROM mz_sources"
```

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles