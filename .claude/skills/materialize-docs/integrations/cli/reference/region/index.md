---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/region/
complexity: intermediate
description: The `mz region` command manages regions in your organization.
doc_type: reference
keywords:
- 'Warning:'
- SHOW THE
- SHOW DETAILED
- mz region
product_area: General
status: stable
title: mz region
---

# mz region

## Purpose
The `mz region` command manages regions in your organization.

If you need to understand the syntax and options for this command, you're in the right place.


The `mz region` command manages regions in your organization.


The `mz region` command manages regions in your organization.

## `enable`

Enable a region.

```shell
mz region enable [options...]
```text

> **Warning:** 
You cannot disable a region with `mz`. To disable a region, contact support.


### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified authentication profile.

### Examples

Enable the `aws/us-east-1` region:

```shell
$ mz region enable --region=aws/us-east-1
```text
```text
Region enabled.
```bash

## `list`, `ls`

List all regions.

```shell
mz region {list,ls}
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].


### Examples

```shell
$ mz region list
```text
```text
Region                  | Status
------------------------|---------
aws/us-east-1           | enabled
aws/eu-west-1           | enabled
```bash

## `show`

Show detailed status for a region.

```shell
mz region show [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].


### Examples

Show the status of the `aws/us-east-1` region:

```shell
$ mz region show --region=aws/us-east-1
```text
```text
Healthy:      yes
SQL address:  2358g2t42.us-east-1.aws.materialize.cloud:6875
HTTP URL:     https://2358g2t42.us-east-1.aws.materialize.cloud
```

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles