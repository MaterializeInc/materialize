---
title: mz region
description: The `mz region` command manages regions in your organization.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz region` command manages regions in your organization.

## `enable`

Enable a region.

```shell
mz region enable [options...]
```

{{< warning >}}
You cannot disable a region with `mz`. To disable a region, contact support.
{{< /warning >}}

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified authentication profile.

### Examples

Enable the `aws/us-east-1` region:

```shell
$ mz region enable --region=aws/us-east-1
```
```
Region enabled.
```

## `list`, `ls`

List all regions.

```shell
mz region {list,ls}
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].


### Examples

```shell
$ mz region list
```
```
Region                  | Status
------------------------|---------
aws/us-east-1           | enabled
aws/eu-west-1           | enabled
```

## `show`

Show detailed status for a region.

```shell
mz region show [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].


### Examples

Show the status of the `aws/us-east-1` region:

```shell
$ mz region show --region=aws/us-east-1
```
```
Healthy:      yes
SQL address:  2358g2t42.us-east-1.aws.materialize.cloud:6875
HTTP URL:     https://2358g2t42.us-east-1.aws.materialize.cloud
```

## Global flags

{{% cli-global-args %}}

[authentication profile]: ../../configuration/#authentication-profiles
