# mz region

The `mz region` command manages regions in your organization.



The `mz region` command manages regions in your organization.

## `enable`

Enable a region.

```shell
mz region enable [options...]
```

> **Warning:** You cannot disable a region with `mz`. To disable a region, contact support.
>


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

Argument           | Environment variables     | Description
-------------------|---------------------------|-----------------------------------------
`‑‑config`         | `MZ_CONFIG`               | Set the [configuration file](/integrations/cli/configuration).<br>Default: `$HOME/.config/materialize/mz.toml`.
`‑f`, `‑‑format`   | `MZ_FORMAT`               | Set the output format: `text` , `json`, or `csv`.<br>Default: `text`.
`‑‑no‑color`       | `NO_COLOR`, `MZ_NO_COLOR` | Disable color output.
`‑‑help`           |                           | Display help and exit.
`‑‑version`        |                           | Display version and exit.


[authentication profile]: ../../configuration/#authentication-profiles
