# mz sql

The `mz sql` command executes SQL statements in a region.



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

Argument           | Environment variables     | Description
-------------------|---------------------------|-----------------------------------------
`‑‑config`         | `MZ_CONFIG`               | Set the [configuration file](/integrations/cli/configuration).<br>Default: `$HOME/.config/materialize/mz.toml`.
`‑f`, `‑‑format`   | `MZ_FORMAT`               | Set the output format: `text` , `json`, or `csv`.<br>Default: `text`.
`‑‑no‑color`       | `NO_COLOR`, `MZ_NO_COLOR` | Disable color output.
`‑‑help`           |                           | Display help and exit.
`‑‑version`        |                           | Display version and exit.


[authentication profile]: ../../configuration/#authentication-profiles
