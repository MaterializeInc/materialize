# mz app-password
The `mz app-password` command manages app passwords for your user account.
The `mz app-password` command manages app passwords for your user account.

## `create`

Create an app password.

```shell
mz app-password create <NAME> [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | Set the name of the app password. If unspecified, `mz` automatically generates a name.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Create an app password for a continuous integration tool:

```shell
$ mz app-password create CI
```
```
mzp_f283gag2t3...
```

## `list`, `ls`

List all app passwords.

```shell
mz app-password {list,ls} [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all app passwords:

```shell
mz app-password list
```
```
Name        | Created at
------------|-----------------
personal    | January 21, 2022
CI          | January 23, 2022
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
