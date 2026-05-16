# mzx

`mzx` is a command-line tool for managing Materialize cloud regions, profiles,
users, app-passwords, and secrets via the Materialize cloud control plane API.
The source lives at [`src/mzx/`](../../src/mzx).

## Status

`mzx` was previously distributed and documented as the `mz` CLI. It's no longer
a supported product surface and is not documented for end users. The binary
continues to ship because internal mzcompose tests (`test/mz-e2e/`,
`test/cloud-canary/`, `test/cluster-spec-sheet/`) rely on it to drive the
cloud control plane during CI.

This is **not** the path users take to interact with a Materialize region from
a terminal. For interactive SQL work, use a standard PostgreSQL client against
the region's pgwire endpoint.

## Build

```shell
cargo build --bin mzx
./target/debug/mzx --help
```

## Internal usage

The mzcompose service `mzx` (defined in
[`misc/python/materialize/mzcompose/services/mzx.py`](../../misc/python/materialize/mzcompose/services/mzx.py))
wraps the binary in a Docker container. Tests invoke it via `c.run("mzx", ...)`
to enable cloud regions, create app passwords, manage secrets, and so on.

## Configuration

`mzx` reads a single TOML file at `$HOME/.config/materialize/mz.toml`. You can
manage it through the [`config`](#config) and [`profile`](#profile) commands or
edit it directly.

### Global parameters

| Name      | Type   | Description                                                                                                                                                                                                                                                                       |
| --------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `profile` | string | Name of the authentication profile to use by default. Default: `default`.                                                                                                                                                                                                         |
| `vault`   | string | Where to store secret configuration parameters: `inline` (in the configuration file) or `keychain` (system keychain, macOS only). Default: `keychain` on macOS, `inline` on Linux.                                                                                                |

### Authentication profiles

Each profile has a name, an associated app password, and a default region. You
can store multiple profiles and switch between them with `--profile=<NAME>` on
any command, or change the default with `mzx config set profile <NAME>`.

| Field             | Type   | Description                                                                                  |
| ----------------- | ------ | -------------------------------------------------------------------------------------------- |
| `app-password`    | string | *Secret.* The app password to use for this profile.                                          |
| `region`          | string | The default region to use for this profile.                                                  |
| `vault`           | string | Per-profile vault override.                                                                  |
| `cloud-endpoint`  | string | *Internal use only.* Override the Materialize API endpoint.                                  |
| `admin-endpoint`  | string | *Internal use only.* Override the Materialize administration endpoint.                      |

### Example `mz.toml`

```toml
profile = "acme-corp"
vault = "inline"

[profiles.acme-corp]
app-password = "mzp_fg91g4fslgq329023..."
region = "aws/us-east-1"

[profiles.hooli]
app-password = "mzp_a48df1039ecb2d94c..."
region = "aws/eu-west-1"
```

## Global flags

These flags can be combined with any subcommand.

| Argument         | Environment variables       | Description                                                                                  |
| ---------------- | --------------------------- | -------------------------------------------------------------------------------------------- |
| `--config`       | `MZ_CONFIG`                 | Path to the configuration file. Default: `$HOME/.config/materialize/mz.toml`.                |
| `--format`, `-f` | `MZ_FORMAT`                 | Output format: `text`, `json`, or `csv`. Default: `text`.                                    |
| `--no-color`     | `NO_COLOR`, `MZ_NO_COLOR`   | Disable color output.                                                                        |
| `--profile`      | `MZ_PROFILE`                | Authentication profile to use.                                                               |
| `--region`       | `MZ_REGION`                 | Region to operate on (when applicable).                                                      |
| `--help`         |                             | Display help and exit.                                                                       |
| `--version`      |                             | Display version and exit.                                                                    |

## Commands

### `app-password`

Manage app passwords for your user account.

#### `mzx app-password create <NAME>`

Create an app password. `<NAME>` is optional; one is generated if omitted.

```shell
$ mzx app-password create CI
mzp_f283gag2t3...
```

#### `mzx app-password list` (alias `ls`)

List all app passwords.

```shell
$ mzx app-password list
Name        | Created at
------------|-----------------
personal    | January 21, 2022
CI          | January 23, 2022
```

### `config`

Manage the global configuration parameters described above.

#### `mzx config get <NAME>`

```shell
$ mzx config get profile
acme-corp
```

#### `mzx config list` (alias `ls`)

```shell
$ mzx config list
Name    | Value
--------|----------
profile | default
vault   | keychain
```

#### `mzx config set <NAME> <VALUE>`

```shell
mzx config set profile hooli
```

#### `mzx config remove <NAME>` (alias `rm`)

```shell
mzx config remove vault
```

### `profile`

Manage authentication profiles.

#### `mzx profile init`

Exchange your account credentials for an app password and store them in a
profile.

| Argument                   | Description                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------ |
| `--force`, `--no-force`    | Force reauthentication if the profile already exists.                                      |
| `--browser`, `--no-browser`| Open a web browser to authenticate, or prompt for username/password on the terminal.       |
| `--region=<REGION>`        | Set the default region for the profile.                                                    |

```shell
$ mzx profile init --no-browser
Email: remote@example.com
Password: ...
Successfully logged in.
```

#### `mzx profile list` (alias `ls`)

```shell
$ mzx profile list
Name
------------
development
production
staging
```

#### `mzx profile remove` (alias `rm`)

```shell
mzx profile remove --profile=acme-corp
```

#### `mzx profile config get <NAME>`

```shell
$ mzx profile config get region --profile=acme-corp
aws/us-east-1
```

#### `mzx profile config list` (alias `ls`)

```shell
$ mzx profile config list
Name           | Value
---------------|---------------
region         | aws/us-east-1
app-password   | mzp_xxxxxxxx...
```

#### `mzx profile config set <NAME> <VALUE>`

```shell
mzx profile config set region aws/eu-west-1
```

#### `mzx profile config remove <NAME>` (alias `rm`)

```shell
mzx profile config rm region
```

### `region`

Manage cloud regions for the organization the active profile belongs to.

#### `mzx region enable`

Enable a region. **Note:** there is no `disable` subcommand — disabling a
region requires support intervention.

| Argument             | Description                       |
| -------------------- | --------------------------------- |
| `--region=<REGION>`  | Region to enable.                 |

```shell
$ mzx region enable --region=aws/us-east-1
Region enabled.
```

#### `mzx region list` (alias `ls`)

```shell
$ mzx region list
Region                  | Status
------------------------|---------
aws/us-east-1           | enabled
aws/eu-west-1           | enabled
```

#### `mzx region show`

Show detailed status for a region.

```shell
$ mzx region show --region=aws/us-east-1
Healthy:      yes
SQL address:  2358g2t42.us-east-1.aws.materialize.cloud:6875
HTTP URL:     https://2358g2t42.us-east-1.aws.materialize.cloud
```

### `secret`

Manage secrets in a region.

#### `mzx secret create <NAME>`

The secret's value is read from stdin. Pass `--force` to overwrite an existing
secret of the same name.

| Argument                | Description                                                                          |
| ----------------------- | ------------------------------------------------------------------------------------ |
| `<NAME>`                | **Required.** Name of the secret.                                                    |
| `--database=<DATABASE>` | Database in which to create the secret. Default: `materialize`.                      |
| `--schema=<SCHEMA>`     | Schema in which to create the secret. Default: first schema in `search_path`.        |
| `--force`               | Overwrite if the secret already exists.                                              |

Using this command is preferred over running `CREATE SECRET` directly, since
the value is taken from stdin and never lands in shell history.

```shell
echo -n "hunter2" | mzx secret create kafka_password
```

### `sql`

Open a `psql` session against a region (or run a single statement).

```shell
mzx sql [options...] [-- psql options...]
```

| Argument             | Description                       |
| -------------------- | --------------------------------- |
| `--region=<REGION>`  | Region to connect to.             |

`mzx sql` connects via `psql`, so any options after `--` are passed through.

```shell
# Open an interactive SQL shell.
mzx sql --region=aws/us-east-1

# Execute a single statement.
mzx sql -- -c "SELECT * FROM mz_sources"
```

### `user`

Manage users in the organization.

#### `mzx user create <EMAIL> <NAME>`

```shell
mzx user create franz@kafka.org "Franz Kafka"
```

#### `mzx user list` (alias `ls`)

```shell
$ mzx user list
Email            | Name
-----------------|-------------
franz@kafka.org  | Franz Kafka
```

#### `mzx user remove <EMAIL>` (alias `rm`)

```shell
mzx user remove franz@kafka.org
```

## Preserved compatibility surface

The binary still uses the legacy `mz` identifiers for configuration and
packaging, so existing user state continues to work:

- Config file: `~/.config/materialize/mz.toml`
- Upgrade-check cache: `~/.mz.ver`
- Postgres `application_name` for `mzx sql`: `mz_psql`
- `psql` resource file: `~/.psqlrc-mz`
- Upgrade-check URL: `https://binaries.materialize.com/mz-latest.version`
- Debian package: `materialize-cli`
- Homebrew formula: `mz`
