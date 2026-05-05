# Materialize CLI

```
The Materialize command-line interface (CLI).

USAGE:
    mz [OPTIONS] <SUBCOMMAND>

OPTIONS:
        --config <PATH>      Set the configuration file [env: MZ_CONFIG=]
        --format <FORMAT>    [env: MZ_FORMAT=] [default: text] [possible values: text, json, csv]
    -h, --help               Print help information
        --no-color           [env: MZ_NO_COLOR=]
    -V, --version            Print version information

SUBCOMMANDS:
    app-password    Manage app passwords for your user account
    config          Manage global configuration parameters for the CLI
    help            Print this message or the help of the given subcommand(s)
    profile         Manage authentication profiles for the CLI
    region          Manage regions in your organization
    secret          Manage secrets in your region
    sql             Execute SQL statements in a region
    user            Manage users in your organization
```

## Getting started

1. Install `mz`:

   ```shell
   # On macOS:
   brew install materializeinc/materialize/mz
   # On Ubuntu/Debian:
   curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
   sudo apt update
   sudo apt install materialize-cli
   ```

2. Log in to your Materialize account:

   ```shell
   mz profile init
   ```

   `mz` will launch your web browser and ask you to log in.

   See [Configuration](#configuration) for alternative configuration methods.

3. Show enabled regions in your organization:

   ```shell
   mz region list
   ```
   ```
   aws/us-east-1  enabled
   aws/eu-west-1  disabled
   aws/us-west-2  disabled
   ```

4. Launch a SQL shell connected to one of the enabled regions in your
   organization:

   ```shell
   # Selecting a specific region
   mz sql --region=aws/us-east-1

   # Using the default region
   mz sql
   ```
   ```
   psql (14.2)
   Type "help" for help.

   you@corp.com=#
   ```

   Replace `aws/us-east-1` with the name of an enabled region in your
   organization. If you don't yet have an enabled region, use
   [`mz region enable`](reference/region) to enable one.

### Configuration

`mz` is configured via a single TOML file stored at
`$HOME/.config/materialize/mz.toml`.

You typically manage configuration using the following commands:

  * [`mz config`](../reference/config), which manages global configuration
    parameters.
  * [`mz profile`](../reference/profile), which manages authentication profiles.

You can also edit the file directly, if you prefer. The format of the file is
shown in the [Example](#example) section.

### Global parameters

`mz` supports several global configuration parameters, documented in the table
below.

Name      | Type   | Description
----------|--------|------------
`profile` | string | The name of the authentication profile to use by default.<br>Default: `default`.
`vault`   | string | The default vault to use to store secret configuration parameters: `inline` or `keychain`. When set to `inline`, secrets are stored directly in the configuration file. When set to `keychain`, secrets are stored in the system keychain (macOS only).<br>Default (Linux): `inline`<br>Default (macOS): `keychain`


Use [`mz config set`](../reference/config#set) to set these parameters.

## Authentication profiles

You can configure `mz` with multiple **authentication profiles** to seamlessly
manage multiple Materialize user accounts. Each profile has a name, an
associated app password, and a default region.

When invoking an `mz` command that requires authentication, you can explicitly
choose which profile to use by passing the `--profile` flag. For example, to use
the `hooli` profile with the `mz sql` command:

```
mz sql --profile=hooli
```

When the profile is not explicitly specified, `mz` uses the default profile
specified in the configuration file. You can change the default profile using
`mz config`. For example, to set the default profile to `hooli`:

```
mz config set profile hooli
```

### Profile parameters

Field             | Type   | Description
------------------|--------|----------------------------
`app-password`    | string | *Secret.* The app password to use for this profile.
`region`          | string | The default region to use for this profile.
`vault`           | string | The vault to use for this profile. See [Global parameters](#global-parameters) above.
`cloud-endpoint`  | string | *Internal use only.* The Materialize API endpoint to use.
`admin-endpoint`  | string | *Internal use only.* The Materialize administration endpoint to use.


### Example

The following is an example `mz.toml` configuration file with two authentication
profiles.

```toml
# Activate the "acme-corp" authentication profile by default.
profile = "acme-corp"

# Store app passwords directly in the configuration file.
vault = "inline"

# Profile for Acme Corp.
[profiles.acme-corp]
# The app password that the CLI will use to authenticate.
app-password = "mzp_fg91g4fslgq329023..."
# The default region to use for the Acme Corp profile.
region = "aws/us-east-1"

# Profile for Hooli.
[profiles.hooli]
app-password = "mzp_a48df1039ecb2d94c..."
region = "aws/eu-west-1"
```
