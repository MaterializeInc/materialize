---
title: "Materialize CLI Configuration"
description: "An authentication profile..."
menu:
  main:
    parent: cli
    name: Configuration
    weight: 2
---

`mz` is configured via a single TOML file stored at
`$HOME/.config/materialize/mz.toml`.

You typically manage configuration via the following two commands:

  * [`mz config`](../reference/config), which manages global configuration
    parameters.
  * [`mz profile`](../reference/profile), which manages authentication profiles.

You can also edit the file directly if you prefer. The format of the file is
shown in the [Example](#example) section.

## Global parameters

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

```shell
mz sql --profile=hooli
```

When the profile is not explicitly specified, `mz` uses the default profile
specified in the configuration file. You can change the default profile using
`mz config`. For example, to set the default profile to `hooli`:

```shell
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


## Example

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
