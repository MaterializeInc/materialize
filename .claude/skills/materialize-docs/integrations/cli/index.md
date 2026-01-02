---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/
complexity: advanced
description: The Materialize command-line interface (CLI).
doc_type: reference
keywords:
- authentication profiles
- UPDATE SUDO
- CREATE APP
- mz - Materialize CLI
- SHOW ENABLED
product_area: General
status: stable
title: mz - Materialize CLI
---

# mz - Materialize CLI

## Purpose
The Materialize command-line interface (CLI).

If you need to understand the syntax and options for this command, you're in the right place.


The Materialize command-line interface (CLI).


The Materialize command-line interface (CLI), lets you interact with
Materialize from your terminal.

You can use `mz` to:

  * Enable new regions
  * Run SQL commands against a region
  * Create app passwords
  * Securely manage secrets
  * Invite new users to your organization

## Getting started

1. Install `mz`:

   ```shell
   # On macOS:
   brew install materializeinc/materialize/mz
   # On Ubuntu/Debian:
   curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
   sudo apt update
   sudo apt install materialize-cli
   ```text

   See [Installation](installation) for additional installation options.

2. Log in to your Materialize account:

   ```shell
   mz profile init
   ```text

   `mz` will attempt to launch your web browser and ask you to log in.

   See [Configuration](configuration) for alternative configuration methods.

3. Show enabled regions in your organization:

   ```shell
   $ mz region list
   ```text
   ```text
   aws/us-east-1  enabled
   aws/eu-west-1  disabled
   ```text

4. Launch a SQL shell connected to one of the enabled regions in your
   organization:

   ```shell
   $ mz sql
   ```text
   ```text
   Authenticated using profile 'default'.
   Connected to the quickstart cluster.
   psql (14.2)
   Type "help" for help.

   materialize=>
   ```text

   You can use the `--region=aws/us-east-1` flag with the name of an enabled region
   in your organization. If you don't yet have an enabled region, use
   [`mz region enable`](reference/region) to enable one.

## Command reference

Command          | Description
-----------------|------------
[`app-password`] | Manage app passwords for your user account.
[`config`]       | Manage configuration for `mz`.
[`profile`]      | Manage authentication profiles for `mz`.
[`region`]       | Manage regions in your organization.
[`secret`]       | Manage secrets in a region.
[`sql`]          | Execute SQL statements in a region.
[`user`]         | Manage users in your organization.

## Global flags

These flags can be used with any command and may be intermixed with any
command-specific flags.

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[Homebrew]: https://brew.sh
[homebrew-tap]: https://github.com/MaterializeInc/homebrew-materialize
[`app-password`]: reference/app-password
[`config`]: reference/config
[`profile`]: reference/profile
[`region`]: reference/region
[`secret`]: reference/secret
[`sql`]: reference/sql
[`user`]: reference/user


---

## Materialize CLI Configuration


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
```text

When the profile is not explicitly specified, `mz` uses the default profile
specified in the configuration file. You can change the default profile using
`mz config`. For example, to set the default profile to `hooli`:

```shell
mz config set profile hooli
```bash

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
```text


---

## Materialize CLI Installation


We offer several installation methods for `mz` on macOS and Linux.

## macOS

On macOS, we we recommend using Homebrew.

### Homebrew

You'll need [Homebrew] installed on your system. Then install `mz` from
[our tap][homebrew-tap]:

```shell
brew install materializeinc/materialize/mz
```bash

### Binary download

```shell
curl -L https://binaries.materialize.com/mz-latest-$(uname -m)-apple-darwin.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
```bash

## Linux

On Linux, we recommend using APT, if supported by your distribution.

### apt (Ubuntu, Debian, or variants)

```shell
curl -fsSL https://dev.materialize.com/apt/materialize.sources | sudo tee /etc/apt/sources.list.d/materialize.sources
sudo apt update
sudo apt install materialize-cli
```bash

### Binary download

```shell
curl -L https://binaries.materialize.com/mz-latest-$(uname -m)-unknown-linux-gnu.tar.gz \
    | sudo tar -xzC /usr/local --strip-components=1
```bash

## Docker

You can use the `materialize/mz` Docker image to run `mz` on any platform that
is supported by Docker. You'll need to mount your local `~/.mz` directory in the
container to ensure that configuration settings and authentiation tokens outlive
the container.

```text
docker run -v $HOME/.mz:/root/.mz materialize/mz [args...]
```

[Homebrew]: https://brew.sh
[homebrew-tap]: https://github.com/MaterializeInc/homebrew-materialize


---

## mz Reference