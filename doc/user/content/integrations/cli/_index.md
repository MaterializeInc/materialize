---
title: Materialize CLI
description: The Materialize command-line interface (CLI).
menu:
  main:
    parent: integrations
    name: CLI
    identifier: cli
    weight: 6
disable_list: true
---

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
   ```

   See [Installation](installation) for additional installation options.

2. Log in to your Materialize account:

   ```shell
   mz profile init
   ```

   `mz` will attempt to launch your web browser and ask you to log in.

   See [Configuration](configuration) for alternative configuration methods.

3. Show enabled regions in your organization:

   ```shell
   $ mz region list
   ```
   ```
   aws/us-east-1  enabled
   aws/eu-west-1  disabled
   ```

4. Launch a SQL shell connected to one of the enabled regions in your
   organization:

   ```shell
   $ mz sql
   ```
   ```
   Authenticated using profile 'default'.
   Connected to the quickstart cluster.
   psql (14.2)
   Type "help" for help.

   materialize=>
   ```

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

{{% cli-global-args %}}

[Homebrew]: https://brew.sh
[homebrew-tap]: https://github.com/MaterializeInc/homebrew-materialize
[`app-password`]: reference/app-password
[`config`]: reference/config
[`profile`]: reference/profile
[`region`]: reference/region
[`secret`]: reference/secret
[`sql`]: reference/sql
[`user`]: reference/user
