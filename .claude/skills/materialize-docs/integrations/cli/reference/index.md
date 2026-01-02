---
audience: developer
canonical_url: https://materialize.com/docs/integrations/cli/reference/
complexity: intermediate
description: Reference section for `mz`, Materialize command-line interface (CLI).
doc_type: reference
keywords:
- SHOW THE
- mz Reference
- CREATE AN
- SHOW DETAILED
- Required.
- CREATE CI
product_area: General
status: stable
title: mz Reference
---

# mz Reference

## Purpose
Reference section for `mz`, Materialize command-line interface (CLI).

If you need to understand the syntax and options for this command, you're in the right place.


Reference section for `mz`, Materialize command-line interface (CLI).


---

## mz app-password


The `mz app-password` command manages app passwords for your user account.

## `create`

Create an app password.

```shell
mz app-password create <NAME> [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | Set the name of the app password. If unspecified, `mz` automatically generates a name.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Create an app password for a continuous integration tool:

```shell
$ mz app-password create CI
```text
```text
mzp_f283gag2t3...
```bash

## `list`, `ls`

List all app passwords.

```shell
mz app-password {list,ls} [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all app passwords:

```shell
mz app-password list
```text
```text
Name        | Created at
------------|-----------------
personal    | January 21, 2022
CI          | January 23, 2022
```bash

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles


---

## mz config


The `mz config` command manages [global configuration parameters] for `mz`.

## `get`

Get the value of a configuration parameter.

```shell
mz config get <NAME> [options...]
```text

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to get.

### Examples

Get the default profile:

```shell
$ mz config get profile
```text
```text
acme-corp
```bash

## `list`, `ls`

List all configuration parameters.

```shell
mz config {list,ls} [options...]
```text

See [Global parameters] for a description of the available configuration
parameters.

### Examples

```shell
$ mz config list
```text
```text
Name    | Value
--------|----------
profile | default
vault   | keychain
```bash

## `remove`, `rm`

Remove a configuration parameter.

```shell
mz config {remove,rm} <NAME> [options...]
```text

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to remove.

### Examples

Remove the `vault` configuration parameter:

```shell
mz config remove vault
```bash

## `set`

Set a configuration parameter.

```shell
mz config set <NAME> <VALUE> [options...]
```text

See [Global parameters] for a description of the available configuration
parameters.

### Arguments

Argument            | Environment variables | Description
--------------------|-----------------------|------------
`<NAME>`            |                       | **Required.** The name of the configuration parameter to set.
`<VALUE>`           |                       | **Required.** The value to set the configuration parameter to.

### Examples

Set the `profile` configuration parameter to `hooli`:

```shell
mz config set profile hooli
```bash

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[global configuration parameters]: ../../configuration/#global-parameters
[Global parameters]: ../../configuration/#global-parameters


---

## mz profile


The `mz profile` command manages [authentication profiles] for `mz`.

## `init`

Initialize an authentication profile by exchanging your user account
credentials for an app password.

```shell
mz profile init [options...]
```bash

### Arguments

Argument                    | Environment variables | Description
----------------------------|-----------------------|------------
`--force`, `‑‑no-force`     |                       | Force reauthentication if the profile already exists.
`--browser`, `‑‑no-browser` |                       | If set, open a web browser to authenticate. Otherwise, prompt for a username and password on the terminal.
`--region=<REGION>`         |                       | Set the default region for the profile.
`--profile=<PROFILE>`       | `MZ_PROFILE`          | Use the specified [authentication profile].


### Examples

```shell
$ mz profile init --no-browser
```text
```text
Email: remote@example.com
Password: ...
Successfully logged in.
```bash

## `list`, `ls`

List available authentication profiles.

```shell
mz profile {list,ls} [options...]
```bash

### Examples

```shell
$ mz profile list
```text
```text
Name
------------
development
production
staging
```bash

## `remove`, `rm`

Remove an authentication profile.

```shell
mz profile {remove,rm} [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove the `acme-corp` profile:

```shell
mz profile remove --profile=acme-corp
```bash

## `config get`

Get a configuration parameter in an authentication profile.

```shell
mz profile config get <NAME> <VALUE> [options...]
```bash

### Flags

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | **Required.** The name of the configuration parameter to get.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Get the default region for the `acme-corp` profile:

```shell
$ mz profile config get region --profile=acme-corp
```text
```text
aws/us-east-1
```bash

## `config list`, `config ls`

List all configuration parameters in an authentication profile.

```shell
mz profile config {list,ls} [options...]
```bash

### Examples

```bash
$ mz profile config list

Name                   | Value
-----------------------|---------
profile                | default
vault                  | keychain
```bash

## `config set`

Set a configuration parameter in an authentication profile.

```shell
mz profile config set <NAME> <VALUE> [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | **Required.** The name of the configuration parameter to set.
`<VALUE>`             |                       | **Required.** The value to set the configuration parameter to.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Set the default region for the active profile:

```shell
mz profile config set region aws/eu-west-1
```bash

## `config remove`, `config rm`

Remove a configuration parameter in an authentication profile.

```shell
mz profile config {remove,rm} <NAME> [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | **Required.** The name of the configuration parameter to remove.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove the default region for the active profile:

```shell
mz profile config rm region
```bash

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profiles]: ../../configuration/#authentication-profiles


---

## mz region


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
```bash

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles


---

## mz secret


The `mz secret` command manages secrets in a region.

## `create`

Create a new secret.

```shell
mz secret create <NAME> [options...]
```text

The secret's value is read from the standard input stream.

By default, the command returns an error if a secret with the provided name
already exists. Pass `--force` to instead update the existing secret with the
new value, if it exists.

> **Note:** 
Using this command is preferred to executing [`CREATE SECRET`](/sql/create-secret) directly, as it avoids leaving the
secret's value in your shell history.


### Arguments

Flag                    | Environment variables | Description
------------------------|-----------------------|------------
`<NAME>`                |                       | **Required.** The name of the secret.
`--database=<DATABASE>` |                       | The database in which to create the secret.<br>Default: `materialize`.
`--schema=<SCHEMA>`     |                       | The schema in which to create the secret.<br>Default: the first schema in the user's default `search_path`.
`--force`               |                       | Overwrite the existing value of the secret, if it exists.
`--profile=<PROFILE>`   | `MZ_PROFILE`          | Use the specified [authentication profile].


## Global arguments

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles


---

## mz sql


The `mz sql` command executes SQL statements in a region.

```shell
mz sql [options...] [-- psql options...]
```bash

## Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--region=<REGION>`   | `MZ_REGION`           | Use the specified region.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

## Examples

Launch a SQL shell against the `aws/us-east-1` region:

```shell
mz sql --region=aws/us-east-1
```text

Execute a single SQL query against the default region for the profile:

```shell
mz sql -- -c "SELECT * FROM mz_sources"
```bash

## Global flags

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles


---

## mz user


The `mz user` command manages users in your organization.

## `create`

Invite a user to your organization.

```shell
mz user create <EMAIL> <NAME> [options...]
```bash

### Arguments

Flag                  | Environment variables | Description
----------------------|-----------------------|------------
`<EMAIL>`             |                       | **Required.** Set the email address of the user.
`<NAME>`              |                       | **Required.** Set the name of the user.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Invite Franz Kafka to your organization:

```shell
mz user create franz@kafka.org "Franz Kafka"
```bash

## `list`, `ls`

List all users in your organization.

```shell
mz user {list,ls} [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

List all users in your organization:

```shell
$ mz user list
```text
```text
Email            | Name
-----------------|-------------
franz@kafka.org  | Franz Kafka
```bash

## `remove`, `rm`

Remove a user from your organization.

```shell
mz user {remove,rm} <EMAIL> [options...]
```bash

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<EMAIL>`             |                       | **Required.** The email address of the user to remove.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove Franz Kafka from your organization:

```shell
mz user remove franz@kafka.org
```

## Global arguments

<!-- Unresolved shortcode: <!-- Unresolved shortcode: <!-- See original docs: cli-global-args --> --> -->

[authentication profile]: ../../configuration/#authentication-profiles