---
title: mz profile
description: The `mz profile` command manages authentication profiles for `mz`.
menu:
  main:
    parent: cli-reference
    weight: 1
---

The `mz profile` command manages [authentication profiles] for `mz`.

## `init`

Initialize an authentication profile by exchanging your user account
credentials for an app password.

```shell
mz profile init [options...]
```

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
```
```
Email: remote@example.com
Password: ...
Successfully logged in.
```

## `list`, `ls`

List available authentication profiles.

```shell
mz profile {list,ls} [options...]
```

### Examples

```shell
$ mz profile list
```
```
Name
------------
development
production
staging
```

## `remove`, `rm`

Remove an authentication profile.

```shell
mz profile {remove,rm} [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove the `acme-corp` profile:

```shell
mz profile remove --profile=acme-corp
```

## `config get`

Get a configuration parameter in an authentication profile.

```shell
mz profile config get <NAME> <VALUE> [options...]
```

### Flags

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | **Required.** The name of the configuration parameter to get.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Get the default region for the `acme-corp` profile:

```shell
$ mz profile config get region --profile=acme-corp
```
```
aws/us-east-1
```

## `config list`, `config ls`

List all configuration parameters in an authentication profile.

```shell
mz profile config {list,ls} [options...]
```

### Examples

```
$ mz profile config list

Name                   | Value
-----------------------|---------
profile                | default
vault                  | keychain
```

## `config set`

Set a configuration parameter in an authentication profile.

```shell
mz profile config set <NAME> <VALUE> [options...]
```

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
```

## `config remove`, `config rm`

Remove a configuration parameter in an authentication profile.

```shell
mz profile config {remove,rm} <NAME> [options...]
```

### Arguments

Argument              | Environment variables | Description
----------------------|-----------------------|------------
`<NAME>`              |                       | **Required.** The name of the configuration parameter to remove.
`--profile=<PROFILE>` | `MZ_PROFILE`          | Use the specified [authentication profile].

### Examples

Remove the default region for the active profile:

```shell
mz profile config rm region
```

## Global flags

{{% cli-global-args %}}

[authentication profiles]: ../../configuration/#authentication-profiles
