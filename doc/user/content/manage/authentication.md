---
title: "Authentication"
description: "Authentication"
aliases:
  - /self-hosted/authentication/
  - /manage/authentication/
menu:
  main:
    parent: manage
    name: Authentication
    identifier: authentication
    weight: 14
---

## Configuring Authentication

To configure authentication for self-managed Materialize, use the `spec.authenticatorKind` setting. This setting determines which authentication method is used:

- `None`: Disables authentication. All users are trusted based on their claimed
  identity **without** any verification.
- `Password`: Requires users to authenticate using a password.

If `spec.authenticatorKind` is not set, the default is **None**.

### Password authentication

Password authentication requires users to authenticate with a password. To
use password authentication, set `spec.authenticatorKind` to `Password` and
configure a password for the internal `mz_system` user.


#### Configure the password for `mz_system`
To configure the password for the internal `mz_system` user, add an
`external_login_password_mz_system` key to the Kubernetes `Secret` referenced in
`spec.backendSecretName` of the Materialize Kubernetes resource.

#### Logging in and creating users

Once password authentication is enabled, only the `mz_system` user will be initially available. This user is used by the Materialize Operator for upgrades and maintenance tasks and can also be used to create additional users.

See [CREATE ROLE](/sql/create-role) for details on creating additional users.


#### Enabling RBAC
By default, role based authorization checks are not enabled when turning on password authentication. To turn on RBAC you can set the system parater
`enable_rbac_checks` to `on`. This should be done as mz_system, or provided via environmentdExtraArgs in the environment CR.
`ALTER SYSTEM SET enable_rbac_checks = 'on'`

See [CREATE ROLE](/sql/rbac) for details on role based authorization.
