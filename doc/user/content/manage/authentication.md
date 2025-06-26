---
title: "Authentication"
description: "Authentication"
menu:
  main:
    parent: manage
    name: Authentication
    identifier: authentication
    weight: 8
---

## Configuring Authentication

To configure authentication for self-managed Materialize, use the `spec.authenticatorKind` setting. This setting determines which authentication method is used:

- `None`: Disables authentication. All users are trusted based on their claimed
  identity **without** any verification.
- `Password`: Requires users to authenticate using a password.

If `spec.authenticatorKind` is not set, the default is **None**.

### Password authentication

***Public Preview*** This feature may have minor stability issues.

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


### Enabling RBAC

{{< include-md file="shared-content/enable-rbac.md" >}}

See [Authorization](/manage/access-control/) for details on role based authorization.
