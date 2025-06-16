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
    weight: 10
---

## Configuring Authentication

Self-managed Materialize has two authenticator kinds:
1. `None` does no authentication. The user is trusted to be who they identify as without verification.
1. `Password` requires the user to provide a password for verification.

Which authenticator to be used can be set in `spec.authenticatorKind` of the Materialize Kubernetes resource.
If unset, the default is `None`.

### Password authentication

In addition to setting `spec.authenticatorKind: Password` in the Materialize Kubernetes resource, it is necessary to also configure a password for the internal `mz_system` user.

To configure the password of the `mz_system` user, add an `external_login_password_mz_system` key to the Kubernetes `Secret` referenced in `spec.backendSecretName` of the Materialize Kubernetes resource.

#### Logging in and creating users

After enabling password authentication, only the `mz_system` user will initially be functional.
This user may be used to create other users and by the Materialize Operator when performing upgrades and other maintenance tasks.

See [CREATE ROLE](/sql/create-role) for details on creating additional users.
