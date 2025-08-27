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

## Configuring Authentication Type

To configure the authentication type used by self-managed Materialize, use the
`spec.authenticatorKind` setting in conjunction with any specific configuration
for the authentication method.

The `spec.authenticatorKind` setting determines which authentication method is
used:

{{% yaml-table data="self_managed/authentication_setting" %}}

## Configuring password authentication

{{< public-preview >}}This feature{{</ public-preview >}}

Password authentication requires users to log in with a password.

To configure self-managed Materialize for password authentication:

 Configuration | Description
---------------| ------------
`spec.authenticatorKind` | Set to `Password` to enable password authentication.
`external_login_password_mz_system` | To the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when password authentication is enabled.

For example, if using Kind, in the `sample-materialize.yaml` file:

```hc {hl_lines="14 24"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: "..."
  persist_backend_url: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v0.147.2
  backendSecretName: materialize-backend
  authenticatorKind: Password
```

#### Logging in and creating users

![Image of Materialize Console login screen with mz_system user](/images/mz_system_login.png
"Materialize Console login screen with mz_system user")

Initially, only the `mz_system` user [^1] is available. To create additional
users, login as the `mz_system` user, using the
`external_login_password_mz_system` password, and use
[`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role):

```mzsql
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```

[^1]: The `mz_system` user is also used by the Materialize Operator for upgrades
and maintenance tasks.

## Enabling RBAC

{{< include-md file="shared-content/enable-rbac.md" >}}

See [Access Control](/manage/access-control/) for details on role based authorization.
