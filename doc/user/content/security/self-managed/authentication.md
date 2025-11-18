---
title: "Authentication"
description: "Authentication"
menu:
  main:
    parent: "security-sm"
    name: Authentication
    identifier: authentication-sm
    weight: 8
---

## Configuring Authentication Type

To configure the authentication type used by Self-Managed Materialize, use the
`spec.authenticatorKind` setting in conjunction with any specific configuration
for the authentication method.

The `spec.authenticatorKind` setting determines which authentication method is
used:

{{% yaml-table data="self_managed/authentication_setting" %}}

## Configuring password authentication

{{< public-preview >}}This feature{{</ public-preview >}}

Password authentication requires users to log in with a password.

To configure Self-Managed Materialize for password authentication:

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

Initially, only the `mz_system` user [^1] is available. To create additional
users:

1. Login as the `mz_system` user, using the
`external_login_password_mz_system` password,
![Image of Materialize Console login screen with mz_system user](/images/mz_system_login.png
"Materialize Console login screen with mz_system user")

1. Use [`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role) to create
   new users:

   ```mzsql
   CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
   ```

[^1]: The `mz_system` user is also used by the Materialize Operator for upgrades
and maintenance tasks.

## Configuring SASL/SCRAM authentication

{{< note >}}
SASL/SCRAM-SHA-256 authentication requires Materialize `v26.0.0` or later.
{{</ note >}}

SASL/SCRAM-SHA-256 authentication is a challenge-response authentication mechanism
that provides security for **PostgreSQL wire protocol connections**. It is
compatible with PostgreSQL clients that support SCRAM-SHA-256.

To configure Self-Managed Materialize for SASL/SCRAM authentication:

| Configuration | Description
|---------------| ------------
|`spec.authenticatorKind` | Set to `Sasl` to enable SASL/SCRAM-SHA-256 authentication for PostgreSQL connections.
|`external_login_password_mz_system` | To the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when SASL authentication is enabled.

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
  authenticatorKind: Sasl
```

### Logging in and creating users

The process is the same as for [password authentication](#logging-in-and-creating-users):

1. Login as the `mz_system` user using the `external_login_password_mz_system` password
2. Use [`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role) to create new users

User passwords are automatically stored in SCRAM-SHA-256 format in the database.

### Authentication behavior

When SASL authentication is enabled:

- **PostgreSQL connections** (e.g., `psql`, client libraries, [connection
  poolers](/integrations/connection-pooling/)) use SCRAM-SHA-256 authentication
- **HTTP/Web Console connections** use standard password authentication

This hybrid approach provides maximum security for SQL connections while maintaining
compatibility with web-based tools.

## Enabling RBAC

{{< include-md file="shared-content/enable-rbac.md" >}}

See [Access Control](/security/self-managed/access-control/) for details on role based authorization.
