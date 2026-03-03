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

{{% include-headless
"/headless/self-managed-deployments/enabled-auth-setting-warning" %}}

### Configuring SASL/SCRAM authentication

{{< note>}}
SASL/SCRAM-SHA-256 authentication requires Materialize `v26.0.0` or later.
{{< /note >}}

SASL authentication requires users to log in with a password.

When SASL authentication is enabled:
- **PostgreSQL connections** (e.g., `psql`, client libraries, [connection
  poolers](/integrations/connection-pooling/)) use SCRAM-SHA-256 authentication.
- **HTTP/Web Console connections** use standard password authentication.

This hybrid approach provides maximum security for SQL connections while
maintaining compatibility with web-based tools.

To configure Self-Managed Materialize for SASL/SCRAM authentication, update the
following fields:

| Resource | Configuration | Description
|----------|---------------| ------------
| Materialize CR | `spec.authenticatorKind` | Set to `Sasl` to enable SASL/SCRAM-SHA-256 authentication for PostgreSQL connections.
| Kubernetes Secret | `external_login_password_mz_system` | Specify the password for the `mz_system` user, who is the only user initially available. Add `external_login_password_mz_system` to the Kubernetes Secret referenced in the Materialize CR's `spec.backendSecretName` field.

The following example Kubernetes manifest includes configuration for
SASL/SCRAM-SHA-256 authentication:

{{< tabs >}}
{{< tab "v1alpha2 (v26.17+)" >}}

```hc {hl_lines="15 25"}
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
  license_key: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha2
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.12.1
  backendSecretName: materialize-backend
  authenticatorKind: Sasl
```

{{< /tab >}}
{{< tab "v1alpha1 (before v26.17)" >}}

```hc {hl_lines="15 25"}
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
  license_key: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.12.1
  backendSecretName: materialize-backend
  authenticatorKind: Sasl
```

{{< /tab >}}
{{< /tabs >}}

{{% include-headless
"/headless/self-managed-deployments/enabled-auth-setting-warning" %}}

### Configuring password authentication

{{< public-preview />}}

Password authentication requires users to log in with a password.

To configure Self-Managed Materialize for password authentication, update the following fields:

| Resource | Configuration | Description
|----------|---------------| ------------
| Materialize CR | `spec.authenticatorKind` | Set to `Password` to enable password authentication.
| Kubernetes Secret | `external_login_password_mz_system` | Specify the password for the `mz_system` user, who is the only user initially available. Add `external_login_password_mz_system` to the Kubernetes Secret referenced in the Materialize CR's `spec.backendSecretName` field.

The following example Kubernetes manifest includes configuration for password
authentication:

{{< tabs >}}
{{< tab "v1alpha2 (v26.17+)" >}}

```hc {hl_lines="15 25"}
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
  license_key: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha2
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.12.1
  backendSecretName: materialize-backend
  authenticatorKind: Password
```

{{< /tab >}}
{{< tab "v1alpha1 (before v26.17)" >}}

```hc {hl_lines="15 25"}
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
  license_key: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.12.1
  backendSecretName: materialize-backend
  authenticatorKind: Password
```

{{< /tab >}}
{{< /tabs >}}

{{% include-headless
"/headless/self-managed-deployments/enabled-auth-setting-warning" %}}

## Logging in and creating users

When authentication is enabled, only the `mz_system` user is initially
available. To create additional users:

1. Login as the `mz_system` user, using the `external_login_password_mz_system`
password. ![Image of Materialize Console login screen with mz_system
user](/images/mz_system_login.png "Materialize Console login screen with
mz_system user")

1. Use [`CREATE ROLE ... WITH LOGIN PASSWORD ...`](/sql/create-role) to create
new users:

   ```mzsql
   CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
   ```

1. Log out as `mz_system` user.

   {{< important >}}

   In general, other than the initial login to create new users, avoid using
   `mz_system` since `mz_system` also used by the Materialize Operator for
   upgrades and maintenance tasks. {{< /important >}}

1. Login as one of the created users.

## RBAC

For details on role-based access control (RBAC), including enabling RBAC, see
[Access Control](/security/self-managed/access-control/).

{{< warning >}}
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.
{{< /warning >}}

## See also

- For all Materialize CR settings, see [Materialize CRD Field
Descriptions](/installation/appendix-materialize-crd-field-descriptions/).
