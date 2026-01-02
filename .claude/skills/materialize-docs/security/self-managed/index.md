---
audience: developer
canonical_url: https://materialize.com/docs/security/self-managed/
complexity: beginner
description: Authentication and authorization in Self-Managed Materialize.
doc_type: reference
keywords:
- CREATE ROLE
- 'Warning:'
- superuser
- SHOW ENABLE_RBAC_CHECKS
- Self-managed
- ALTER SYSTEM
- CREATE ADDITIONAL
- 'Note:'
- 'Important:'
product_area: Security
status: beta
title: Self-managed
---

# Self-managed

## Purpose
Authentication and authorization in Self-Managed Materialize.

If you need to understand the syntax and options for this command, you're in the right place.


Authentication and authorization in Self-Managed Materialize.


This section covers security for Self-Managed Materialize.

| Guide | Description |
|-------|-------------|
| [Authentication](/security/self-managed/authentication/) | Enable authentication |
| [Access control](/security/self-managed/access-control/) | Reference for role-based access management (RBAC) |

See also

- [Appendix: Privileges](/security/appendix/appendix-privileges/)
- [Appendix: Privileges by commands](/security/appendix/appendix-command-privileges/)
- [Appendix: Built-in roles](/security/appendix/appendix-built-in-roles/)


---

## Access control (Role-based)


> **Note:** 
Initially, only the `mz_system` user (which has superuser/administrator
privileges) is available to manage roles.


<a name="role-based-access-control-rbac" ></a>

## Role-based access control

In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to [database
roles](/security/self-managed/access-control/manage-roles/).

## Enabling RBAC

> **Warning:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


By default, role-based access control (RBAC) checks are not enabled (i.e.,
enforced) when using [authentication](/security/self-managed/authentication/#configuring-authentication-type). To
enable RBAC, set the system parameter `enable_rbac_checks` to `'on'` or `True`.
You can enable the parameter in one of the following ways:

- For [local installations using
  Kind/Minikube](/installation/#installation-guides), set `spec.enableRbac:
  true` option when instantiating the Materialize object.

- For [Cloud deployments using Materialize's
  Terraforms](/installation/#installation-guides), set `enable_rbac_checks` in
  the environment CR via the `environmentdExtraArgs` flag option.

- After the Materialize instance is running, run the following command as
  `mz_system` user:

  ```mzsql
  ALTER SYSTEM SET enable_rbac_checks = 'on';
  ```text

If more than one method is used, the `ALTER SYSTEM` command will take precedence
over the Kubernetes configuration.

To view the current value for `enable_rbac_checks`, run the following `SHOW`
command:

```mzsql
SHOW enable_rbac_checks;
```text

> **Important:** 
If RBAC is not enabled, all users have <red>**superuser**</red> privileges.


## Roles and privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

- To create additional users or service accounts, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE
... WITH LOGIN PASSWORD ...`](/sql/create-role):

```mzsql
CREATE ROLE <user> WITH LOGIN PASSWORD '<password>';
```text


- To create functional roles, login as the `mz_system` user,
using the `external_login_password_mz_system` password, and use [`CREATE ROLE`](/sql/create-role):

```mzsql
CREATE ROLE <role>;
```bash


### Managing privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/grant-... -->

### Initial privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/db-rol... -->

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/public... -->

In addition, all roles have:
- `USAGE` on all built-in types and [all system catalog
schemas](/sql/system-catalog/).
- `SELECT` on [system catalog objects](/sql/system-catalog/).
- All [applicable privileges](/security/appendix/appendix-privileges/) for
  an object they create; for example, the creator of a schema gets `CREATE` and
  `USAGE`; the creator of a table gets `SELECT`, `INSERT`, `UPDATE`, and
  `DELETE`.


You can modify the privileges of your organization's `PUBLIC` role as well as
the modify default privileges for `PUBLIC`.

## Privilege inheritance and modular access control

In Materialize, when you grant a role to another role (user role/service account
role/independent role), the target role inherits privileges through the granted
role.

In general, to grant a user or service account privileges, create roles with the
desired privileges and grant these roles to the user or service account role.
Although you can grant privileges directly to the user or service account role,
using separate, reusable roles is recommended for better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-sm/revoke... -->

## Best practices

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-sm" h... -->


---

## Authentication


This section covers authentication.

## Configuring Authentication Type

To configure the authentication type used by Self-Managed Materialize, use the
`spec.authenticatorKind` setting in conjunction with any specific configuration
for the authentication method.

The `spec.authenticatorKind` setting determines which authentication method is
used:

<!-- Unresolved shortcode: {{% yaml-table data="self_managed/authentication_s... -->

> **Warning:** 
Ensure that the `authenticatorKind` field is set for any future version upgrades or rollouts of the Materialize CR. Having it undefined will reset `authenticationKind` to `None`.


## Configuring password authentication

> **Public Preview:** This feature is in public preview.This feature

Password authentication requires users to log in with a password.

To configure Self-Managed Materialize for password authentication, update the following fields in the Materialize CR. For all Materialize CR settings, see [here](/installation/appendix-materialize-crd-field-descriptions/).

 Configuration | Description
---------------| ------------
`spec.authenticatorKind` | Set to `Password` to enable password authentication.
`external_login_password_mz_system` | Set to the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when password authentication is enabled.

An example Materialize CR YAML file:

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
```bash

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
   ```json

[^1]: The `mz_system` user is also used by the Materialize Operator for upgrades
and maintenance tasks.

## Configuring SASL/SCRAM authentication

> **Note:** 
SASL/SCRAM-SHA-256 authentication requires Materialize `v26.0.0` or later.


SASL/SCRAM-SHA-256 authentication is a challenge-response authentication mechanism
that provides security for **PostgreSQL wire protocol connections**. It is
compatible with PostgreSQL clients that support SCRAM-SHA-256.

To configure Self-Managed Materialize for SASL/SCRAM authentication, update the following fields in the Materialize CR. For all Materialize CR settings, see [here](/installation/appendix-materialize-crd-field-descriptions/).

| Configuration | Description
|---------------| ------------
|`spec.authenticatorKind` | Set to `Sasl` to enable SASL/SCRAM-SHA-256 authentication for PostgreSQL connections.
|`external_login_password_mz_system` | Set to the Kubernetes Secret referenced by `spec.backendSecretName`, add the secret key `external_login_password_mz_system`. This is the password for the `mz_system` user [^1], who is the only user initially available when SASL authentication is enabled.

An example Materialize CR YAML file:

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
```bash

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

## Deploying authentication

Using the configured Materialize CR YAML file, we recommend rolling out the changes by adding the following fields:
```yaml
spec:
  ...
  requestRollout: <SOME_NEW_UUID> # Generate new UUID for rollout
  forceRollout: <SOME_NEW_UUID> # Rollout without requiring a version change
```

For more information on rollout configuration, view our [installation overview](/installation/#rollout-configuration).

> **Warning:** 
Ensure that the `authenticatorKind` field is set for any future version upgrades or rollouts of the Materialize CR. Having it undefined will reset `authenticationKind` to `None`.


## Enabling RBAC

<!-- Include not found: shared-content/enable-rbac.md -->

See [Access Control](/security/self-managed/access-control/) for details on role based authorization.