---
audience: developer
canonical_url: https://materialize.com/docs/security/cloud/
complexity: intermediate
description: Security for Materialize Cloud
doc_type: reference
keywords:
- 'database

  objects'
- CREATE ROLES
- CREATE NETWORK
- 'Disambiguation:'
- administrator
- CREATE A
- 'Tip:'
- Cloud
product_area: Security
status: stable
title: Cloud
---

# Cloud

## Purpose
Security for Materialize Cloud

If you need to understand the syntax and options for this command, you're in the right place.


Security for Materialize Cloud


This section covers security for Materialize Cloud.

| Guide | Description |
|-------|-------------|
| [User and service accounts](/security/cloud/users-service-accounts/) | Add user/service accounts |
| [Access control](/security/cloud/access-control/) | Reference for role-based access management (RBAC) |
| [Manage network policies](/security/cloud/manage-network-policies/) | Set up network policies |


See also:

- [Appendix: Privileges](/security/appendix/appendix-privileges/)
- [Appendix: Privileges by commands](/security/appendix/appendix-command-privileges/)
- [Appendix: Built-in roles](/security/appendix/appendix-built-in-roles/)


---

## Access control (Role-based)


> **Disambiguation:** <!-- Include not found: shared-content/rbac-cloud/rbac-intro-disambiguation.md -->

This section focuses on the database access control. For information on
organization roles, see [Users and service
accounts](../users-service-accounts/).


## Role-based access control (RBAC)

In Materialize, role-based access control (RBAC) governs access to **database
objects** through privileges granted to [database
roles](./manage-roles/).

## Roles and privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/db-... -->

### Managing privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/db-... -->

> **Disambiguation:** <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/gra... -->

### Initial privileges

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/db-... -->

<!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/pub... -->

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
desired privileges and grant these roles to the database role associated with
the user/service account email/name. Although you can grant privileges directly
to the associated roles, using separate, reusable roles is recommended for
better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- <!-- Unresolved shortcode: {{% include-md file="shared-content/rbac-cloud/rev... -->

## Best practices

<!-- Unresolved shortcode: {{% yaml-sections data="rbac/recommendations-cloud... -->


---

## Manage network policies


> **Tip:** 
We recommend using [Terraform](https://registry.terraform.io/providers/MaterializeInc/materialize/latest/docs/resources/network_policy)
to configure and manage network policies.


By default, Materialize is available on the public internet without any
network-layer access control. As an **administrator** of a Materialize
organization, you can configure network policies to restrict access to a
Materialize region using IP-based rules.

## Create a network policy

> **Note:** 
Network policies are applied **globally** (i.e., at the region level) and rules
can only be configured for **ingress traffic**.


To create a new network policy, use the [`CREATE NETWORK POLICY`](/sql/create-network-policy)
statement to provide a list of rules for allowed ingress traffic.

```sql
CREATE NETWORK POLICY office_access_policy (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32')
  )
);
```bash

## Alter a network policy

To alter an existing network policy, use the [`ALTER NETWORK POLICY`](/sql/alter-network-policy)
statement. Changes to a network policy will only affect new connections
and **will not** terminate active connections.

```mzsql
ALTER NETWORK POLICY office_access_policy SET (
  RULES (
    new_york (action='allow', direction='ingress',address='1.2.3.4/28'),
    minnesota (action='allow',direction='ingress',address='2.3.4.5/32'),
    boston (action='allow',direction='ingress',address='4.5.6.7/32')
  )
);
```bash

### Lockout prevention

To prevent lockout, the IP of the active user is validated against the policy
changes requested. This prevents users from modifying network policies in a way
that could lock them out of the system.

## Drop a network policy

To drop an existing network policy, use the [`DROP NETWORK POLICY`](/sql/drop-network-policy) statement.

```mzsql
DROP NETWORK POLICY office_access_policy;
```

To drop the pre-installed `default` network policy (or the network policy
subsequently set as default), you must first set a new system default using
the [`ALTER SYSTEM SET network_policy`](/sql/alter-system-set) statement.


---

## User and service accounts


As an administrator of a Materialize organization, you can manage the users and
apps (via service accounts) that can access your Materialize organization and
resources.

## Organization roles

During creation of a user/service account in Materialize, the account is
assigned an organization role:

<!-- Dynamic table: rbac/organization_roles - see original docs -->

> **Note:** 
- The first user for an organization is automatically assigned the
  **Organization Admin** role.

- An [Organization
Admin](/security/cloud/users-service-accounts/#organization-roles) has
<red>**superuser**</red> privileges in the database. Following the principle of
least privilege, only assign **Organization Admin** role to those users who
require superuser privileges.


- Users/service accounts can be granted additional database roles and privileges
  as needed.


## User accounts

As an **Organization admin**, you can [invite new
users](./invite-users/) via the Materialize Console. When you invite a new user,
Materialize will email the user with an invitation link.

> **Note:** 
- Until the user accepts the invitation and logs in, the user is listed as
**Pending Approval**.

- When the user accepts the invitation, the user can set the user password and
log in to activate their account. The first time the user logs in, a database
role with the same name as their e-mail address is created, and the account
creation is complete.


For instructions on inviting users to your Materialize organization, see [Invite
users](./invite-users/).

## Service accounts

> **Tip:** 

As a best practice, we recommend you use service accounts to connect external
applications and services to Materialize.


As an **Organization admin**, you can create a new service account via
the [Materialize Console](/console/) or via
[Terraform](/manage/terraform/).

> **Note:** 

- The new account creation is not finished until the first time you connect with
the account.

- The first time the account connects, a database role with the same name as the
specified service account **User** is created, and the service account creation is complete.


For instructions on creating a new service account in your Materialize
organization, see [Create service accounts](./create-service-accounts/).

## Single sign-on (SSO)

As an **Organization admin**, you can configure single sign-on (SSO) as
an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or [OpenID
Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize Console using the same authentication scheme and credentials across
all systems in your organization.

To configure SSO for your Materialize organization, follow [this step-by-step
guide](./sso/).

## See also

- [Role-based access control](/security/cloud/access-control/)
- [Manage with dbt](/manage/dbt/)
- [Manage with Terraform](/manage/terraform/)