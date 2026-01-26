---
title: "Access control (Role-based)"
description: "How to configure and manage role-based database access control (RBAC) in Materialize."
disable_list: true

menu:
  main:
    parent: "security-sm"
    name: Access control
    identifier: 'access-control-sm'
    weight: 10
---

{{< note >}}
Initially, only the `mz_system` user (which has superuser/administrator
privileges) is available to manage roles.
{{</ note >}}

<a name="role-based-access-control-rbac" ></a>

## Role-based access control

In Materialize, role-based access control (RBAC) governs access to objects
through privileges granted to [database
roles](/security/self-managed/access-control/manage-roles/).

## Enabling RBAC

{{% include-headless "/headless/rbac-sm/enable-rbac" %}}

## Roles and privileges

{{% include-headless "/headless/rbac-sm/db-roles" %}}

- {{% include-headless "/headless/rbac-sm/create-users" %}}

- {{% include-headless "/headless/rbac-sm/create-functional-roles" %}}

### Managing privileges

{{% include-headless "/headless/rbac-sm/db-roles-managing-privileges" %}}

{{< annotation type="Disambiguation" >}}
{{% include-headless "/headless/rbac-sm/grant-vs-alter-default-privilege" %}}
{{</ annotation >}}

### Initial privileges

{{% include-headless "/headless/rbac-sm/db-roles-initial-privileges" %}}

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
- {{% include-headless "/headless/rbac-sm/revoke-roles-consideration" %}}

## Best practices

{{% yaml-sections data="rbac/recommendations-sm" heading-field="recommendation" heading-level=3 %}}
