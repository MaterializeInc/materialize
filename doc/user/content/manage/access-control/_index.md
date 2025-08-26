---
title: "Access control (Role-based)"
description: "How to configure and manage role-based database access control (RBAC) in Materialize."
disable_list: true
aliases:
  - /manage/access-control/sso/
  - /manage/access-control/create-service-accounts/
  - /manage/access-control/manage-network-policies/
  - /manage/access-control/rbac/
menu:
  main:
    parent: manage
    name: Access control
    identifier: 'access-control'
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
roles](/manage/access-control/manage-roles/).

## Enabling RBAC

{{< include-md file="shared-content/rbac/enable-rbac.md" >}}

## Roles and privileges

{{% include-md file="shared-content/rbac/db-roles.md" %}}

- {{< include-md file="shared-content/rbac/create-users.md" >}}

- {{< include-md file="shared-content/rbac/create-functional-roles.md" >}}

### Managing privileges

{{% include-md file="shared-content/rbac/db-roles-managing-privileges.md" %}}

{{< annotation type="Disambiguation" >}}
{{% include-md file="shared-content/rbac/grant-vs-alter-default-privilege.md"
%}}
{{</ annotation >}}

### Initial privileges

{{< include-md file="shared-content/rbac/db-roles-initial-privileges.md" >}}

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
- {{% include-md file="shared-content/rbac/revoke-roles-consideration.md" %}}

## Best practices

{{% yaml-sections data="rbac/recommendations" heading-field="recommendation" heading-level=3 %}}
