---
title: "Access control (Role-based)"
description: "How to configure and manage role-based database access control (RBAC) in Materialize."
disable_list: true
aliases:
  - /manage/access-control/rbac/
  - /manage/access-control/
menu:
  main:
    parent: security
    identifier: 'access-control'
    weight: 12
---

{{< annotation type="Disambiguation" >}}
{{< include-md file="shared-content/rbac/rbac-intro-disambiguation.md" >}}

This section focuses on the database access control. For information on
organization roles, see [Users and service
accounts](/security/users-service-accounts/).
{{</ annotation >}}



## Role-based access control (RBAC)

In Materialize, role-based access control (RBAC) governs access to **database
objects** through privileges granted to [database
roles](/security/access-control/manage-roles/).

## Roles and privileges

{{% include-md file="shared-content/rbac/db-roles.md" %}}

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
desired privileges and grant these roles to the database role associated with
the user/service account email/name. Although you can grant privileges directly
to the associated roles, using separate, reusable roles is recommended for
better access management.

With privilege inheritance, you can compose more complex roles by
combining existing roles, enabling modular access control. However:

- Inheritance only applies to role privileges; role attributes and parameters
  are not inherited.
- {{% include-md file="shared-content/rbac/revoke-roles-consideration.md" %}}

## Best practices

{{% yaml-sections data="rbac/recommendations" heading-field="recommendation" heading-level=3 %}}
