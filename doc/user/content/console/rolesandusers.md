---
title: "Roles and Users"
description: "Manage roles, privileges, and users in the Materialize console"
menu:
  main:
    parent: console
    weight: 32
    identifier: console-roles-and-users
---

The **Roles and Users** screen allows organization administrators to manage
Materialize RBAC to create roles, grant privileges to the roles and grant roles
to the users.

### Roles

![Roles list](/images/console/rbac/console-roles-list.png "Roles list")

From the **Roles** page, you can:

- View all current roles in the organization and privileges granted to a role.
- **Create new roles** by clicking **Create New Role** and grant privileges to the role.
- **Edit roles** by clicking **Edit Role** and grant/revoke privileges to the role.
- **Drop roles** by clicking on the **Drop Role** option.

### Roles graph view

![Roles Graph](/images/console/rbac/console-role-graph.png "Roles graph")

View all current roles and their hierarchy in graph view.

### Role details

![Roles Details](/images/console/rbac/console-role-details.png "Role Details")

By clicking on a row in the roles list, you can:

- View all the privileges granted to a role.
- View inherited roles.
- View users granted to this role and remove the users from this role.

![Remove User](/images/console/rbac/console-roles-remove-user.png "Remove User")

### Create role

Users can create new roles by filling out the Create Role form to grant
privileges to a role or inherit from another role.

![Create Role](/images/console/rbac/console-create-role.png "Create Role")

Add privileges to the role:

![Add Privilege](/images/console/rbac/console-role-add-privilege.png "Add Privilege")

You can also see the Terraform / SQL code in real time as privileges are added.

### Edit role

Users can edit existing roles to revoke/grant more privileges.

![Edit Role](/images/console/rbac/console-edit-role.png "Edit Role")

Edit role will let you revoke or grant more privileges just like the create role
form.

![Edit Form](/images/console/rbac/console-edit-role-form.png "Edit Form")

### Drop role

![Drop Role](/images/console/rbac/console-drop-role.png "Drop Role")

If a role **owns an object**, then the objects have to be reassigned or dropped.

![Drop Role Objects](/images/console/rbac/console-role-drop-own-objects.png "Drop Role Objects")

Roles cannot be dropped if there are **privileges assigned**, so privileges must
be revoked before the role can be dropped.

![Drop Role Privileges](/images/console/rbac/console-drop-role-with-privileges.png "Drop Role Privileges")

### Users

You can see the users in your organization and add roles to any user.

![Users](/images/console/rbac/console-role-users.png "Users")

Roles for the user can be edited and added to the user.

![Add User](/images/console/rbac/console-add-role-to-user.png "Add Role User")
