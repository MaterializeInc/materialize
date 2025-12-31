---
audience: developer
canonical_url: https://materialize.com/docs/security/cloud/users-service-accounts/invite-users/
complexity: intermediate
description: How to invite new users to a Materialize organization.
doc_type: reference
keywords:
- Pending Approval
- Organization administrator
- Account Settings
- Account
- SELECT ROLE
- Invite users
- 'Note:'
- SELECT THE
product_area: Security
status: stable
title: Invite users
---

# Invite users

## Purpose
How to invite new users to a Materialize organization.

If you need to understand the syntax and options for this command, you're in the right place.


How to invite new users to a Materialize organization.



> **Note:** 
- Until the user accepts the invitation and logs in, the user is listed as
**Pending Approval**.

- When the user accepts the invitation, the user can set the user password and
log in to activate their account. The first time the user logs in, a database
role with the same name as their e-mail address is created, and the account
creation is complete.




As an **Organization administrator**, you can invite new users via the
Materialize Console.

1. [Log in to the Materialize Console](/console/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. In the **Select Role**, select the organization role for the user:

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




1. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with an invitation link.

   > **Note:** 
- Until the user accepts the invitation and logs in, the user is listed as
**Pending Approval**.

- When the user accepts the invitation, the user can set the user password and
log in to activate their account. The first time the user logs in, a database
role with the same name as their e-mail address is created, and the account
creation is complete.




## Next steps

The organization role for a user/service account determines the default level of
database access. Once the account creation is complete, you can use [role-based
access control
(RBAC)](/security/cloud/access-control/#role-based-access-control-rbac) to
control access for that account.


