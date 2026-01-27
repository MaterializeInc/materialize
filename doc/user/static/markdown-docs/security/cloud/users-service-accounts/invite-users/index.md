# Invite users
How to invite new users to a Materialize organization.
> **Note:** - Until the user accepts the invitation and logs in, the user is listed as
> **Pending Approval**.
> - When the user accepts the invitation, the user can set the user password and
> log in to activate their account. The first time the user logs in, a database
> role with the same name as their e-mail address is created, and the account
> creation is complete.

As an **Organization administrator**, you can invite new users via the
Materialize Console.

1. [Log in to the Materialize Console](/console/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. In the **Select Role**, select the organization role for the user:

   | Organization role | Description |
   | --- | --- |
   | <strong>Organization Admin</strong> | <ul> <li> <p><strong>Console access</strong>: Has access to all Materialize console features, including administrative features (e.g., invite users, create service accounts, manage billing, and organization settings).</p> </li> <li> <p><strong>Database access</strong>: Has <red><strong>superuser</strong></red> privileges in the database.</p> </li> </ul>  |
   | <strong>Organization Member</strong> | <ul> <li> <p><strong>Console access</strong>: Has no access to Materialize console administrative features.</p> </li> <li> <p><strong>Database access</strong>: Inherits role-level privileges defined by the <code>PUBLIC</code> role; may also have additional privileges via grants or default privileges. See <a href="/security/cloud/access-control/#roles-and-privileges" >Access control control</a>.</p> </li> </ul>  |


   > **Note:** - The first user for an organization is automatically assigned the
   >   **Organization Admin** role.
   > - An [Organization
   > Admin](/security/cloud/users-service-accounts/#organization-roles) has
   > <red>**superuser**</red> privileges in the database. Following the principle of
   > least privilege, only assign **Organization Admin** role to those users who
   > require superuser privileges.
   > - Users/service accounts can be granted additional database roles and privileges
   >   as needed.

1. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with an invitation link.

   > **Note:** - Until the user accepts the invitation and logs in, the user is listed as
   > **Pending Approval**.
   > - When the user accepts the invitation, the user can set the user password and
   > log in to activate their account. The first time the user logs in, a database
   > role with the same name as their e-mail address is created, and the account
   > creation is complete.

## Next steps

The organization role for a user/service account determines the default level of
database access. Once the account creation is complete, you can use [role-based
access control
(RBAC)](/security/cloud/access-control/#role-based-access-control-rbac) to
control access for that account.
