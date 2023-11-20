---
title: "Invite users"
description: "Invite new users to a Materialize organization."
aliases:
  - /invite-users/
---

As an **administrator** of a Materialize organization, you can invite new users
via the Materialize console. Depending on the level of access each user should
have, you can assign them `Organization Admin` or `Organization Member`
privileges.

These privileges primarily determine a user's access level to the console, but
also have implications in the default access level to the database. More
granular permissions within the database must be handled separately using
[role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

## Step 1. Invite a new user

1. [Log in to the Materialize console](https://console.materialize.com/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - `Organization Admin`: can perform adminstration tasks in the console, like
      inviting new users, editing account and security information, or managing
      billing. Admins have _superuser_ privileges in the database.

    - `Organization Member`: can login to the console and has restricted access
      to the database, depending  on the privileges defined via
      [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

2. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with a verification link. The user will
   verify their email and create a password for their account. The user will
   then be able to login to the Materialize console.
