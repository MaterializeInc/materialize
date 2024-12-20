---
title: "Invite users"
description: "How to invite new users to a Materialize organization."
aliases:
  - /invite-users/
disable_toc: true
menu:
  main:
    parent: access-control
    weight: 15
---

As an **administrator** of a Materialize organization, you can invite new users
via the Materialize Console. Depending on the level of access each user should
have, you can assign them `Organization Admin` or `Organization Member`
privileges.

These privileges primarily determine a user's access level to the console, but
also have implications in the default access level to the database. More
granular permissions within the database must be handled separately using
[role-based access control
(RBAC)](/manage/access-control/#role-based-access-control-rbac).

## How to invite a new user

1. [Log in to the Materialize Console](https://console.materialize.com/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. In the **Select Role**, select *Organization Admin* or *Organization Member*
   depending on the level of console access the user needs:

    - `Organization Admin`: can perform adminstration tasks in the console, like
      inviting new users, editing account and security information, or managing
      billing. Admins have _superuser_ privileges in the database.

    - `Organization Member`: can login to the console and has restricted access
      to the database, depending  on the privileges defined via
      [role-based access control (RBAC)](/manage/access-control/#role-based-access-control-rbac).

2. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with an invitation link. When the user
   accepts the invitation, the user can create an account and login.

   Until the user accepts the invitation, the user is listed as **Pending
   Approval**.
