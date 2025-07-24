---
title: "Invite users"
description: "How to invite new users to a Materialize organization."
aliases:
  - /invite-users/
  - /manage/access-control/invite-users/
disable_toc: true
menu:
  main:
    parent: user-service-accounts
    weight: 5
---

As an **Organization administrator**, you can invite new users via the
Materialize Console.

## How to invite a new user

1. [Log in to the Materialize Console](https://console.materialize.com/).

1. Navigate to **Account** > **Account Settings** > **Users**.

1. Click **Invite User** and fill in the user information.

1. In the **Select Role**, select the organization role for the user:

   {{< yaml-table data="rbac/organization_roles" >}}

2. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with an invitation link.

   {{< include-md file="shared-content/invite-user-note.md" >}}

## Next steps

The organization role assigned to the user determines the user's default level
of database access. For more information on access control in Materialize, see
[role-based access control
(RBAC)](/manage/access-control/#role-based-access-control-rbac).
