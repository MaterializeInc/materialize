---
title: "Invite users"
description: "Invite new organization members or administrators"
menu:
  main:
    parent: manage
    weight: 10
---

This page outlines the steps for inviting a new user. You can invite new users
to your Materialize organization and assign **Admin** or **Member** privileges.

{{< note >}}
You must be an Admin in your organization to invite new users.
{{</ note >}}

1. [Login to the Materialize console](https://console.materialize.com/).

1. Navigate to Account > Account Settings > Users.

1. Click **Invite User** and fill in the user information.

1. Select *Organization Admin* or *Organization Member* depending on what level of console access the user needs:

    - **Organization Admin**: Can invite new users, edit account information, edit account security information, and are super users in the database.
    - **Organization Member**: Can login to the console and have database permissions definied via [role-based access control](/manage/access-control/).

2. Click the **Invite** button at the bottom right section of the screen.

   Materialize will email the user with a verification link. The user will
   verify their email and create a password for their account. The user will
   then be able to login to the Materialize console.
