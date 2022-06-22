---
title: "Manage a workspace"
description: "Add or remove users to a Materialize Cloud workspace and set up workspace security."
disable_toc: false
menu:
  main:
    parent: "cloud"
    weight:
aliases:
  - /cloud/edit-deployments
  - /cloud/administer-workspace
---

{{< cloud-notice >}}

If you're an administrative user, you can manage workspace details and security settings. To start, click on your profile icon and select **Account Settings**. Administrative options are under the **Workspace** heading.

Click the option you'd like to edit.

## Account Details

You can add or edit the following for your workspace:

* Company name
* Address
* Website
* Default timezone
* Default currency

## Security

You can modify the following settings:

Setting | Description
--------|------------
**MFA**  |  Require multi-factor authentication for all users. If this is disabled, individuals may still opt to set up multi-factor authentication in their [personal profile](../settings).
**User Lockout** | The number of incorrect password attempts allowed before the user is locked out.
**Password History** |  The number of different passwords required before the user is allowed to reuse one.

## SSO

You must configure the SSO settings before you can enable single sign-on. Currently, Materialize Cloud supports the following IDPs:

- OpenID Connect
- Google SSO
- Github SSO

## API Tokens

1. Click **Generate Token**.

1. Enter a token description and select a [role](../manage-users/#user-roles).

1. Copy the client ID and the secret key to a secure location. You will not be able to access the secret key again.

If you need to delete the token later, click the three dots menu
(**⋮**) and select **Delete API Token**. You are asked to confirm before the deletion proceeds.
