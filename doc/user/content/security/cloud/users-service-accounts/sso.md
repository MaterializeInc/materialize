---
title: "Configure single sign-on (SSO)"
description: "Configure single sign-on (SSO) using SAML or Open ID Connect as an additional layer of account security."
aliases:
  - /manage/sso/
  - /manage/access-control/sso/
  - /manage/users-service-accounts/sso/
  - /security/users-service-accounts/sso/
menu:
  main:
    parent: user-service-accounts
    weight: 15
---

As an **administrator** of a Materialize organization, you can configure single
sign-on (SSO) as an additional layer of account security using your existing
[SAML](https://auth0.com/blog/how-saml-authentication-works/)- or
[OpenID Connect](https://auth0.com/intro-to-iam/what-is-openid-connect-oidc)-based
identity provider. This ensures that all users can securely log in to the
Materialize console using the same authentication scheme and credentials across
all systems in your organization.

{{< note >}}
Single sign-on in Materialize only supports authentication into the Materialize
console. Permissions within the database are handled separately using
[role-based access control](/security/cloud/access-control/).
{{</ note >}}

## Before you begin

* You must have an existing SAML- or OpenID Connect-based identity provider.
* Only users assigned the `OrganizationAdmin` role can view and modify SSO settings.

## Configure authentication

* [Log in to the Materialize console](/developer-tools/console/).

* Navigate to **Account** > **Account Settings** > **SSO**.

{{< tabs >}}
{{< tab "OpenID Connect" >}}

* Click **Add New** and choose the `OpenID Connect` connection type.

* Add the issuer URL, client ID, and secret key provided by your identity provider.

{{< /tab >}}
{{< tab "SAML" >}}

* Click **Add New** and choose the `SAML` connection type.

* Add the SSO endpoint and public certificate provided by your identity provider.

{{< /tab >}}
{{< /tabs >}}

* Optionally, add the SSO domain provided by your identity provider. Click **Proceed**.

* Select the organization role for the user:

  {{% include-headless "/headless/rbac-cloud/organization-roles" %}}

## Use a unique Entity ID for each organization

By default, every Materialize organization uses the same SAML service provider
(SP) Entity ID:

```
https://cloud.materialize.com/auth/saml/metadata
```

If you connect more than one Materialize organization to the same identity
provider, for example one Microsoft Entra ID tenant with a separate enterprise
application for each organization, the identity provider requires each
application to have a different Entity ID. In that case, give each organization
its own Entity ID:

```
https://cloud.materialize.com/auth/saml/metadata/<organization-id>
```

If you connect only one Materialize organization to your identity provider, you
don't need to change the Entity ID.

You can't change this setting in the Materialize console. Use the
[`set-sso-entity-id.sh`](/scripts/set-sso-entity-id.sh) script to view and
change it.

{{< note >}}
The SSO page in the Materialize console always shows the shared default Entity
ID, even after you change it. To see the Entity ID that an organization uses,
run `./set-sso-entity-id.sh list`.
{{< /note >}}

### Before you begin

* Install [`curl`](https://curl.se/) and [`jq`](https://jqlang.org/). The
  script runs on macOS and Linux.
* Have a **Personal** [app password](/developer-tools/console/create-new/#create-new-app-password-cloud-only)
  from the organization you want to change. The script determines the
  organization from the app password. The `preview` command works with any
  member's app password; all other commands need an app password for a user
  with the `OrganizationAdmin` role.
* Download the script and make it executable:

  ```shell
  curl -fsSLO https://materialize.com/docs/scripts/set-sso-entity-id.sh
  chmod +x set-sso-entity-id.sh
  ```

Repeat the following steps for each organization, using an app password from
that organization.

### Step 1. Preview the Entity ID

Enter your app password, then run `preview`. The `read` command keeps the app
password out of your shell history, and the script reads it from the
`MZ_APP_PASSWORD` environment variable for each command you run:

```shell
read -rs MZ_APP_PASSWORD && export MZ_APP_PASSWORD
./set-sso-entity-id.sh preview
```

The `preview` command shows the organization-specific Entity ID and doesn't
change anything:

```
Organization: 585dd1a4-9a72-4db9-8915-221cdf530067

Organization-specific Entity ID:
  https://cloud.materialize.com/auth/saml/metadata/585dd1a4-9a72-4db9-8915-221cdf530067

Enter this value in your identity provider:
  Microsoft Entra ID: Basic SAML Configuration -> Identifier (Entity ID)
  Okta:               SAML Settings -> Audience URI (SP Entity ID)
```

### Step 2. Configure SSO, if the organization doesn't use it yet

If the organization already uses SSO, skip to
[Step 3](#step-3-apply-the-entity-id).

1. In your identity provider, set the Entity ID of the application for this
   organization to the value from Step 1:

   * **Microsoft Entra ID:** In the enterprise application, go to **Single
     sign-on** > **Basic SAML Configuration** and set **Identifier (Entity
     ID)**.
   * **Okta:** In the application, go to **General** > **SAML Settings** and
     set **Audience URI (SP Entity ID)**.

   Leave the reply URL (assertion consumer service URL) unchanged.

1. [Configure SAML SSO](#configure-authentication) in the Materialize console
   using the SSO endpoint and public certificate from this application.

### Step 3. Apply the Entity ID

SSO login for the organization fails until Materialize and your identity
provider use the same Entity ID. If you completed Step 2, they already match
once you run `set`. If the organization already uses SSO, keep the gap to a few
seconds by changing both sides together:

1. Open the Entity ID setting of the application in your identity provider, as
   described in Step 2, and enter the value from Step 1 without saving it.

1. Run `set`:

   ```shell
   ./set-sso-entity-id.sh set
   ```

   The script shows the current and new Entity ID and waits for you to type
   `yes`.

1. Save the change in your identity provider, then type `yes`.

When the change succeeds, the script prints the Entity ID that Materialize now
uses:

```
Done. Materialize now uses this Entity ID for the organization:
  https://cloud.materialize.com/auth/saml/metadata/585dd1a4-9a72-4db9-8915-221cdf530067
```

If the organization has more than one SSO configuration, the script lists them.
Rerun the command with `--config-id <configuration-id>` to choose one. To list
the configurations and the Entity ID each one uses, run
`./set-sso-entity-id.sh list`.

{{< note >}}
SSO affects only logging in to the Materialize console. Connections that
authenticate with app passwords aren't affected.

The script signs in with your app password, not through SSO, so it keeps
working if SSO login fails. To recover, run `./set-sso-entity-id.sh reset` and
restore the previous Entity ID in your identity provider.
{{< /note >}}

### Step 4. Verify SSO login

Log in to the [Materialize console](/developer-tools/console/) through SSO to
confirm that the configuration works.

### Revert to the default Entity ID

To switch the organization back to the shared default Entity ID, run `reset`
and set the Entity ID in your identity provider back to
`https://cloud.materialize.com/auth/saml/metadata`, using the same order as
Step 3:

```shell
./set-sso-entity-id.sh reset
```

For all commands and options, run `./set-sso-entity-id.sh --help`.

## Next steps

{{% include-headless "/headless/rbac-cloud/account-creation-next-steps" %}}
