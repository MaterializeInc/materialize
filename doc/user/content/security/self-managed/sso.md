---
title: "Single sign-on (SSO)"
description: "Configure OIDC-based single sign-on (SSO) for Self-Managed Materialize."
menu:
  main:
    parent: "security-sm"
    name: "Single sign-on (SSO)"
    identifier: "sso-sm"
    weight: 9
---

{{< public-preview />}}

Single sign-on (SSO) allows users to authenticate to Self-Managed Materialize
using their organization's identity provider (IdP) via
[OpenID Connect (OIDC)](https://openid.net/developers/how-connect-works/).
Instead of managing passwords directly in Materialize, users sign in through
their IdP (e.g., Okta, Microsoft Entra ID) and receive a JWT token that
Materialize validates.

{{< note >}}
SSO handles **authentication** only. Permissions within the database are managed
separately using [role-based access control (RBAC)](/security/self-managed/access-control/).
{{</ note >}}

## Before you begin

Make sure you have:

- An OIDC-capable identity provider (e.g., Okta, Microsoft Entra ID, or any
  provider that supports OpenID Connect).
- Admin access to your Kubernetes cluster where Materialize is deployed.

## Step 1. Configure your identity provider

Create an OIDC application in your identity provider and note the **issuer URL**
and **client ID**. You will need these to configure Materialize.

Set the redirect URI to:

```
https://<your-console-domain>/auth/callback
```

Replace `<your-console-domain>` with the domain where your Materialize console
is accessible.

{{< tabs >}}
{{< tab "Okta" >}}

The following steps create the OIDC application for the **Materialize console**
(browser-based login). If you also need service accounts, you will create
additional applications in the [Service accounts](#service-accounts) section.

1. In the Okta Admin Console, go to **Applications** > **Applications** and
   click **Create App Integration**.

1. Select **OIDC - OpenID Connect** as the sign-in method and **Single-Page
   Application** as the application type. Click **Next**.

1. Configure the application:
   - **App integration name**: Enter a name (e.g., `Materialize`).
   - **Grant type**: Ensure **Authorization Code** is selected (PKCE is used
     automatically for single-page applications).
   - **Sign-in redirect URIs**: Enter
     `https://<your-console-domain>/auth/callback`.
   - **Sign-out redirect URIs**: Optionally, enter
     `https://<your-console-domain>/account/login`.

1. Click **Save**.

1. On the application's **General** tab, note the **Client ID**.

   *Single-page applications use PKCE instead of a client secret. You do not
   need a client secret for the console application.*

1. Go to **Security** > **API** and note your **Issuer URI** from the
   authorization server you want to use (e.g.,
   `https://your-org.okta.com/oauth2/default`).

1. Go to the **Assignments** tab and assign the users or groups that should have
   access to Materialize.

1. Configure the authorization server. In the Okta Admin Console, go to
   **Security** > **API** and click on the authorization server you want to
   use (e.g., **default**).

   1. On the **Settings** tab, note the **Issuer** URI. This is the value you
      will use for the `oidc_issuer` system parameter.

   1. Go to the **Scopes** tab and ensure the `openid` and `email` scopes
      exist (they are present by default).

   1. Go to the **Access Policies** tab. You need at least one policy with a
      rule that allows the grant types you plan to use:

      - **Authorization Code**: Required for the console login.
      - **Resource Owner Password**: Required for the
        [ROPC service account flow](#resource-owner-password-flow).
      - **Client Credentials**: Required for the
        [Client Credentials service account flow](#client-credentials-flow).

      To add or edit a rule:

      1. Click **Add New Access Policy** (or select an existing policy).
      1. Click **Add Rule** within the policy.
      1. Under **Grant type is**, check the grant types you need.
      1. Under **Assigned to**, select the clients (applications) this rule
         applies to.
      1. Click **Create Rule**.

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In the [Azure portal](https://portal.azure.com), go to **Microsoft Entra
   ID** > **App registrations** and click **New registration**.

1. Configure the registration:
   - **Name**: Enter a name (e.g., `Materialize`).
   - **Supported account types**: Select the appropriate option for your
     organization (typically **Accounts in this organizational directory only**).
   - **Redirect URI**: Select **Single-page application (SPA)** and enter
     `https://<your-console-domain>/auth/callback`.

1. Click **Register**.

1. If you plan to use the [Resource Owner Password
   flow](#resource-owner-password-flow) for service accounts, go to
   **Authentication** and set **Allow public client flows** to **Yes**.

1. On the application's **Overview** page, note the **Application (client) ID**
   and the **Directory (tenant) ID**.

1. Go to **Certificates & secrets** > **New client secret**. Add a description
   and expiration, then click **Add**. Note the secret **Value**.

   *A client secret is not required for the console login (which uses
   authorization code with PKCE), but is needed if you plan to use the
   [Client Credentials flow](#client-credentials-flow) for service accounts.*

1. Construct your issuer URL using your tenant ID:

   ```
   https://login.microsoftonline.com/<tenant-id>/v2.0
   ```

1. Go to **Enterprise applications** > select your application > **Users and
   groups** and assign the users or groups that should have access to
   Materialize.

{{< /tab >}}
{{< tab "Generic OIDC" >}}

1. In your identity provider, create a new OIDC **public** client application
   (single-page application type) with the **Authorization Code** grant type
   and **PKCE** support.

1. Set the redirect URI to `https://<your-console-domain>/auth/callback`.

1. Note the **client ID** and **issuer URL** provided by your identity provider.
   The issuer URL is typically the base URL of your identity provider's OIDC
   discovery endpoint (without `/.well-known/openid-configuration`).

1. Ensure the `openid` scope is available.

1. Assign users or groups that should have access to Materialize.

{{< /tab >}}
{{< /tabs >}}

## Step 2. Enable OIDC authentication

To configure Self-Managed Materialize for OIDC authentication, update the
following fields:

| Resource | Configuration | Description
|----------|---------------| ------------
| Materialize CR | `spec.authenticatorKind` | Set to `Oidc` to enable OIDC authentication.
| Kubernetes Secret | `external_login_password_mz_system` | Specify the password for the `mz_system` user. Add `external_login_password_mz_system` to the Kubernetes Secret referenced in the Materialize CR's `spec.backendSecretName` field. The `mz_system` user **always** authenticates with a password. This user is required by the Materialize Operator for upgrades and serves as an emergency administrative account.

The following example Kubernetes manifest includes configuration for OIDC
authentication:

```hc {hl_lines="15 25"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: "..."
  persist_backend_url: "..."
  license_key: "..."
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:v26.18.0 # Use v26.18.0 or later
  backendSecretName: materialize-backend
  authenticatorKind: Oidc
  requestRollout: 00000000-0000-0000-0000-000000000003 # Switching to Oidc requires a rollout
```

Apply the updated manifest to your Kubernetes cluster. See
[Upgrading](/self-managed-deployments/upgrading/#rollout-configuration) for
details on rollout configuration.

{{% include-headless
"/headless/self-managed-deployments/enabled-auth-setting-warning" %}}

## Step 3. Configure OIDC system parameters

Configure the OIDC system parameters to connect Materialize to your identity
provider. You can use either a
[ConfigMap](/self-managed-deployments/configuration-system-parameters/#configure-system-parameters-via-configmap)
or SQL commands, but it is strongly recommended to use a ConfigMap.

### OIDC system parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `oidc_issuer` | The OIDC issuer URL (e.g., `https://your-org.okta.com/oauth2/default`). Materialize uses this to discover the JWKS endpoint for token validation. | Yes | None |
| `oidc_audience` | A JSON array of expected audience values for token validation (e.g., `["your-client-id"]`). Use the **client ID from [Step 1](#step-1-configure-your-identity-provider)**. Materialize checks that the JWT's `aud` claim contains at least one of these values. **By default, this is empty, and audience validation is skipped.**| No | `[]` |
| `oidc_authentication_claim` | The JWT claim to use as the Materialize username. | No | `sub` |
 | `oidc_audience` | A JSON array of expected audience values for token validation (e.g., `["your-client-id"]`). Use the **client ID from [Step 1](#step-1-configure-your-identity-provider)**. Materialize checks that the JWT's `aud` claim contains at least one of these values. **By default, this is empty, and audience validation is skipped.** | No | `[]` |
| `oidc_authentication_claim` | The JWT claim to use as the Materialize username. For ID tokens (human users), common values are `email` or `preferred_username`. For access tokens from the [Client Credentials flow](#client-credentials-flow), ensure this claim exists in the token. | No | `sub` |
| `console_oidc_client_id` | The OIDC client ID used by the web console for the authorization code flow. | For console login | Empty |
| `console_oidc_scopes` | Space-separated OIDC scopes requested by the web console when obtaining a token. Scopes control which claims are included in the token. The `openid` scope is required to obtain an ID token. Add `email` to include the `email` claim, or `profile` to include name claims. If `oidc_authentication_claim` references a claim like `email`, you must request the corresponding scope here. | For console login | Empty |

{{< warning >}}
When `oidc_audience` is empty, audience validation is skipped. This means
**any** valid token from the same identity provider can authenticate to
Materialize, including tokens issued for other applications. **Always set
`oidc_audience` in production environments.**
{{</ warning >}}

### Configure via ConfigMap

Create a ConfigMap with your OIDC parameters and reference it in the Materialize
CR's `spec.systemParameterConfigmapName` field. At this point, your manifest should look like:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "oidc_issuer": "YOUR_OIDC_ISSUER",
      "oidc_audience": "[\"YOUR_CLIENT_ID\"]",
      "oidc_authentication_claim": "email",
      "console_oidc_client_id": "YOUR_CLIENT_ID",
      "console_oidc_scopes": "openid email"
    }
---
apiVersion: v1
kind: Secret
metadata:
  name: materialize-backend
  namespace: materialize-environment
stringData:
  metadata_backend_url: ...
  persist_backend_url: ...
  license_key: ...
  external_login_password_mz_system: "enter_mz_system_password"
---
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  authenticatorKind: Oidc
  backendSecretName: materialize-backend
  requestRollout: 00000000-0000-0000-0000-000000000003 # Switching to Oidc requires a rollout
  systemParameterConfigmapName: mz-system-params
```

### Configure via SQL

Alternatively, connect as `mz_system` and set the parameters using
`ALTER SYSTEM SET`:

```mzsql
ALTER SYSTEM SET oidc_issuer = 'https://your-org.okta.com/oauth2/default';
ALTER SYSTEM SET oidc_audience = '["YOUR_CLIENT_ID"]';
ALTER SYSTEM SET oidc_authentication_claim = 'email';
ALTER SYSTEM SET console_oidc_client_id = 'YOUR_CLIENT_ID';
ALTER SYSTEM SET console_oidc_scopes = 'openid';
```

## Step 4. Verify the configuration

1. Navigate to your Materialize console. You should see an option to sign in
   via your identity provider via "Use single sign-on".

   ![Materialize console login screen showing the SSO sign-in
   option](/images/console/console-self-managed-sso.png "Materialize console login screen
   with SSO option")

1. Sign in through your IdP. After successful authentication, you are redirected
   back to the Materialize console.

1. To confirm which SQL role you've signed in as via SSO, open the [SQL Shell](/console/sql-shell/) in the Materialize console. In the welcome message, you should see the role's name labelled under "User". This is derived from the `oidc_authentication_claim` claim in your identity token:

![Materialize console Shell](/images/console/console.png "Materialize console Shell")

## Connecting via SQL clients

To connect to Materialize using a SQL client like `psql`, you need an OIDC
token and the `oidc_auth_enabled=true` connection option.

{{< important >}}
The `oidc_auth_enabled=true` connection option is **required** for OIDC
authentication over the PostgreSQL wire protocol. Without it, Materialize
defaults to password authentication.
{{</ important >}}

<!-- TODO (SangJunBak): Uncomment once v26.19 is released.
### Get your token from the console

The easiest way to get an ID token is through the Materialize console:

1. Sign in to the console via SSO.
1. Click **Connect** in the navigation bar. The connection dialog displays your
   ID token along with connection instructions.
1. Copy the provided ID token.

![Materialize console Connect dialog showing the ID
token](/images/sso-connect-token.png "Materialize console Connect dialog
with ID token")
-->

### Connect with psql

Use the ID token as your password:

```shell
PGOPTIONS='-c oidc_auth_enabled=true' \
PGPASSWORD="<your-id-token>" \
psql -h <materialize-host> -p 6875 -U <username> materialize
```

Replace `<username>` with the value of the authentication claim in your JWT
(e.g., your email address if `oidc_authentication_claim` is set to `email`).

{{< note >}}
Materialize validates the token at **connection time only**. Once a connection
is established, it persists until disconnected, regardless of token expiry.
{{</ note >}}

### Get a token using CLI tools

You can also obtain an ID token outside the console using OIDC CLI tools such as
[`oidc-agent`](https://indigo-dc.gitbook.io/oidc-agent) or
[`oauth2c`](https://github.com/cloudentity/oauth2c).

## Auto-provisioned roles

Each user who authenticates via OIDC is associated with a single database role
in Materialize. When a user signs in and no matching role exists, Materialize
**automatically creates** a role for them. The role name is the value of the JWT
claim configured in `oidc_authentication_claim`.

For example, if `oidc_authentication_claim` is set to `email` and a user signs
in with the following JWT:

```json
{
  "sub": "auth0|abc123",
  "email": "alice@your-org.com",
  "name": "Alice",
  "iat": 1516239022
}
```

Materialize creates a role named `alice@your-org.com`.

Auto-provisioned roles:

- Have default privileges only.
- Must be granted additional privileges through
  [RBAC](/security/self-managed/access-control/manage-roles/).
- Are not automatically removed when the user is removed from the IdP. See
  [De-provisioning users](#de-provisioning-users) for cleanup instructions.

{{< note >}}
If the claim value does not match an existing role, Materialize auto-provisions
a new role. To avoid orphaned roles, verify the claim mapping before switching
users over. Using [jwt.io](https://jwt.io), you can confirm the claim value
matches the existing SQL username.
{{</ note >}}

{{< important >}}
Materialize does not support mapping IdP groups to database roles. After a user
is auto-provisioned, configure their privileges separately using
[RBAC](/security/self-managed/access-control/manage-roles/).
{{</ important >}}

### Auditing auto-provisioned roles

To view which roles were auto-provisioned via OIDC, query `mz_audit_events`:

```mzsql
SELECT details
FROM mz_audit_events
WHERE event_type = 'create' AND object_type = 'role' AND details ->> 'auto_provision_source' = 'oidc'
ORDER BY occurred_at DESC;
```

Roles created through OIDC authentication will have `auto_provision_source` set to
`oidc`.

## Service accounts

For machine-to-machine access, you can use service accounts that authenticate
via OIDC tokens. There are two approaches depending on what your identity
provider supports.

### Resource Owner Password flow

Use this approach when you need a service account that authenticates with a
username and password to obtain an ID token.

{{< tabs >}}
{{< tab "Okta" >}}

1. In the Okta Admin Console, go to **Applications** > **Applications** and
   click **Create App Integration**.

1. Select **OIDC - OpenID Connect** as the sign-in method and **Native
   Application** as the application type. Click **Next**.

1. Configure the application:
   - **App integration name**: Enter a name (e.g., `Materialize ROPC`).
   - **Grant type**: Enable **Resource Owner Password**.

1. Click **Save** and note the **Client ID** and **Client Secret**.

1. Go to the **Assignments** tab and assign the service account user.

1. Ensure your authorization server's access policy includes a rule that
   allows the **Resource Owner Password** grant type for this application.
   See the authorization server setup in
   [Step 1](#step-1-configure-your-identity-provider).

1. In Okta, create a new user to serve as the service account (e.g.,
   `svc-materialize@your-org.com`).

   *The Resource Owner Password flow does not support MFA in Okta. The service
   account user must not have MFA enabled.*

1. Fetch an ID token:

   ```shell
   curl -X POST https://your-org.okta.com/oauth2/default/v1/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -H "Accept: application/json" \
     --data-urlencode "grant_type=password" \
     --data-urlencode "username=svc-materialize@your-org.com" \
     --data-urlencode "password=YOUR_SERVICE_ACCOUNT_PASSWORD" \
     --data-urlencode "scope=openid email" \
     --data-urlencode "client_id=YOUR_ROPC_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_ROPC_CLIENT_SECRET"
   ```

1. Extract the `id_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<id-token>" \
   psql -h <materialize-host> -p 6875 -U svc-materialize@your-org.com materialize
   ```

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In Microsoft Entra ID, create a new user to serve as the service account.

1. Assign the user to your Materialize application under **Enterprise
   applications** > **Users and groups**.

1. Ensure **Allow public client flows** is set to **Yes** in your app
   registration's **Authentication** settings. If you followed
   [Step 1](#step-1-configure-your-identity-provider), this is already
   configured.

1. Fetch an ID token:

   ```shell
   curl -X POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=password" \
     --data-urlencode "username=svc-materialize@your-org.com" \
     --data-urlencode "password=YOUR_SERVICE_ACCOUNT_PASSWORD" \
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_CLIENT_ID"
   ```

1. Extract the `id_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<id-token>" \
   psql -h <materialize-host> -p 6875 -U svc-materialize@your-org.com materialize
   ```

{{< /tab >}}
{{< tab "Generic OIDC" >}}

1. Create a service account user in your identity provider.

1. Assign the service account to your Materialize application.

1. Enable the Resource Owner Password Credentials grant for your application.

1. Fetch an ID token from your IdP's token endpoint:

   ```shell
   curl -X POST https://your-idp.com/oauth2/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=password" \
     --data-urlencode "username=svc-materialize@your-org.com" \
     --data-urlencode "password=YOUR_SERVICE_ACCOUNT_PASSWORD" \
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_CLIENT_SECRET"
   ```

1. Extract the `id_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<id-token>" \
   psql -h <materialize-host> -p 6875 -U svc-materialize@your-org.com materialize
   ```

{{< /tab >}}
{{< /tabs >}}

### Client Credentials flow

Use this approach to treat an IdP client as a service account. This is useful
for automated systems that do not have a user context.

The `oidc_authentication_claim` setting determines which token claim maps to the
Materialize role name. Note that Client Credentials tokens are **access tokens**
(not ID tokens) and may not include the same claims as user tokens. In
particular, the default `sub` claim contains the client ID, not a
human-readable name.

{{< tabs >}}
{{< tab "Okta" >}}

1. In the Okta Admin Console, go to **Applications** > **Applications** and
   click **Create App Integration**.

1. Select **OIDC - OpenID Connect** as the sign-in method and **Web
   Application** as the application type. Click **Next**.

1. Configure the application:
   - **App integration name**: Enter a name (e.g., `Materialize Service Account 1`).
   - **Grant type**: Enable **Client Credentials** (deselect other grant
     types).

1. Click **Save** and note the **Client ID** and **Client Secret**.

1. Ensure your authorization server's access policy includes a rule that
   allows the **Client Credentials** grant type for this application.
   See the authorization server setup in
   [Step 1](#step-1-configure-your-identity-provider).

1. Ensure `oidc_audience` includes the expected audience value for tokens
   from your authorization server. In Okta, the `aud` claim is set to the
   authorization server's audience (configured in **Security** > **API** >
   your auth server > **Settings**), not the client ID. For the default
   authorization server, this is typically `api://default`:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["api://default"]';
   ```

1. **Configure a custom claim for the service account identity.**

   The `oidc_authentication_claim` setting is global — it applies to both
   human users (ID tokens) and service accounts (access tokens). If set to
   `email`, human users get readable role names (e.g., `alice@your-org.com`),
   but Client Credentials access tokens do not include an `email` claim by
   default. If set to `sub`, Client Credentials tokens work, but human users
   lose email-based role names and get opaque subject IDs instead.

   To solve this, create a custom claim (e.g., `sql_username`) in your
   authorization server that maps to `user.email` for ID tokens and to a
   configured value for access tokens:

   1. In the Okta Admin Console, go to **Security** > **API** and select your
      authorization server.

   1. Go to the **Claims** tab and click **Add Claim**.

   1. Configure the claim for **ID tokens** (human users):
      - **Name**: `sql_username`
      - **Include in token type**: **ID Token** (always).
      - **Value type**: **Expression**.
      - **Value**: `appuser.email`
      - **Include in**: **Any scope**.

   1. Click **Create**, then click **Add Claim** again.

   1. Configure the claim for **access tokens** (service accounts):
      - **Name**: `sql_username`
      - **Include in token type**: **Access Token** (always).
      - **Value type**: **Expression**.
      - **Value**: `app.sub`
      - **Include in**: **Any scope**.

   1. Click **Create**.

   1. Set the authentication claim in Materialize:

      ```mzsql
      ALTER SYSTEM SET oidc_authentication_claim = 'sql_username';
      ```

   *If you have multiple service accounts using Client Credentials, each needs
   its own Okta application.

1. Fetch an access token:

   ```shell
   curl -X POST https://your-org.okta.com/oauth2/default/v1/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -H "Accept: application/json" \
     --data-urlencode "grant_type=client_credentials" \
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_SERVICE_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_SERVICE_CLIENT_SECRET"
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<access-token>" \
   psql -h <materialize-host> -p 6875 -U <service-account-name> materialize
   ```

   The `<service-account-name>` must match the value of the `sql_username`
   claim in the access token (e.g., `svc-my-service`).

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In the [Azure portal](https://portal.azure.com), go to **Microsoft Entra
   ID** > **App registrations** and click **New registration**.

1. Configure the registration:
   - **Name**: Enter a name (e.g., `Materialize Service Account`).
   - **Supported account types**: Select the appropriate option for your
     organization.

1. Click **Register**.

1. On the application's **Overview** page, note the **Application (client) ID**.

1. Go to **Certificates & secrets** > **New client secret**. Add a description
   and expiration, then click **Add**. Note the secret **Value**.

1. Ensure `oidc_audience` includes the expected audience value for Client
   Credentials tokens. In Entra, the `aud` claim is determined by the `scope`
   parameter in the token request. When using `YOUR_SERVICE_CLIENT_ID/.default`,
   the audience is the service client ID:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_SERVICE_CLIENT_ID"]';
   ```

1. Fetch an access token:

   ```shell
   curl -X POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=client_credentials" \
     --data-urlencode "scope=YOUR_SERVICE_CLIENT_ID/.default" \
     --data-urlencode "client_id=YOUR_SERVICE_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_SERVICE_CLIENT_SECRET"
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<access-token>" \
   psql -h <materialize-host> -p 6875 -U <service-account-name> materialize
   ```

   The `<service-account-name>` must match the value of the authentication
   claim in the access token.

{{< /tab >}}
{{< tab "Generic OIDC" >}}

1. In your identity provider, create a new OIDC client application with the
   **Client Credentials** grant type.

1. Note the **Client ID** and **Client Secret**.

1. Ensure `oidc_audience` includes the expected audience value for Client
   Credentials tokens. Check the `aud` claim in the token issued by your IdP
   to determine the correct value:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_AUDIENCE_VALUE"]';
   ```

1. Fetch an access token from your IdP's token endpoint:

   ```shell
   curl -X POST https://your-idp.com/oauth2/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=client_credentials" \
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_SERVICE_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_SERVICE_CLIENT_SECRET"
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
   PGOPTIONS='-c oidc_auth_enabled=true' \
   PGPASSWORD="<access-token>" \
   psql -h <materialize-host> -p 6875 -U <service-account-name> materialize
   ```

   The `<service-account-name>` must match the value of the authentication
   claim in the access token.

{{< /tab >}}
{{< /tabs >}}

## De-provisioning users

When a user is removed from the identity provider, they can no longer
authenticate to Materialize because their JWT tokens will no longer be valid.
However, the corresponding Materialize role is **not automatically deleted**.
This is intentional to avoid disrupting ownership of database objects.

To remove the role after de-provisioning:

```mzsql
-- Reassign owned objects if needed
REASSIGN OWNED BY <username> TO <new-owner>;
-- Then drop the role
DROP ROLE <username>;
```

## Troubleshooting

| Symptom | Possible cause | Resolution |
|---------|---------------|------------|
| Console does not show SSO login option | `console_oidc_client_id` and `console_oidc_scopes` are not set | Set `console_oidc_client_id` to your OIDC client ID |
| SSO login redirects fail | Incorrect IdP configuration | Verify the redirect URI is set to `https://<your-console-domain>/auth/callback` and the IdP application type is set as a Single Page Application |
| SSO login redirects to login page | Materialize database is rejecting the token | Verify that the token generated by your IdP includes the required claims.
| environmentd fails to upgrade | external_login_password_mz_system not set | Ensure the external_login_password_mz_system is configured |
| "Invalid token" error on psql connection | Wrong or expired JWT token | Obtain a fresh token; verify `oidc_issuer` matches the token's `iss` claim |
| "Audience validation failed" | Client ID not in `oidc_audience` | Add the client ID to `oidc_audience`: `ALTER SYSTEM SET oidc_audience = '["your-client-id"]'` |
| User gets wrong role name | `oidc_authentication_claim` set to wrong claim | Verify the claim name and check the JWT contents (e.g., using [jwt.io](https://jwt.io)) |
| psql falls back to password auth | Missing `oidc_auth_enabled=true` | Add `PGOPTIONS='-c oidc_auth_enabled=true'` to your connection command |

To inspect the current OIDC configuration, login as `mz_system` and run the following SQL:

```mzsql
SHOW oidc_issuer;
SHOW oidc_audience;
SHOW oidc_authentication_claim;
SHOW console_oidc_client_id;
SHOW console_oidc_scopes;
```

## FAQ

### What happens during a blue/green deployment?

OIDC configuration (system parameters) and auto-provisioned roles are persisted
in the Materialize catalog. Blue/green deployments do not affect SSO
configuration or user roles. No additional action is required.

### What happens if I tear down my Materialize environment?

Role data and OIDC configuration are stored in the Materialize catalog, which is
persisted in your configured object storage (e.g., S3). If you delete the
Materialize instance in Kubernetes and re-apply the Materialize CR, the instance
rehydrates from the persisted catalog, recovering all roles and configuration.

If the underlying object storage is also deleted, the catalog and all role data
are lost. Use your cloud provider's disaster recovery policies to protect against this scenario.

## See also

- [Migrate to SSO](/security/self-managed/sso-migration/)
- [Authentication](/security/self-managed/authentication/)
- [Access control](/security/self-managed/access-control/)
- [Manage roles](/security/self-managed/access-control/manage-roles/)
- [System parameters configuration](/self-managed-deployments/configuration-system-parameters/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
