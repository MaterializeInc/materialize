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

Single sign-on (SSO) allows users to authenticate with Self-Managed Materialize
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

1. In the Okta Admin Console, go to **Applications** > **Applications** and
   click **Create App Integration**.

1. Select **OIDC - OpenID Connect** as the sign-in method and **Web
   Application** as the application type. Click **Next**.

1. Configure the application:
   - **App integration name**: Enter a name (e.g., `Materialize`).
   - **Grant type**: Ensure **Authorization Code** is selected.
   - **Sign-in redirect URIs**: Enter
     `https://<your-console-domain>/auth/callback`.
   - **Sign-out redirect URIs**: Optionally, enter
     `https://<your-console-domain>`.

1. Click **Save**.

1. On the application's **General** tab, note the **Client ID**.

1. Go to **Security** > **API** and note your **Issuer URI** from the
   authorization server you want to use (e.g.,
   `https://your-org.okta.com/oauth2/default`).

1. Go to the **Assignments** tab and assign the users or groups that should have
   access to Materialize.

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In the [Azure portal](https://portal.azure.com), go to **Microsoft Entra
   ID** > **App registrations** and click **New registration**.

1. Configure the registration:
   - **Name**: Enter a name (e.g., `Materialize`).
   - **Supported account types**: Select the appropriate option for your
     organization (typically **Accounts in this organizational directory only**).
   - **Redirect URI**: Select **Web** and enter
     `https://<your-console-domain>/auth/callback`.

1. Click **Register**.

1. On the application's **Overview** page, note the **Application (client) ID**
   and the **Directory (tenant) ID**.

1. Go to **Certificates & secrets** > **New client secret**. Add a description
   and expiration, then click **Add**. Note the secret **Value**.

1. Construct your issuer URL using your tenant ID:

   ```
   https://login.microsoftonline.com/<tenant-id>/v2.0
   ```

1. Go to **Enterprise applications** > select your application > **Users and
   groups** and assign the users or groups that should have access to
   Materialize.

{{< /tab >}}
{{< tab "Generic OIDC" >}}

1. In your identity provider, create a new OIDC client application with the
   **Authorization Code** grant type.

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
  environmentdImageRef: materialize/environmentd:vX.Y.Z
  backendSecretName: materialize-backend
  authenticatorKind: Oidc
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
| `oidc_audience` | A JSON array of client IDs for audience validation (e.g., `["your-client-id"]`). Materialize checks that the JWT's `aud` claim contains at least one of these values. | No | `[]` |
| `oidc_authentication_claim` | The JWT claim to use as the Materialize username. | No | `sub` |
| `console_oidc_client_id` | The OIDC client ID used by the web console for the authorization code flow. | For console login | Empty |
| `console_oidc_scopes` | Space-separated OIDC scopes requested by the web console when obtaining an ID token (e.g., `openid`). | For console login | Empty |

{{< warning >}}
When `oidc_audience` is empty, audience validation is skipped. This means
**any** valid token from the same identity provider can authenticate to
Materialize, including tokens issued for other applications. Always set
`oidc_audience` in production environments.
{{</ warning >}}

### Configure via ConfigMap

Create a ConfigMap with your OIDC parameters and reference it in the Materialize
CR's `spec.systemParameterConfigmapName` field:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  system-params.json: |
    {
      "oidc_issuer": "https://your-org.okta.com/oauth2/default",
      "oidc_audience": "[\"YOUR_CLIENT_ID\"]",
      "oidc_authentication_claim": "email",
      "console_oidc_client_id": "YOUR_CLIENT_ID",
      "console_oidc_scopes": "openid"
    }
```

Then add the ConfigMap name to your Materialize CR:

```yaml
spec:
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

1. To verify that your role was created, navigate to the [SQL shell](/console/sql-shell/) and run:

   ```mzsql
   SELECT name, autoprovisionsource FROM mz_roles WHERE autoprovisionsource = 'oidc';
   ```

   You should see your username (derived from the configured authentication
   claim) listed with an `autoprovisionsource` of `oidc`.

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

When a user authenticates via OIDC and no matching Materialize role exists, the
role is **automatically created**. The role name is derived from the JWT claim
specified by `oidc_authentication_claim` (default: `sub`).

Auto-provisioned roles:

- Have default privileges
- Must be granted additional privileges through RBAC.

{{< important >}}
Materialize does not support mapping IdP groups to database roles. After a user
is auto-provisioned, configure their privileges separately using
[RBAC](/security/self-managed/access-control/manage-roles/).
{{</ important >}}

## Service accounts

For machine-to-machine access, you can use service accounts that authenticate
via OIDC tokens. There are two approaches depending on what your identity
provider supports.

### Resource Owner Password flow

Use this approach when you need a service account that authenticates with a
username and password to obtain an ID token.

{{< tabs >}}
{{< tab "Okta" >}}

1. In Okta, create a new user to serve as the service account (e.g.,
   `svc-materialize@your-org.com`).

1. Assign the service account user to your Materialize application.

1. On the application's **General** tab, under **Grant type**, enable **Resource
   Owner Password**.

1. Ensure the sign-on policy for this application only requires password
   authentication (not MFA) for the service account.

1. Fetch an ID token:

   ```shell
   curl -X POST https://your-org.okta.com/oauth2/default/v1/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -H "Accept: application/json" \
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
{{< tab "Microsoft Entra ID" >}}

1. In Microsoft Entra ID, create a new user to serve as the service account.

1. Assign the user to your Materialize application under **Enterprise
   applications** > **Users and groups**.

1. Ensure the application allows the Resource Owner Password Credentials flow.
   In the app registration, go to **Authentication** and set **Allow public
   client flows** to **Yes**.

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
Materialize role name, the same as for user tokens.

{{< tabs >}}
{{< tab "Okta" >}}

1. In Okta, create a new application (or reuse an existing one) and note the
   **Client ID** and **Client Secret**.

1. Add the new client ID to the `oidc_audience` system parameter:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_CONSOLE_CLIENT_ID", "YOUR_SERVICE_CLIENT_ID"]';
   ```

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

   The `<service-account-name>` must match the value of the authentication
   claim in the access token.

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In Microsoft Entra ID, go to your app registration and note the **Application
   (client) ID**. Create a new client secret under **Certificates & secrets**.

1. Add the new client ID to the `oidc_audience` system parameter:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_CONSOLE_CLIENT_ID", "YOUR_SERVICE_CLIENT_ID"]';
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

1. In your identity provider, create or reuse a client and note the **Client
   ID** and **Client Secret**.

1. Add the new client ID to the `oidc_audience` system parameter:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_CONSOLE_CLIENT_ID", "YOUR_SERVICE_CLIENT_ID"]';
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

## Migrating from password authentication to OIDC

If you have an existing Materialize deployment using password authentication, you
can migrate to OIDC without losing access to existing roles and their owned
objects. The key is to configure `oidc_authentication_claim` so that the claim
value in the JWT matches the existing SQL username for each user.

### Step 1. Review existing roles

List the current roles in your Materialize deployment:

```mzsql
SELECT name FROM mz_roles WHERE name NOT LIKE 'mz_%';
```

Note these usernames — you will need to ensure they match the OIDC token claim
values.

### Step 2. Choose the right authentication claim

The `oidc_authentication_claim` parameter determines which JWT claim maps to the
Materialize role name. When a user authenticates via OIDC, Materialize looks up
the value of this claim and uses it as the username.

To preserve access to existing roles, set `oidc_authentication_claim` to a claim
whose value matches the existing SQL username for each user. Common options:

| Claim | Example value | When to use |
|-------|---------------|-------------|
| `email` | `alice@your-org.com` | If existing roles are named after email addresses |
| `preferred_username` | `alice` | If existing roles use short usernames |
| `sub` (default) | `auth0\|abc123` | If existing roles match the IdP's subject identifiers |

For example, if your existing roles are email addresses like
`alice@your-org.com`, set:

```mzsql
ALTER SYSTEM SET oidc_authentication_claim = 'email';
```

When `alice@your-org.com` signs in via OIDC, Materialize resolves the `email`
claim from the JWT and matches it to the existing `alice@your-org.com` role.
The user retains all privileges and object ownership from the original role.

{{< note >}}
If the claim value does not match an existing role, Materialize auto-provisions
a new role. To avoid orphaned roles, verify the claim mapping before switching
users over. Using [jwt.io](https://jwt.io), you can confirm the claim value matches the existing SQL username.
{{</ note >}}


## Auditing

To view which roles were auto-provisioned via OIDC, query the
`autoprovisionsource` column in `mz_roles`:

```mzsql
SELECT name, autoprovisionsource FROM mz_roles;
```

Roles created through OIDC authentication will have `autoprovisionsource` set to
`oidc`.

## Troubleshooting

| Symptom | Possible cause | Resolution |
|---------|---------------|------------|
| Console does not show SSO login option | `console_oidc_client_id` nd `console_oidc_scopes` is not set | Set `console_oidc_client_id` to your OIDC client ID |
| SSO login redirects fail | Incorrect redirect URI in IdP | Verify the redirect URI is set to `https://<your-console-domain>/auth/callback` |
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

## See also

- [Authentication](/security/self-managed/authentication/)
- [Access control](/security/self-managed/access-control/)
- [Manage roles](/security/self-managed/access-control/manage-roles/)
- [System parameters configuration](/self-managed-deployments/configuration-system-parameters/)
- [Materialize CRD Field Descriptions](/installation/appendix-materialize-crd-field-descriptions/)
