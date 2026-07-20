---
title: "Single sign-on (SSO)"
description: "Configure OIDC-based single sign-on (SSO) for Self-Managed Materialize."
menu:
  main:
    parent: "authentication-sm"
    name: "Single sign-on (SSO)"
    identifier: "sso-sm"
    weight: 1
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

{{< note >}}
**Current limitations:**

- **SAML** authentication is not supported. Materialize supports OIDC only.
- **SCIM** is not supported. Users are auto-provisioned on first SSO login (see [Auto-provisioning roles](#auto-provisioning-roles)), but removing a user from your IdP does not automatically deprovision their Materialize role.
- **IdP group-to-role mapping** is not supported. Each user maps 1:1 to a single Materialize role via a JWT claim; privileges and group-based assignment are managed via [RBAC](/security/self-managed/access-control/).
{{</ note >}}

## Before you begin

Make sure you have:

- An OIDC-capable identity provider (e.g., Okta, Microsoft Entra ID, or any
  provider that supports OpenID Connect).
- Admin access to your Kubernetes cluster where Materialize is deployed.

## Step 1. Configure your identity provider

{{< note >}}
You will use the following values from your IdP configuration to [configure
OIDC system parameters for Materialize](#step-3-configure-oidc-system-parameters):

- The OIDC **issuer URL**
- The **client ID** for the console application
- If using service accounts, the client ID, client secret, and expected
  audience for each service-account application
{{</ note >}}

{{< tabs >}}
{{< tab "Okta" >}}

The following steps create the OIDC application for the **Materialize Console**
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
     `https://<your-console-domain>/auth/callback`. If you want to use the
     [CLI token flow](#get-a-token-using-cli-tools), also add
     `http://localhost:9876/callback`.
   - **Sign-out redirect URIs**: Optionally, enter
     `https://<your-console-domain>/account/login`.

1. Click **Save**.

1. On the application's **General** tab, note the **Client ID**.

   *Single-page applications use PKCE instead of a client secret. You do not
   need a client secret for the console application.*

1. Go to **Security** > **API** and note your **Issuer URI** from the
   authorization server you want to use (e.g.,
   `https://your-org.okta.com/oauth2/default`).


   **Custom domains:** When the authorization server **Issuer** is set to **Dynamic (based on Request Domain)**, Okta issues tokens whose `iss` claim uses your custom domain (for example, `https://sso.your-org.com/oauth2/default`) instead of the default Okta URL. Configure the `oidc_issuer` system parameter in Materialize to match that issuer value exactly.

   {{% include-headless "/headless/okta-custom-authorization-server-warning" %}}

1. Go to the **Assignments** tab and assign the users or groups that should have
   access to Materialize.

   When a user authenticates via SSO, Materialize uses a JWT claim to determine
   the role name. See [Mapping IdP users to Materialize roles](#mapping-idp-users-to-materialize-roles) for more details.

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
     `https://<your-console-domain>/auth/callback`. After registration, you
     can add `http://localhost:9876/callback` under **Authentication** if you
     want to use the [CLI token flow](#get-a-token-using-cli-tools).

1. Click **Register**.

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

   When a user authenticates via SSO, Materialize uses a JWT claim to determine
   the role name. See [Mapping IdP users to Materialize roles](#mapping-idp-users-to-materialize-roles) for more details.

{{< /tab >}}
{{< tab "Generic OIDC" >}}

1. In your identity provider, create a new OIDC **public** client application
   (single-page application type) with the **Authorization Code** grant type
   and **PKCE** support.

1. Set the redirect URI to `https://<your-console-domain>/auth/callback`. If
   you want to use the [CLI token flow](#get-a-token-using-cli-tools), also
   add `http://localhost:9876/callback`.

1. Note the **client ID** and **issuer URL** provided by your identity provider.
   The issuer URL is typically the base URL of your identity provider's OIDC
   discovery endpoint (without `/.well-known/openid-configuration`).

1. Ensure the `openid` scope is available.

1. Assign users or groups that should have access to Materialize.

   When a user authenticates via SSO, Materialize uses a JWT claim to determine
   the role name. See [Mapping IdP users to Materialize roles](#mapping-idp-users-to-materialize-roles) for more details.

{{< /tab >}}
{{< /tabs >}}

{{< note >}}
Once you have configured your IdP, you will need the following values to [configure
OIDC system parameters for Materialize](#step-3-configure-oidc-system-parameters):

- The OIDC **issuer URL**
- The **client ID** for the console application
- If using service accounts, the client ID, client secret, and expected
  audience for each service-account application
{{</ note >}}

## Step 2. Enable OIDC authentication

To configure Self-Managed Materialize for OIDC authentication, update the
following fields:

| Resource | Configuration | Description
|----------|---------------| ------------
| Materialize CR | `spec.authenticatorKind` | Set to `Oidc` to enable OIDC authentication.
| Kubernetes Secret | `external_login_password_mz_system` | Specify the password for the `mz_system` user. Add `external_login_password_mz_system` to the Kubernetes Secret referenced in the Materialize CR's `spec.backendSecretName` field. The `mz_system` user **always** authenticates with a password. This user is required by the Materialize Operator for upgrades and serves as an emergency administrative account.
| ConfigMap | `mz-system-params` | Create an empty system parameter ConfigMap and reference it from the Materialize CR's `spec.systemParameterConfigmapName` field. You will populate it with your OIDC parameters in [Step 3](#step-3-configure-oidc-system-parameters).

The following example Kubernetes manifest includes configuration for OIDC
authentication:

{{< tabs >}}
{{< tab "v1alpha1" >}}

{{< self-managed/crd-version-note "v1alpha1" >}}

```yaml {hl_lines="6-15 26 36-38"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  # Create an empty system parameter configmap for later steps
  system-params.json: |
    {
    }
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
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
  backendSecretName: materialize-backend
  authenticatorKind: Oidc
  requestRollout: 00000000-0000-0000-0000-000000000003 # Switching to Oidc requires a rollout
  systemParameterConfigmapName: mz-system-params # Adding a system parameter configmap requires a rollout
```

Apply the updated manifest to your Kubernetes cluster. See
[Upgrading](/self-managed-deployments/upgrading/#rollout-configuration) for
details on rollout configuration.

{{< /tab >}}
{{< tab "v1" >}}

{{< self-managed/crd-version-note "v1" >}}

```yaml {hl_lines="6-15 26 36-37"}
apiVersion: v1
kind: Namespace
metadata:
  name: materialize-environment
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mz-system-params
  namespace: materialize-environment
data:
  # Create an empty system parameter configmap for later steps
  system-params.json: |
    {
    }
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
apiVersion: materialize.cloud/v1
kind: Materialize
metadata:
  name: 12345678-1234-1234-1234-123456789012
  namespace: materialize-environment
spec:
  environmentdImageRef: materialize/environmentd:{{< self-managed/versions/get-latest-version >}}
  backendSecretName: materialize-backend
  authenticatorKind: Oidc
  systemParameterConfigmapName: mz-system-params
```

Apply the updated manifest to your Kubernetes cluster. With the `v1` CRD,
rollouts trigger automatically when spec fields change, so no `requestRollout`
is needed. See
[Upgrading](/self-managed-deployments/upgrading/#rollout-configuration)
for details on rollout configuration.

{{< /tab >}}
{{< /tabs >}}

{{% include-headless
"/headless/self-managed-deployments/enabled-auth-setting-warning" %}}

## Step 3. Configure OIDC system parameters

Configure the OIDC system parameters to connect Materialize to your identity
provider. You can use either a
[ConfigMap](/self-managed-deployments/configuration-system-parameters/#configure-system-parameters-via-configmap)
or SQL commands, but it is strongly recommended to use a ConfigMap. See [Configure via Configmap](#configure-via-configmap) for more details.

### OIDC system parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `oidc_issuer` | The OIDC issuer URL (e.g., `https://your-org.okta.com/oauth2/default`). Materialize uses this to discover the JWKS endpoint for token validation. | Yes | None |
| `oidc_audience` | A JSON array of expected audience values for token validation (e.g., `["your-client-id"]`). Use the **client ID from [Step 1](#step-1-configure-your-identity-provider)**. Materialize checks that the JWT's `aud` claim contains at least one of these values. **By default, this is empty, and audience validation is skipped.**| No | `[]` |
| `oidc_authentication_claim` | The JWT claim to use as the Materialize username. For ID tokens (human users), a common claim is `email`. For access tokens from the [Client Credentials flow](#client-credentials-flow), ensure this claim exists in the token. See [Mapping IdP users to Materialize roles](#mapping-idp-users-to-materialize-roles) for details. | No | `sub` |
| `console_oidc_client_id` | The OIDC client ID used by the web console for the authorization code flow. | For console login | Empty |
| `console_oidc_scopes` | Space-separated OIDC scopes requested by the web console when obtaining a token. Scopes control which claims are included in the token. The `openid` scope is required to obtain an ID token. Add `email` to include the `email` claim, or `profile` to include name claims. If `oidc_authentication_claim` references a claim like `email`, you must request the corresponding scope here. | For console login | Empty |

{{< warning >}}
When `oidc_audience` is empty, audience validation is skipped. This means
**any** valid token from the same identity provider can authenticate to
Materialize, including tokens issued for other applications. **Always set
`oidc_audience` in production environments.**
{{</ warning >}}

### Configure via ConfigMap

In [Step 2](#step-2-enable-oidc-authentication), you already created an empty
`mz-system-params` ConfigMap. Now, populate that ConfigMap with your
OIDC parameters. At this point, your manifest should look like:

```yaml {hl_lines="9-13"}
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
```

{{< note >}}
This example sets `oidc_authentication_claim` to `email` rather than the default
`sub`, so each user's role name comes from their `email` claim. Because the
authentication claim references `email`, `console_oidc_scopes` includes the
`email` scope to ensure that claim is present in the token.
{{</ note >}}

Apply the updated ConfigMap to your Kubernetes cluster. The changes could take
up to a minute to take effect. For more
on configuring system parameters via a ConfigMap, see [System parameters
configuration](/self-managed-deployments/configuration-system-parameters/#configure-system-parameters-via-configmap).

### Configure via SQL

Alternatively, connect as `mz_system` and set the parameters using
`ALTER SYSTEM SET`. The `mz_system` user always authenticates with a password,
even when OIDC is enabled.

```mzsql
ALTER SYSTEM SET oidc_issuer = 'https://your-org.okta.com/oauth2/default';
ALTER SYSTEM SET oidc_audience = '["YOUR_CLIENT_ID"]';
ALTER SYSTEM SET oidc_authentication_claim = 'email';
ALTER SYSTEM SET console_oidc_client_id = 'YOUR_CLIENT_ID';
ALTER SYSTEM SET console_oidc_scopes = 'openid email';
```

## Step 4. Verify the configuration

1. Navigate to your Materialize Console. You should see an option to **Use
   single sign-on**.

   ![Materialize Console login screen showing the SSO sign-in
   option](/images/console/console-self-managed-sso.png "Materialize Console login screen
   with SSO option")

1. Sign in through your IdP. After successful authentication, you are redirected
   back to the Materialize Console.

1. To confirm which role you've signed in as via SSO, open the [SQL Shell](/console/sql-shell/) in the Materialize Console. In the welcome message, you should see the role name labeled under "User". This is derived from the `oidc_authentication_claim` claim in your identity token:

![Materialize Console Shell](/images/console/console.png "Materialize Console Shell")

## Connecting via SQL clients

To connect to Materialize using a SQL client like `psql`, you need an OIDC ID token.

If your client doesn't support OAuth, you can
create a role with a SQL password instead. See [SQL password authentication](#sql-password-authentication-recommended-for-non-oauth-clients).

### Get a token using CLI tools

You can fetch an
ID token from the command line using [`oauth2c`](https://github.com/cloudentity/oauth2c).
This is useful when configuring a non-interactive client like dbt or Terraform.

1. **Confirm `http://localhost:9876/callback` is registered as a redirect URI**
   on the console OIDC client (added in
   [Step 1](#step-1-configure-your-identity-provider)). `oauth2c` listens on
   this URL during the auth code exchange.

1. **Install `oauth2c`.** On macOS:

    ```shell
    brew install cloudentity/tap/oauth2c
    ```

    Other platforms: see the
    [`oauth2c` installation guide](https://github.com/cloudentity/oauth2c#installation).

1. **Run `oauth2c` to fetch the ID token:**

    ```shell
    oauth2c <ISSUER_URL> \
      --client-id <YOUR_CLIENT_ID> \
      --response-types code \
      --response-mode form_post \
      --grant-type authorization_code \
      --pkce \
      --scopes 'openid email' \
      --auth-method none \
      --silent | jq -r '.id_token'
    ```

    A browser window opens to complete the IdP login. After signing in,
    `oauth2c` prints the ID token to stdout.

ID tokens expire (typically within an hour). Re-run the command above when
your token expires.

### Get a token from the Materialize console

Clicking "Connect" in the Materialize console will provide you with an ID token that you can use to connect.

![Materialize Console connect instructions for OIDC](/images/console/console-connect-oidc.png "Materialize Console connect screen for OIDC")


### Connect with psql

Use the ID token as your password:

```shell
PGPASSWORD="<your-id-token>" \
psql -h <materialize-host> -p 6875 -U <username> materialize
```

Replace `<username>` with the value of the authentication claim in your JWT
(e.g., your email address if `oidc_authentication_claim` is set to `email`).

{{< note >}}
Materialize validates the token at **connection time only**. Once a connection
is established, it persists until disconnected, regardless of token expiry.
{{</ note >}}


## Connecting MCP clients

*OAuth sign-in for MCP clients is available starting in v26.31.*

Materialize provides built-in [MCP servers](/integrations/mcp-server/) at
`/api/mcp/agent` and `/api/mcp/developer`. When SSO is enabled, MCP clients
can authenticate with OAuth instead of an [MCP
token](/integrations/mcp-server/mcp-agent/#method-2-token-based-authentication).
Materialize publishes OAuth 2.0 Protected Resource Metadata ([RFC
9728](https://datatracker.ietf.org/doc/html/rfc9728)) at
`/.well-known/oauth-protected-resource`, which MCP-aware clients use to
discover your identity provider automatically.

Unlike the console, which authenticates with an ID token, MCP clients present
an OAuth **access token**. Access tokens have additional requirements:

1. **Pre-register an OIDC client for MCP.** The MCP specification expects the
   IdP to support anonymous Dynamic Client Registration ([RFC
   7591](https://datatracker.ietf.org/doc/html/rfc7591)). Most enterprise
   IdPs, including Okta, do not allow anonymous registration, so MCP clients
   fail during registration (in Okta, with HTTP 403 `E0000005`). Instead,
   create or reuse a public OIDC client with PKCE, add
   `http://localhost:<port>/callback` as a sign-in redirect URI, and configure
   the MCP client with the client ID explicitly.

1. **Include the authentication claim in access tokens.** IdPs typically
   include claims like `email` only in ID tokens. If
   `oidc_authentication_claim` is set to `email`, configure your authorization
   server to add an `email` claim to access tokens (in Okta, a claim with
   value `user.email`, included in the access token). Otherwise Materialize
   rejects the token because the authentication claim is missing.

1. **Add the authorization server audience to `oidc_audience`.** The `aud`
   claim of an access token is the authorization server's audience value, not
   the client ID. For Okta's default authorization server this is
   `api://default`. Materialize also validates ID tokens (browser sign-in),
   whose `aud` claim is the console client ID, so both values must be
   present in `oidc_audience`. Add the audience entries to the array,
   preserving any existing values:

   ```mzsql
   ALTER SYSTEM SET oidc_audience = '["YOUR_CLIENT_ID", "api://default"]';
   ```

   If several applications share this authorization server, consider
   creating a Materialize-dedicated custom authorization server in Okta so
   only tokens issued for Materialize carry the `api://materialize`-style
   audience. Otherwise any application on the same authorization server
   receives tokens with `aud=api://default`, which Materialize would then
   accept.

1. **Optional: define an `mcp.read` scope.** Materialize advertises the
   `mcp.read` scope in its resource metadata. The scope is not enforced by
   Materialize (authorization happens through
   [RBAC](/security/self-managed/access-control/)), but clients that request
   advertised scopes fail against IdPs that reject unknown scopes, such as
   Okta, unless the scope exists on the authorization server.

1. **Optional: add the `offline_access` scope for refresh tokens.** MCP
   clients typically request `offline_access` so they can refresh access
   tokens without a new browser sign-in. Okta and most enterprise IdPs
   require this scope to exist on the authorization server. If the client
   fails to reconnect after the first access token expires, add
   `offline_access` to the authorization server's scopes.

1. **Connect your MCP client.** For example, to connect Claude Code to the
   `materialize-agent` MCP server with a pre-registered client:

   ```shell
   claude mcp add --transport http materialize-agent \
     https://<host>:6876/api/mcp/agent \
     --client-id <YOUR_CLIENT_ID> --callback-port 8080
   ```

   The `--callback-port` value must match the
   `http://localhost:<port>/callback` redirect URI registered on the OIDC
   client. For more information, see
   [MCP servers](/integrations/mcp-server/).

{{< note >}}
Deployments behind a load balancer or proxy that rewrites the `Host` header
must set the `http_host_name` configuration so that the URLs Materialize
publishes in its resource metadata are correct.
{{</ note >}}

## Provisioning roles

### Mapping IdP users to Materialize roles

Each user or service account that authenticates via OIDC maps to a single
Materialize database role. When a user authenticates into Materialize, their role name is the value of the JWT claim keyed by `oidc_authentication_claim`.

For example, if `oidc_authentication_claim` is set to `email` and a user authenticates with the following JWT:

```json
{
  "sub": "auth0|abc123",
  "email": "alice@your-org.com",
  "name": "Alice",
  "iat": 1516239022
}
```

Their role name will be `alice@your-org.com`.

If a user logs in and no matching role exists, Materialize auto-provisions one,
as described in the next section.

### Auto-provisioning roles

When a user signs in and no role matching their `oidc_authentication_claim`
value exists, Materialize **automatically creates** a role for them.

Auto-provisioned roles:
- Have default privileges only.
- Must be granted additional privileges through
  [RBAC](/security/self-managed/access-control/manage-roles/).
- Are not automatically removed when the user is removed from the IdP. See
  [De-provisioning users](#de-provisioning-users) for cleanup instructions.

#### Auditing auto-provisioned roles

To view which roles were auto-provisioned via OIDC, query `mz_audit_events`:

```mzsql
SELECT details
FROM mz_audit_events
WHERE event_type = 'create' AND object_type = 'role' AND details ->> 'auto_provision_source' = 'oidc'
ORDER BY occurred_at DESC;
```

Roles created through OIDC authentication will have `auto_provision_source` set to
`oidc`.

### Pre-provisioning roles

An administrator can create roles before users login, rather than rely on
auto-provisioning. To pre-provision a role, connect as a superuser and create
the role with a name matching the expected JWT claim value:

```mzsql
CREATE ROLE "alice@your-org.com" WITH LOGIN;
```

Like auto-provisioned roles, pre-provisioned roles start with default privileges
only and must be granted additional privileges through
[RBAC](/security/self-managed/access-control/manage-roles/).

## Service accounts

For machine-to-machine access, you have three options:

- [SQL password authentication](#sql-password-authentication-recommended-for-non-oauth-clients):
  for clients that don't support OAuth flows.
- [Resource Owner Password flow](#resource-owner-password-flow): for service
  accounts that authenticate against your IdP with a username and password.
- [Client Credentials flow](#client-credentials-flow): for service accounts
  that authenticate against your IdP without a user context.

### SQL password authentication (recommended for non-OAuth clients)

Even with OIDC enabled, Materialize still accepts SQL password authentication.
This is required for clients that don't support OAuth flows. The simplest way to
give such a service or application access is to create a role with a SQL
password.

1. As a user with the `CREATEROLE` privilege, create the role with a password:

    ```mzsql
    CREATE ROLE "svc-dbt" WITH LOGIN PASSWORD 'a-strong-password';
    ```

1. Grant the privileges this service account needs. See
   [Manage database roles](/security/self-managed/access-control/manage-roles/)
   for the full privilege model.

1. Connect using the password directly:

    ```shell
    PGPASSWORD="a-strong-password" \
    psql -h <materialize-host> -p 6875 -U svc-dbt materialize
    ```

For dbt-specific setup, see [dbt connection profiles](/manage/dbt/get-started/).
For Terraform, see [Terraform: get started](/manage/terraform/get-started/).

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
   PGPASSWORD="<id-token>" \
   psql -h <materialize-host> -p 6875 -U svc-materialize@your-org.com materialize
   ```

{{< /tab >}}
{{< tab "Microsoft Entra ID" >}}

1. In the [Azure portal](https://portal.azure.com), go to **Microsoft Entra
   ID** > **App registrations** and click **New registration**. Create a
   dedicated registration for this flow rather than reusing the console
   application from [Step 1](#step-1-configure-your-identity-provider).

1. Configure the registration:
   - **Name**: Enter a name (e.g., `Materialize ROPC`).
   - **Supported account types**: Select the appropriate option for your
     organization.

1. Click **Register**.

1. Go to **Authentication** and set **Allow public client flows** to **Yes**.
   This is required for the Resource Owner Password flow.

1. On the application's **Overview** page, note the **Application (client) ID**
   and the **Directory (tenant) ID**.

1. Go to **Certificates & secrets** > **New client secret**. Add a description
   and expiration, then click **Add**. Note the secret **Value**.

1. Create a new user to serve as the service account, then assign it to this
   application under **Enterprise applications** > **Users and groups**.

1. Fetch an ID token:

   ```shell
   curl -X POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=password" \
     --data-urlencode "username=svc-materialize@your-org.com" \
     --data-urlencode "password=YOUR_SERVICE_ACCOUNT_PASSWORD" \
     --data-urlencode "scope=openid email" \
      --data-urlencode "client_id=YOUR_CLIENT_ID"
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_CLIENT_SECRET"
   ```

1. Extract the `id_token` from the JSON response and use it to connect:

   ```shell
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
     --data-urlencode "scope=openid email" \
     --data-urlencode "client_id=YOUR_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_CLIENT_SECRET"
   ```

1. Extract the `id_token` from the JSON response and use it to connect:

   ```shell
   PGPASSWORD="<id-token>" \
   psql -h <materialize-host> -p 6875 -U svc-materialize@your-org.com materialize
   ```

{{< /tab >}}
{{< /tabs >}}

### Client Credentials flow

Use this approach to treat an IdP client as a service account. This is useful
for automated systems that do not have a user context.

{{< note >}}
`oidc_audience` is an array of values. Before running the `ALTER SYSTEM SET
oidc_audience` examples below, check the current value with `SHOW oidc_audience;`
and **append** the new audience rather than overwriting it. Otherwise you may
remove the console's audience or other configured values.
{{</ note >}}

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
   its own Okta application.*

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

1. Ensure `oidc_audience` includes the expected audience value for tokens
   from your authorization server. In Okta, the `aud` claim is set to the
   authorization server's audience (configured in **Security** > **API** >
   your auth server > **Settings**), not the client ID. For the default
   authorization server, this is typically `api://default`:

   ```mzsql
   -- Make sure to add to the array if already set
   ALTER SYSTEM SET oidc_audience = '["api://default"]';
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
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

1. Fetch an access token:

   ```shell
   curl -X POST https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=client_credentials" \
     --data-urlencode "scope=YOUR_SERVICE_CLIENT_ID/.default" \
     --data-urlencode "client_id=YOUR_SERVICE_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_SERVICE_CLIENT_SECRET"
   ```

1. Ensure `oidc_audience` includes the expected audience value for Client
   Credentials tokens. In Entra, the `aud` claim is determined by the `scope`
   parameter in the token request. When using `YOUR_SERVICE_CLIENT_ID/.default`,
   the audience is the service client ID:

   ```mzsql
   -- Make sure to add to the array if already set
   ALTER SYSTEM SET oidc_audience = '["YOUR_SERVICE_CLIENT_ID"]';
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
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

1. Fetch an access token from your IdP's token endpoint:

   ```shell
   curl -X POST https://your-idp.com/oauth2/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     --data-urlencode "grant_type=client_credentials" \
     --data-urlencode "scope=openid" \
     --data-urlencode "client_id=YOUR_SERVICE_CLIENT_ID" \
     --data-urlencode "client_secret=YOUR_SERVICE_CLIENT_SECRET"
   ```

1. Ensure `oidc_audience` includes the expected audience value for Client
   Credentials tokens. Check the `aud` claim in the token issued by your IdP
   to determine the correct value:

   ```mzsql
   -- Make sure to add to the array if already set
   ALTER SYSTEM SET oidc_audience = '["YOUR_AUDIENCE_VALUE"]';
   ```

1. Extract the `access_token` from the JSON response and use it to connect:

   ```shell
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
| MCP client fails during registration with the IdP (HTTP 403) | The IdP does not support anonymous Dynamic Client Registration | Pre-register an OIDC client and configure the MCP client with its client ID. See [Connecting MCP clients](#connecting-mcp-clients) |
| MCP client login fails with `invalid_scope` | The client requested the advertised `mcp.read` scope, which does not exist on the authorization server | Add an `mcp.read` scope to the authorization server, or configure the client to request only standard scopes |
| MCP client completes login but the connection is rejected | Access token `aud` is not in `oidc_audience`, or the authentication claim is missing from access tokens | See [Connecting MCP clients](#connecting-mcp-clients) |

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
