# OIDC Authentication

- Associated: (Insert list of associated epics, issues, or PRs)

PRD: https://www.notion.so/materialize/SSO-for-Self-Managed-2a613f48d37b806f9cb2d06914454d15


## Problem

Our goal is to enable single sign on (SSO) for our self managed product

## Success Criteria

- Creating a user & adding roles: An admin gives a user access to Materialize
- When the user is removed from the upstream IDP they are no longer able to login to Materialize but their role will persist in its current state.
- The end user is able to create a token to connect to materialize via psql / postgres clients

## Non-goals
- JWK refresh
- The end user is able to visit the Materialize console, and sign in with their Identity Provider (IdP)
- SCIM
- Syncing IdP roles and attributes to Materialize roles and attributes

## Configuration

OIDC authentication can be enabled by specifying the following Materialize CRD:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  ...
spec:
  ...
  authenticatorKind: Oidc
  oidcAuthenticationSettings:
    # Must match the OIDC client ID. Optional, but we should recommend setting this such that users can't authenticate with JWTs intended for other apps.
    audience: 060a4f3d-1cac-46e4-b5a5-6b9c66cd9431
    # The claim that represents the user's name in Materialize.
    # Usually either "sub" or "email". Required.
    authenticationClaim: email
    # The expected issuer URL, must correspond to the `iss` field of the JWT.
    # Required.
    issuer: https://dev-123456.okta.com/oauth2/default
    # The JWKS (JSON Web Key Set) used to validate JWT signatures. Optional.
    # If not provided, jwksFetchFromIssuer must be true.
    # Format: {"keys": [{"kty": "RSA", "kid": "...", "n": "...", "e": "..."}]}
    jwks: '{"keys": [...]}'
    # If true, fetches JWKS from https://{issuer}/.well-known/openid-configuration.
    # Overrides jwks if both are specified. Defaults to false.
    # Note: Not all JWT providers support .well-known/openid-configuration,
    # so use jwks directly if the provider doesn't support it.
    jwksFetchFromIssuer: true
```

Where in environmentd, it’ll look like so:

```bash
bin/environmentd -- \
--listeners-config-path='/listeners/oidc.json' \
--oidc-authentication-setting="{\"audience\":\"060a4f3d-1cac-46e4-b5a5-6b9c66cd9431\",\"authentication_claim\":\"email\",\"issuer\":\"https://dev-123456.okta.com/oauth2/default\",\"jwks\":{\"keys\":[...]},\"jwks_fetch_from_issuer\":true,\"token_endpoint\":\"https://dev-123456.okta.com/oauth2/default/v1/token\"}"
```

## Testing Frameworks

- For unit testing, use cargo nextest. Similar to frontegg-mock, create a mock oidc server that exposes the correct endpoints, provides a correct JWKS, and generates a JWT with desired claims and fields.
- For end to end testing, use mzcompose and integrate an Ory hydra server docker image as the IdP. Then we can assert against an instance of Materialize created by orchestratord.

## Phase 1: Create a OIDC authenticator kind

### Solution proposal: Creating a user & adding roles: An admin, someone in charge of ensuring that users can only access applications which they are authorized to, gives a user access to Materialize

If an admin wants to forbid JWTs issued for other applications (e.g., Slack, internal tools) authenticating with Materialize, they can specify the `aud` (audience) claim to match the Materialize-specific OIDC client ID. We will keep it optional to keep parity with the Frontegg authenticator.

When a user first logs in with a valid token, we create a role for them if one does not already exist.

### Solution proposal: The user should be disabled from logging in when a user is removed from the upstream IDP. However, the database level role should still exist.

When doing pgwire Oidc authentication, we can accept a cleartext password that is the access token. The OIDC authenticator will do JWT authentication on the access token. If the token is expired, the session will not be established. We will not do any invalidation on the session if the session has already been authenticated/established, but the token is expired. Eventually, the token will expire and the user will not be able to authenticate a new session. This creates a tradeoff between security and developer experience, but is acceptable since organizations will have supplemental methods of deprovisioning users outside of the database. This accomplishes disabling a user from logging in, but the database role still existing.

**Alternative: Use a refresh token flow to invalidate active sessions**

When doing pgwire Oidc authentication, we can accept a cleartext password of the form `access=<ACCESS_TOKEN>&refresh=<REFRESH_TOKEN>` where `&` is a delimiter and `refresh=<REFRESH_TOKEN>` is optional. The OIDC authenticator will then try to authenticate again and fetch a new access token using the refresh token when close to expiration (using the token API URL in the spec above). If the refresh token doesn’t exist, the session will invalidate. This would require users to have their IdP client generate `refresh` tokens. For token expiration checking, in a task, we'll repeatedly wait for `(expiration - now) * 0.8` and see if it's less than a minute. This is also how we check token expiration in the Frontegg authenticator. We'll also implement a config variable to turn off this mechanism and have it default to true.

This approach would enhance security by ensuring that sessions are invalidated once the access token expires. However, it would also introduce additional complexity and degrade the developer experience, as it would require users to configure refresh tokens in their IdP. Additionally, some IdPs may impose rate limits on token refresh operations. By opting for a simpler design, we minimize potential incompatibilities with various IdPs.

**Alternative: Use SASL Authentication using the OAUTHBEARER mechanism rather than a cleartext password**

This would be the most Postgres compatible way of doing this and is what it uses for its `oauth` authentication method. However, it may run into compatibility issues with clients. For example in `psql`, there’s no obvious way of sending the bearer token directly without going through libpq's device-grant flow. Furthermore, assuming access tokens are short lived, this could lead to poor UX given there’s no native way to re-authenticate a pgwire session. Finally, our HTTP endpoints wouldn’t be able to support this given they don’t support SASL auth.

In case we need to support SASL+OAUTH in the same Materialize instance, we can create a new port for it. For now we will call it out of scope.

OAUTHBEARER reference: [https://www.postgresql.org/docs/18/sasl-authentication.html#SASL-OAUTHBEARER](https://www.postgresql.org/docs/18/sasl-authentication.html#SASL-OAUTHBEARER)

### Solution proposal: The end user is able to create a token to connect to materialize via psql / postgres clients

Unfortunately, to provide a nice flow to generate the necessary access token and refresh token, we’d need to control the client. Thus we’ll leave the retrieval of the access token/refresh token to the user, similar to CockroachDB.

**Out of scope**:
- Providing an easy way to gain access tokens  / refresh tokens from the user's IdP.
- Controlling the client and reviving the `mz` cli

### Solution proposal: The end user is able to visit the Materialize console, and sign in with their IdP

**Out of scope**:
-A generic Frontend SSO redirect flow would need to be implemented to retrieve an access token and refresh token. However once retrieved, the SQL HTTP / WS API endpoints can use bearer authorization like Cloud and accept the access token. The Console would be in charge of refreshing the access token.

### Work items:

- Create a OIDC Authenticator
    - Create environmentd CLI arguments to accept the OIDC authentication configuration above
    - Wire up the HTTP endpoints, WS endpoints, and pgwire for this authenticator kind
- Add a `Oidc` authenticator kind to orchestratord and create a `Oidc` listener config
    - Still leave a port open for password auth for `mz_system` logins given admins can’t map a user in their IdP to `mz_system`

An MVP of what this might look like exists here: [https://github.com/MaterializeInc/materialize/pull/34516](https://github.com/MaterializeInc/materialize/pull/34516). Some differences from the proposed design:
- Does not implement the refresh token flow
- Does not validate the psql username against the OIDC user
- Does not do `aud` validation
- Does not use a JSON map for the environmentd CLI arguments

### Tests:

- Successful login (e2e mzcompose)
- Session should error if access token is invalid (Rust unit test)
- A user shouldn't be able to login as another user (Rust unit test)
- Platform-check simple login check (platform-check framework)
- JWTs should only be accepted when a valid JWK is set (we do not want to accept JWTs that are not signed with a real, cryptographically sound key)

## Phase 2: Make OIDC configurable on runtime

When solely relying on flags to `environmentd` for OIDC configuation, for an admin to update the OIDC configuration (i.e. rotating the JWKs), they'd need to do an entire rollout of the Materialize instance. To prevent this, we can provide an alternative way of updating the configuration on runtime. By creating system parameter variables for each configuration variable, we can enable users to update this config through SQL or through the system parameter configmap. Orchestratord will use the Materialize CRD spec to populate the default system variables for OIDC configuration.

## Out of scope: Sync roles and attributes from an IdP to Materialize roles and attributes

This phase is out of scope, but it's still useful to describe how we can implement it.

### Potential approach: Syncing roles through the `roles` claim in a JWT
Based on the `roles` claim, we can synchronize the roles the user belongs to in the catalog. We do this by doing the following:

- First, in our authenticator, save `roles` from the `roles` claim inside the user’s `external_metadata`
- Next, in `handle_startup_inner()` we diff them with the user’s current roles and if there’s a difference, apply the changes with `GRANT` operations. We can use `catalog_transact_with_context()` for this. Furthermore, the roles would need to exist in the system first.
- On error (e.g. if the ALTER isn’t allowed), we’ll end the connection with a descriptive error message. This is a similar pattern we use for initializing network policies.

For syncing the `SUPERUSER` attribute, we can do something similar to above where we allow the user to configure a keyword that represents the "superuser role". Then in the `roles` claim, we look for that role and sync their superuser status using the same steps above, except we use `ALTER` instead of `GRANT`. This is similar to how we identify superusers in Cloud currently. The difference being in Cloud, we don't update the catalog and use `external_metadata` as the source of truth.
  - We can keep using the session’ metadata as the source of truth to keep parity with Cloud, but eventually we’ll want to use the catalog as the source of truth for all.

Prototype: [https://github.com/MaterializeInc/materialize/pull/34372/commits](https://github.com/MaterializeInc/materialize/pull/34372/commits)
