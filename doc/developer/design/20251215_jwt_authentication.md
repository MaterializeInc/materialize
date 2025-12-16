# JWT Authentication

- Associated: (Insert list of associated epics, issues, or PRs)

PRD: https://www.notion.so/materialize/SSO-for-Self-Managed-2a613f48d37b806f9cb2d06914454d15


## Problem

Our goal is to enable single sign on (SSO) for our self managed product

## Success Criteria

- Creating a user & adding roles: An admin gives a user access to Materialize
- The user should be disabled from logging in when a user is de-provisioned. However, the database level role should still exist.
- The end user is able to create a token to connect to materialize via psql / postgres clients
- The end user is able to visit the Materialize console, and sign in with their IdP

## Configuration

JWT authentication can be enabled by specifying the following Materialize CRD:

```yaml
apiVersion: materialize.cloud/v1alpha1
kind: Materialize
metadata:
  ...
spec:
  ...
  authenticatorKind: Jwt
  jwtAuthenticationSettings:
  # Must match the OIDC client ID. Required.
    audience: 060a4f3d-1cac-46e4-b5a5-6b9c66cd9431
    # The claim that represents the user's name in Materialize.
    # Usually either "sub" or "email". Required.
    authenticationClaim: email
    # The key for the `groups` claim. Optional and defaults to "groups"
    groupClaim: groups
    # The expected issuer URL, must correspond to the `iss` field of the JWT.
    # Implicitly fetch JWKS from
    # https://{ domain }/.well-known/openid-configuration and allow override
    # if we need to. Required.
    issuer: https://dev-123456.okta.com/oauth2/default
    # Where Materialize will request tokens from the IdP using the refresh token
    # if it exists. Optional.
    tokenEndpoint: https://dev-123456.okta.com/oauth2/default/v1/token
```

Where in environmentd, it’ll look like so:

```bash
bin/environmentd -- \
--listeners-config-path='/listeners/jwt.json' \
--jwt-authentication-setting="{\"audience\":\"060a4f3d-1cac-46e4-b5a5-6b9c66cd9431\",\"authentication_claim\":\"email\",\"group_claim\":\"groups\",\"issuer\":\"https://dev-123456.okta.com/oauth2/default\",\"token_endpoint\":\"https://dev-123456.okta.com/oauth2/default/v1/token\"}"
```

## Testing Frameworks

- For unit testing, use cargo nextest. Similar to frontegg-mock, create a mock oidc server that exposes the correct endpoints, provides a correct JWKS, and generates a JWT with desired claims and fields.
- For end to end testing, use mzcompose and integrate an Ory hydra server docker image as the IDP. Then we can assert against an instance of Materialize created by orchestratord.

## Phase 1: Create a JWT authenticator kind

### Solution proposal: Creating a user & adding roles: An admin gives a user access to Materialize

By specifying an audience, we ensure an admin must explicitly give a user in the IDP access to Materialize as long as they use a client exclusively for Materialize. Otherwise when a user first logins, we create a role for them if it does not exist.

### Solution proposal: The user should be disabled from logging in when a user is de-provisioned. However, the database level role should still exist.

When doing pgwire jwt authentication, we can accept a cleartext password of the form `access=<ACCESS_TOKEN>&refresh=<REFRESH_TOKEN>` where `&` is a delimiter and `refresh=<REFRESH_TOKEN>` is optional. The JWT authenticator will then try to authenticate again and fetch a new access token using the refresh token when close to expiration (using the token API URL in the spec above). If the refresh token doesn’t exist, the session will invalidate. The implementation will be very similar to how we refresh tokens for the Frontegg authenticator. This would require users to have their IDP client generate `refresh` tokens.

By suggesting a short time to live for access tokens, this accomplishes invalidating sessions on deprovisioning of a user. When admins deprovision a user, the next time the user tries to authenticate or refresh their access token, the token API will not allow the user to login but will keep the role in the database.

**Alternative: Use SASL Authentication using the OAUTHBEARER mechanism rather than a cleartext password**

This would be the most Postgres compatible way of doing this and is what it uses for its `oauth` authentication method. However, it may run into compatibility issues with clients. For example in `psql`, there’s no obvious way of sending the bearer token directly without going through libpq's device-grant flow. Furthermore, assuming access tokens are short lived, this could lead to poor UX given there’s no native way to re-authenticate a pgwire session. Finally, our HTTP endpoints wouldn’t be able to support this given they don’t support SASL auth.

OAUTHBEARER reference: [https://www.postgresql.org/docs/18/sasl-authentication.html#SASL-OAUTHBEARER](https://www.postgresql.org/docs/18/sasl-authentication.html#SASL-OAUTHBEARER)

### Solution proposal: The end user is able to create a token to connect to materialize via psql / postgres clients

Unfortunately, to provide a nice flow to generate the necessary access token and refresh token, we’d need to control the client. Thus we’ll leave the retrieval of the access token/refresh token to the user, similar to CockroachDB.

**Alternative: Revive the mz CLI**

We have an `mz` CLI that’s catered to Cloud and no longer supported. We can potentially bring this back.

**Open question:** Is there anything we can do on our side to easily provide access tokens / refresh tokens to the user without controlling the client? This feels like the missing piece between JWT authentication and something like `aws sso login` in the AWS CLI

### Solution proposal: The end user is able to visit the Materialize console, and sign in with their IdP

A generic Frontend SSO redirect flow would need to be implemented to retrieve an access token and refresh token. However once retrieved, the SQL HTTP / WS API endpoints can use bearer authorization like Cloud and accept the access token. The Console would be in charge of refreshing the access token. The Console work is out of scope for this design document.

### Work items:

- Create a JWT Authenticator
    - Create environmentd CLI arguments to accept the JWT authentication configuration above
    - Wire up the HTTP endpoints, WS endpoints, and pgwire for this authenticator kind
- Add a `jwt` authenticator kind to orchestratord and create a `jwt` listener config
    - Still leave a port open for password auth for `mz_system` logins given admins can’t map a user in their IDP to `mz_system`

An MVP of what this might look like exists here: [https://github.com/MaterializeInc/materialize/pull/34516](https://github.com/MaterializeInc/materialize/pull/34516). Some differences from the proposed design:
- Does not implement the refresh token flow
- Does not validate the psql username against the OIDC user
- Does not do `aud` validation
- Does not use a JSON map for the environmentd CLI arguments

### Tests:

- Successful login (e2e mzcompose)
- Invalidating the session on access token expiration and no refresh token (Rust unit test)
- A token should successfully refresh if the access token and refresh token are valid (Rust unit test)
- Session should error if access token is invalid (Rust unit test)
- Session should error if refresh token is invalid (Rust unit test)
- De-provisioning a user should invalidate the refresh token (e2e mzcompose)
- Platform-check simple login check (platform-check framework)

## Phase 2: Map the `admin` claim to a user’s superuser attribute

Based on the `admin` claim, we can set the `superuser` attribute we store in the catalog for password authentication. We do this by doing the following:

- First, in our authenticator, save `admin` inside the user’s `external_metadata`
- Next, in `handle_startup_inner()` we diff them with the user’s current superuser status and if there’s a difference, apply the changes with an `ALTER` operation. We can use `catalog_transact_with_context()` for this.
- On error (e.g. if the ALTER isn’t allowed), we’ll end the connection with a descriptive error message. This is a similar pattern we use for initializing network policies.

This is similar to how we identify superusers in Frontegg auth, except we also treat it as an operation to update the catalog
  - We can keep using the session’ metadata as the source of truth to keep parity with Cloud, but eventually we’ll want to use the catalog as the source of truth for all. We can call this **out of scope.**

Prototype: [https://github.com/MaterializeInc/materialize/pull/34372/commits](https://github.com/MaterializeInc/materialize/pull/34372/commits)

### Tests

- Authenticating with a varying `admin` claim should reflect when querying the superuser status of adapter
- Superuser status should reflect in `mz_roles` (Rust unit test)


<!--
What is left unaddressed by this design document that needs to be
closed out?

When a design document is authored and shared, there might still be
open questions that need to be explored. Through the design document
process, you are responsible for getting answers to these open
questions. All open questions should be answered by the time a design
document is merged.
-->
