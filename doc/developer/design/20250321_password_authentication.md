# Adding Password Authentication to Materialize

-   Associated:
    -   https://github.com/MaterializeInc/cloud/issues/10754

## The Problem

Currently, Materialize does not provide a mechanism for password-based authentication in self-hosted environments. Users need a secure way to authenticate with the database to control access to secure their environments and enable role-based access control (RBAC).

## Success Criteria

A successful solution should accomplish the following:

1. **Secure Authentication**: Implement a password authentication method for Materialize that ensures secure access control.
2. **Compatibility with Future Authentication Methods**: Design the solution to support future authentication methods like JWT or mTLS without blocking their implementation.
3. **Broad PostgreSQL Driver Support**: Ensure the password authentication mechanism works seamlessly with various PostgreSQL clients and tools, including pgwire, Tableau, PowerBI, etc.
4. **HTTP support**: The authentication should work for the external HTTP interface to materialize.
5. **Password Storage Upgrade Path**: Implement a system for securely storing passwords in a way that can be easily upgraded to stronger encryption mechanisms, such as SCRAM-SHA256.
6. **Bootstrapping System Users**: Provide a mechanism for bootstrapping the password and enabling login for system users.
7. **Compatibility with Cloud Product**: The solution should be optionally enabled allowing both the current Frontegg authentication and no authentication to be used.

## Out of Scope

The solution will **not**:

1. Implement SCRAM-SHA256 authentication in this phase, although the design will prepare for an easy transition to SCRAM-SHA256 in the future.
2. Support complex user features such as password expiration or password validation rules.
3. Integrate with third-party identity providers (IDPs) like SCIM, SAML, or OAuth.
4. Auth integration at the balancer layer.

## Solution Proposal

The proposed solution is to implement a **password authentication mechanism** that aligns with [PostgreSQL's "password" authentication method](https://www.postgresql.org/docs/current/auth-password.html).
The key components are:

### 1. PostgreSQL-Compatible Password Authentication:

This password authentication will function similarly to PostgreSQL’s "password" method. Users will authenticate with a username and password passed in plain text to Environmentd, which will securely compare the stored values in the system. The "plain text" values may be encrypted at the TLS layer but will be plain text within the pgwire connection.

Passwords hashed with `scram-sha-256` will be stored in the configured secret store. A new catalog entry will be created to store sensitive authentication metadata, like a reference to the secret store, and possibly eventually attempt counts or other metadata.

**Authentication Flow**:

1. The user sends a username and password to the Materialize server.
2. The protocol layer will communicate with the coordinator layer to validate the password. (via the adapter Client)
  a. The coordinator layer will check the password against the stored hash. If the password is correct, the user will be authenticated.
  b. If the password is incorrect, the user will be denied access.

3. **Audit and Metrics:** Login and failed login requests should be logged. Metrics should be created to monitor potential security events.

**Password Rotation or Expiration**: While not currently in scope, the design should allow for additions in the future, such as password rotation and expiration.

### 2. Password Management Syntax:

Users should be able to manage their passwords and set passwords for roles they create. Superusers should be able to alter any non-internal user's password.

-   **Syntax Examples**:

    -   `CREATE ROLE hunter WITH PASSWORD 'hunter2';`
    -   `ALTER ROLE hunter SET (PASSWORD 'hunter2');`
    -   `ALTER ROLE user_name PASSWORD NULL`

-   **Password Policies**: We will not implement password policies but should enforce a configurable minimum password length.

-   **Login Role Determination**: We should display whether a role has a password and can be used as a login role in `mz_roles`. See [pg_roles](https://www.postgresql.org/docs/9.1/view-pg-roles.html).

### 3. Hashed Password Storage

-   **Secure Hashing**: For future compatibility with SCRAM, passwords should be hashed using `scram-sha-256` encryption. The RFC for [SCRAM](https://datatracker.ietf.org/doc/html/rfc5802) dictates an iteration count of at least 4096. However, the RFC for SCRAM-SHA-256 dictates that "[the hash iteration-count should be such that a modern machine will take 0.1 seconds to perform the complete algorithm](https://www.rfc-editor.org/rfc/rfc7677)". This also dictates at least 4096 with a recommendation of 15000. We should aim for 15000 if it does not negatively impact connection times or load.

-   **Access Control**: Hashed passwords should not be extracted from the secret store and should not be accessible through queryable tables. Passwords must not be logged or output in stack traces.

-   **Password Dating**: The creation date of passwords will be recorded along with the hashed and salted password. When the password changes a creation date should be reset. The creation date of passwords should be queryable.

-   **Storage Location Password Hash**: The password will be stored in the catalog in its own object, `RoleAuth`. This object must record the following:
    - role_id
    - creation_date
    - algorithm / algorithm meta: IE for sha256: algo: sha256, salt: "some-string", iteration_count: some-int
    Example struct:
    ```Rust
    RoleAuth {
        role_id: ident
        password_hash: String
        creation_date: Date
    }
    ```

    - Iterations must be >= 400,000... this **must be configurable with a system variable.**
    - We must use at least 32 bytes of cryptographically random data for salts.

-   **OPTIONAL: Password Versioning**: Updates to the password hashing mechanisms may be required.
  - For passwords received in plain text we should be able re-encrypt them with stronger algorithms.
  - For client side challenges we may be able to increase the number of hash iterations.
  We will already be storing the necessary password hashing metadata to perform these actions.

### 4. Configurable admin system login:

Passwords for `mz_system`(v1) and `mz_support`(TBD) roles will be configurable via an Environmentd flag `external_login_password_<USER>`, or `external_login_password_secret_<user>` only one is required to be implemented, but the emulator must support configuration via environment variables. Orchestratord should provide a set of parameters to set these parameters. Additionally, We should enable login of system users through the external ports when they have external login passwords set. Login through the external port must not be possible unless this flag is set, this logic should not rely on whether the internal user has a password. Our Helm chart and Orchestratord will be adjusted to support these parameters.


### 5. HTTP Authentication:

Environmentd's HTTP endpoint will have added support for session-based authentication. This includes session creation, storage, and deleting/setting cookies. To handle sessions we should use [tower session-store](https://docs.rs/tower-sessions/latest/tower_sessions/). This should explicitly require secure cookies `.with_secure(true)` when running with TLS.
Required new endpoints:
  - POST: api/login
    - Accepts username, password Authentication Headers
    - imposes strict rate limiting : returning 429
    - returns 401 on auth failure
    - returns 201 on success and sets a session token in the cookies
  - POST: api/logout
    - returns 204 if session is found
    - returns 404 if session is not found

Additionally, we'll need to add a tower middleware layer that validates cookie authentication for all endpoints returning 403 when the user is not authenticated.


### Related Superuser Details:
Enabling this feature we will require us to provide an alternative method to declare superusers. This is currently done through JWT metadata. In order for this to work with passwords we will need to provide syntax:
```sql
ALTER ROLE ... WITH SUPERUSER;
ALTER ROLE ... WITH NO SUPERUSER;
```

A role's superuser state should be stored as a rolsuper along with the user in the catalog. It may make sense to make this an Option<bool>. If the value of rolsuper is not `Some(v)` then we defer to metadata if we're using frontegg auth.


### Note on Configurable Authentication Mechanism

The authentication mechanism for an environment must be configurable. A new flag will be created to select the authentication `mz_authentication_type` with the options of `frontegg`, `password`, and `disabled`. This will subsume the enable_authentication flag.


### Note on Console builds:

Console does currently do not support runtime or startup configuration. Configuration is handled only at build time. To resolve this we should add a `config.json` or `config.js` file which can be mounted directly into the Nginx container assets. This file should come from a materialize-console config map which must be setup by Orchestratord.  We will also need changes to the console to support reading in configuration from this map. The initial config value here should be `authentication_type: password`, in cloud we should use `authentication_type: frontegg` or `authentication_type: jwt`. The console build process can still be used to set default values for this config file.


### Note on Emulator Auth:
By default authentication will not enabled in the emulator. This should be configurable with the ability to both turn on authentication and to set a password for the `mz_system` user.

## Minimal Viable Prototype

A minimal viable prototype (MVP) for this solution will include:

1. **Password Authentication Implementation**: Implement the basic password authentication flow that authenticates users in the pgwire and HTTP protocol layers.
2. **Password Management**: Add password management via role create/alter.
3. **Secure Password Storage**: Implement hashed salted password storage using compatible with `scram-sha-256`.
4. **Bootstrapping System Users**: Provide a mechanism for bootstrapping the password and enabling login for system users.
5. **HTTP Session Management**: Environmentd's HTTP layer needs to handle session management via cookies and internal session storage.
6. **Enhanced Superuser Management**: Support assigning / unassigning superuser for roles.

## Alternatives

1. **Authd Service**: This would involve creating a separate authentication service to handle authentication. This approach is more complex and introduces security concerns, as it would require developing secure endpoints and integrating a user management system that could be prone to additional complexity and security risks.

    - **Reasons Not Chosen**: This solution is more involved, requiring significant additional work to secure, develop, and maintain. Additionally, it does not align with Postgres.

2. **Password Authentication in Ingress Layer**: Using a front-end proxy like PGbouncer or NGINX to handle password authentication before traffic hits Materialize.

    - **Reasons Not Chosen**: This approach requires significant customization and doesn't solve the problem within the Materialize database itself. It also still necessitates internal authentication for role management.

3. **Store Passwords in K8s Secrets**: It would be viable to store passwords in Kubernetes secrets.

-   **Reasons Not Chosen**: This may create an untenable number of Kubernetes secrets, and may cause issues with passing security reviews.

4. **App Passwords**: Rather than allowing users to configure a password per user we could generate "app-passwords" that have a many-to-one relationship with users, are auto-generated, and stored in k8s secrets.

-   **Reasons Not Chosen**: This is slightly more complicated and non-compliant with Postgres.

-   **Reasons I still might prefer this**: It does have a number of benefits, including slightly better security via enforcement of random strong passwords, and a better rotation story. It's also possible we could have different app password protocols/versions that allow users to manage their passwords entirely through secrets directly or something like Hashicorp vault.

## Open questions

1. **Where exactly are passwords stored**: While it makes sense to store passwords in the catalog, I have not yet identified exactly where they should be stored. Should it go alongside `mz_roles`, or in a new `mz_auth_id` table. The latter makes sense as it could store password metadata, and might let us store multiple passwords down the road.
**A:** In the catalog

2. **Audit Logging and Brute Force Detection**: Do we need to log all failed password attempts and the IPs of those attempts? Do we need to take action to lock an IP or User when an attempt threshold is met?
**A:** yes log attempts and failed attempts along with the password. No mitigation is required, at least not for v1.

3. **Password Strength Validation**: Should Materialize include built-in password strength validation, or should it be handled outside the system (e.g., via Kubernetes or other security tools)?
**A:** MZ should not have built-in password strength, down the road we may want to consider something like postgres's [passwordcheck](https://www.postgresql.org/docs/current/passwordcheck.html
).
