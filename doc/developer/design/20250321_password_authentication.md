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
6. **Configurable System User Password**: Support settings passwords for internal roles, `mz_system` and `mz_support`, via environment variables that can be set up in Orchestratord using k8s secrets.
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

3. **OPTIONAL: Book-Keeping**: Login and failed login requests should be tracked. Metrics should be created to monitor potential security events.

**Password Rotation or Expiration**: While not currently in scope, the design should allow for additions in the future, such as password rotation and expiration.

**Abuse Detection**: Action should be taken action when brute force attempts are detected, either by locking the user or blocking the requesting client IP.

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

-   **OPTIONAL: Password Versioning**: Updates to the password hashing mechanisms may be required. As long as we are receiving passwords in plain text we should be able to take a validated password and replace the existing a new securely hashed value. This may require prefixing the password with data about the hash algorithm or parameters.

### 4. Configurable System User Passwords:

Passwords for `mz_system` and `mz_support` roles will be settable via environment variables `MZ_MZ_SYSTEM_ROLE_PASSWORD` and `MZ_MZ_SUPPORT_ROLE_PASSWORD`. Orchestratord should provide a set of parameters to set these variables via a kubernetes secret. Additionally, We should enable login of `mz_system` through the external ports when they have passwords set. This should require both an explicit feature flag, `enable_public_internal_user_login` to be set to true, and a password set in an environment variable.

### 5. HTTP Authentication:

Environmentd's HTTP endpoint will need to have added support for session-based authentication. This includes session creation, storage, and deleting/setting cookies. To handle sessions we should use [tower session-store](https://docs.rs/tower-sessions/latest/tower_sessions/). This should explicitly require secure cookies `.with_secure(true)` when running with TLS.

### Configurable Authentication Mechanism

The authentication mechanism for an environment must be configurable. A new flag will be created to select the authentication `mz_authentication_type` with the options of `frontegg`, `password`, and `disabled`. This will subsume the enable_authentication flag.

## Minimal Viable Prototype

A minimal viable prototype (MVP) for this solution will include:

1. **Password Authentication Implementation**: Implement the basic password authentication flow that authenticates users in the pgwire and HTTP protocol layers.
2. **Password Management**: Add password management via role create/alter.
3. **Secure Password Storage**: Implement hashed salted password storage using compatible with `scram-sha-256`.
4. **Environment Variables for System Roles **: Create new flags to set passwords for system roles (`mz_system` and `mz_support`). Update orchestratord to pass in these flags as environment variables from k8s secrets.
5. **HTTP Session Management**: Environmentd's HTTP layer needs to handle session management via cookies and internal session storage.

## Alternatives

1. **Authd Service**: This would involve creating a separate authentication service to handle authentication. This approach is more complex and introduces security concerns, as it would require developing secure endpoints and integrating a user management system that could be prone to additional complexity and security risks.

    - **Reasons Not Chosen**: This solution is more involved, requiring significant additional work to secure, develop, and maintain. Additionally, it does not align with the Postgres.

2. **Password Authentication in Ingress Layer**: Using a front-end proxy like PGbouncer or NGINX to handle password authentication before traffic hits Materialize.

    - **Reasons Not Chosen**: This approach requires significant customization and doesn't solve the problem within the Materialize database itself. It also still necessitates internal authentication for role management.

3. **Store Passwords in K8s Secrets**: It would be viable to store passwords in Kubernetes secrets.

-   **Reasons Not Chosen**: This may create an untenable number of Kubernetes secrets.

4. **App Passwords**: Rather than allowing users to configure a password per user we could generate "app-passwords" that have a many-to-one relationship with users, are auto-generated, and stored in k8s secrets.

-   **Reasons Not Chosen**: This is slightly more complicated and non-compliant with Postgres.

-   **Reasons I still might prefer this**: It does have a number of benefits, including slightly better security via enforcement of random strong passwords, and a better rotation story. It's also possible we could have different app password protocols/versions that allow users to manage their passwords entirely through secrets directly or something like Hashicorp vault.

## Open questions

1. **Where exactly are passwords stored**: While it makes sense to store passwords in the catalog, I have not yet identified exactly where they should be stored. Should it go alongside `mz_roles`, or in a new `mz_auth_id` table. The latter makes sense as it could store password metadata, and might let us store multiple passwords down the road.

2. **Audit Logging and Brute Force Detection**: Do we need to log all failed password attempts and the IPs of those attempts? Do we need to take action to lock an IP or User when an attempt threshold is met?

3. **Password Strength Validation**: Should Materialize include built-in password strength validation, or should it be handled outside the system (e.g., via Kubernetes or other security tools)?
