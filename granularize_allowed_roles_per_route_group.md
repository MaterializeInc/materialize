# Refactor: Make allowed_roles property more granular; per route-group rather than for all routes

## Context/Problem

There exists a security bug where self managed environments with password/oidc auth can have
any non-system user access our internal HTTP routes. These include injecting audit events,
leader promotion, and etc. To elaborate, consider the following listener config we use for
passsword auth:
```jsonc
{
  ...,
  "http": {
    "external": {
      "authenticator_kind": "Password",
      "allowed_roles": "NormalAndInternal",
      "routes": {
        "base": true,
        "internal": true,
        "profiling": true,
        ...
      },
      ...
    }
  }
}
```
The setting `"allowed_roles": "NormalAndInternal",` means normal AND system users can access this route.
Meaning any of the routes under the `internal` group, non system users can request against.
What's worse is authorization checks via `check_role_allowed` are scattered around, making it
easy for bugs to slip past.


Historically, "internal" was a separate `InternalHttpServer` on an isolated port (6878);
network topology *was* the authorization. The "Unify HTTP servers" commit (`a1bf91c156`) split that into
two orthogonal config axes — `routes.internal` (route surface) and `allowed_roles` (listener admission)
— and nothing re-couples them.

## Solution

Give each HTTP route group its own `AllowedRoles` so a single listener can serve the group `base` to
`Normal` roles while restricting groups `internal` and `profiling` to `Internal` roles.

Design decisions:
- **Lower AllowedRoles from HttpListenerConfig to HttpListenerConfig.routes[RouteGroup].AllowedRoles**
  We make the `AllowedRoles` property more granular. If the route group isn't enabled, we omit from the JSON
  (see example below for details). This does imply that the new schema isn't backwards compatible with
  the old one. This is okay however since each `environmentd` only runs its corresponding version's listener
  config, never the previous version's.
- **Authorization is one middleware.** The five scattered `check_role_allowed` calls collapse into a
  single `http_authz` middleware that runs immediately after authentication, reading the `AuthedUser`
  and a per-route `RouteAllowedRoles` extension. Websockets API will still need to call `check_role_allowed`
  separately since middleware doesn't work for Websockets.
- **The listener config is versioned.** We need to maintain backwards compatibility for orchestratord
  since it needs to be able to serve the previous listener config schema for previous versions of Materialize.
- **Versioning uses the unified Materialize version scheme.**
  Makes it easy to reason about which version belongs to which database version.


### Example:

Before:
```jsonc
// Legacy schema (introduced in 0.147.0): one top-level role for all routes.
{
  ...,
  "http": {
    "external": {
      "authenticator_kind": "Password",
      "allowed_roles": "NormalAndInternal",
      "routes": {
        "base": true,
        "internal": true,
        "profiling": false
      },
      ...
    }
  }
}
```
After:
```jsonc
// 26.31.0+ schema: per-group roles; the HTTP listener has no top-level allowed_roles.
{
  ...,
  "version": "26.31.0",
  "http": {
    "external": {
      "routes": {
        // We can scope allowed_roles to "NormalAndInternal" here
        "base": {
          "enabled": true,
          "allowed_roles": "NormalAndInternal"
        },
        // We can scope allowed_roles to "Internal" here
        "internal": {
          "enabled": true,
          "allowed_roles": "Internal"
        },
        // When enabled is false, we omit allowed_roles
        "profiling": {
          "enabled": false
        }
      },
      ...
    }
  }
}
```
