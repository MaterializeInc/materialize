---
source: src/frontegg-mock/src/handlers.rs
revision: 8041e666f1
---

# frontegg-mock::handlers

Re-exports all handler functions from five submodules: `auth` (login, token exchange, refresh), `user` (user and API token CRUD, roles), `group` (group CRUD and membership), `scim` (SCIM 2.0 configuration), and `sso` (SSO configuration, domains, group mappings, default roles).
These functions are directly registered on the axum `Router` in `server.rs`.
