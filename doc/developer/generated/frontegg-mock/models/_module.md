---
source: src/frontegg-mock/src/models.rs
revision: 8041e666f1
---

# frontegg-mock::models

Re-exports all model types from five submodules: `token` (authentication and API token request/response types), `user` (user configuration and CRUD types), `group` (group, role, and membership types), `scim` (SCIM 2.0 configuration types), and `sso` (SSO configuration, domain, and group-mapping types).
Together these types constitute the complete data model for the Frontegg mock server's in-memory state and HTTP API surface.
