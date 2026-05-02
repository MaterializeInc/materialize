---
source: src/frontegg-mock/src/handlers/sso.rs
revision: e757b4d11b
---

# frontegg-mock::handlers::sso

Implements the full Frontegg SSO configuration API: CRUD for SSO configs, domains (with base64-encoded public certificates), group mappings, and default roles.
Public certificates are stored base64-encoded and decoded on update; `Domain.txt_record` is generated from random UUIDs to simulate Frontegg's DNS ownership challenge.
