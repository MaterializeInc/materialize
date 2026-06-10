---
source: src/frontegg-mock/src/handlers/scim.rs
revision: e757b4d11b
---

# frontegg-mock::handlers::scim

Implements SCIM 2.0 configuration handlers: list all configurations, create a new configuration (generating a random bearer token and associating the caller's tenant ID from the JWT), and delete a configuration by ID.
