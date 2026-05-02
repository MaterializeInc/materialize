---
source: src/frontegg-mock/src/handlers/user.rs
revision: e757b4d11b
---

# frontegg-mock::handlers::user

Implements user and API token management handlers covering the full Frontegg user API surface: get/create/delete user, list/create/delete user and tenant API tokens, get user profile, get users v3 (with filtering, sorting, pagination), update user roles, list roles, add/remove users from groups, and an internal endpoint for retrieving a user's plaintext password (used in tests).
JWT claims are validated on protected endpoints by decoding the Bearer token against the server's decoding key.
