---
source: src/frontegg-client/src/lib.rs
revision: 30d929249e
---

# frontegg-client

Provides an async HTTP client for the Frontegg Admin API, supporting user, role, and app-password management.
The crate is organized into three public modules — `client` (the `Client` struct with authentication and API methods), `config` (`ClientBuilder`/`ClientConfig`), and `error` (`Error`/`ApiError`) — plus an internal `parse` module for paginated responses.
Key dependencies are `mz-frontegg-auth` (for `AppPassword` and `Claims`), `reqwest`, and `jsonwebtoken`.
Downstream consumers include `mz` (the CLI) and `cloud-api`.
