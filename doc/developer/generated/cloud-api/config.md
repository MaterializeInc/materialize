---
source: src/cloud-api/src/config.rs
revision: 0e36455b03
---

# mz-cloud-api::config

Defines `ClientConfig` (holds a `mz_frontegg_client::Client` for auth) and `ClientBuilder` (sets the API endpoint, defaulting to `https://api.cloud.materialize.com`).
`ClientBuilder::build` constructs a `reqwest::Client` with no redirects and a 60-second timeout, then assembles the `Client`.
Also declares `API_VERSION_HEADER` (`X-Materialize-Api-Version`) used by region API requests.
