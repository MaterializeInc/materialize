---
source: src/frontegg-client/src/config.rs
revision: 2d8c8533cd
---

# frontegg-client::config

Defines `ClientConfig` (required parameters: authentication method) and `ClientBuilder` (optional endpoint override) for constructing a `Client`.
The default endpoint points to `https://admin.cloud.materialize.com`; `ClientBuilder::build` assembles the underlying `reqwest::Client` with a 60-second timeout and no redirects.
