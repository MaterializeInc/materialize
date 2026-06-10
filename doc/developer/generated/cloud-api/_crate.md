---
source: src/cloud-api/src/lib.rs
revision: 6c50c23bea
---

# mz-cloud-api

HTTP client for the Materialize Cloud API.
Authenticates via an `mz_frontegg_client::Client` (token management is delegated entirely to that crate).
Exposes three modules: `config` (client builder and endpoint configuration), `client` (typed methods for cloud regions and customer regions), and `error` (error types).
The top-level usage pattern is: build a `ClientConfig` with a Frontegg client, call `ClientBuilder::default().build(config)`, then use `list_cloud_regions`, `get_region`, `create_region`, or `delete_region`.
