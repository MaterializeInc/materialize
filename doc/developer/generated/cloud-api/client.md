---
source: src/cloud-api/src/client.rs
revision: e757b4d11b
---

# mz-cloud-api::client

Defines the `Client` struct (holds a `reqwest::Client`, an `Arc<mz_frontegg_client::Client>` for auth, and the base endpoint `Url`) and the internal request-building machinery.
`build_global_request` targets the client's own endpoint; `build_region_request` targets a `CloudProvider`'s Region API URL and optionally attaches the `X-Materialize-Api-Version` header.
`send_request` sends the built request, treats HTTP 204 as `Error::SuccesfullButNoContent`, deserialises JSON success responses, and maps error bodies to `ApiError`.
This module re-exports the `cloud_provider` and `region` submodules.
