---
source: src/cloud-api/src/error.rs
revision: 1308fdbda2
---

# mz-cloud-api::error

Defines `ApiError` (HTTP status code plus a vector of error messages) and the `Error` enum that covers all failure modes in the crate.
`Error` variants include `Transport` (reqwest network failure), `Api` (server returned an error body), `AdminApi` (Frontegg admin error), `EmptyRegion` (no customer region in the requested cloud region), `CloudProviderRegionParseError`, `InvalidEndpointDomain`, `UrlParseError`, `UrlBaseError`, `TimeoutError`, and `SuccesfullButNoContent` (HTTP 204, used when a region is not yet enabled).
