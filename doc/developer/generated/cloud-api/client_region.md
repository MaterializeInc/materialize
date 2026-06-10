---
source: src/cloud-api/src/client/region.rs
revision: adfa404693
---

# mz-cloud-api::client::region

Defines `Region` (with an optional `RegionInfo` and a `RegionState`), `RegionInfo` (SQL address, HTTP address, DNS resolvability, enabled-at timestamp), and `RegionState` (`Enabled`, `EnablementPending`, `DeletionPending`, `SoftDeleted`).
`Client::get_region` sends `GET /api/region` to the Region API (API version 1) and maps `SoftDeleted`, `DeletionPending`, and HTTP 204 to `Error::EmptyRegion`.
`Client::get_all_regions` calls `list_cloud_regions` and `get_region` for each provider, skipping any that return `Error::EmptyRegion`.
`Client::create_region` sends `PATCH /api/region` with an optional image ref, extra args, and resource allocations, using a 60-second timeout.
`Client::delete_region` sends `DELETE /api/region` (optionally with `hardDelete=true`) and polls until the region reaches `SoftDeleted` or disappears, returning `Error::TimeoutError` after 600 attempts.
