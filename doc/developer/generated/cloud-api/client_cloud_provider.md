---
source: src/cloud-api/src/client/cloud_provider.rs
revision: c51f9c3f8c
---

# mz-cloud-api::client::cloud_provider

Defines `CloudProvider` (deserialized from the cloud sync API: `id`, `name`, `url`, `cloud_provider`) and `CloudProviderRegion` (an enum covering `aws/us-east-1`, `aws/eu-west-1`, `aws/us-west-2`).
`CloudProvider::as_cloud_provider_region` and `CloudProviderRegion::from_cloud_provider` convert between the two types by matching on the `id` string.
`CloudProviderRegion` implements `FromStr` and `Display` using the canonical `<provider>/<region>` format.
`Client::list_cloud_regions` paginates `GET /api/cloud-regions` (limit 50, cursor-based) and collects all `CloudProvider` results.
