---
source: src/cloud-provider/src/lib.rs
revision: 6916f9d270
---

# mz-cloud-provider

Defines the `CloudProvider` enum, which identifies the deployment environment Materialize is running in.
Variants cover real cloud providers (AWS, GCP, Azure, Generic) as well as pseudo-providers used in development and testing (Local, Docker, MzCompose, Cloudtest).
The `is_cloud` method distinguishes actual cloud deployments from local environments.
Implements `Display` and `FromStr` for string serialization and configuration parsing.
