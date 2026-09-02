---
source: src/aws-glue-schema-registry/src/config.rs
revision: a8a4d88813
---

# mz-aws-glue-schema-registry::config

Builder for `Client`.

`ClientConfig` holds an `aws_types::SdkConfig` (which carries region, endpoint override, and credentials from the user's `AWS CONNECTION`). `build()` constructs a `Client` infallibly; the underlying `aws_sdk_glue::Client` validates lazily on first request. No Materialize-specific config beyond `SdkConfig` is required.
