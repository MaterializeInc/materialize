---
source: src/storage-types/src/connections/aws.rs
revision: aa87532765
---

# storage-types::connections::aws

Defines `AwsConnection` and `AwsAuth` for configuring authentication and region/endpoint settings for AWS services used by sources and sinks.
`AwsAuth` distinguishes between assume-role authentication and static key credentials.
Implements `AlterCompatible` with fully permissive alter semantics, meaning all fields of an AWS connection can be changed.
Provides `load_sdk_config` to materialise a live `SdkConfig` from the connection details at runtime.
