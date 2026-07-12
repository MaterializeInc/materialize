---
source: src/storage-types/src/connections/aws.rs
revision: 1e24476fac
---

# storage-types::connections::aws

Defines `AwsConnection` and `AwsAuth` for configuring authentication and region/endpoint settings for AWS services used by sources and sinks.
`AwsAuth` distinguishes between assume-role authentication and static key credentials.
Implements `AlterCompatible` with fully permissive alter semantics, meaning all fields of an AWS connection can be changed.
Provides `load_sdk_config` to materialise a live `SdkConfig` from the connection details at runtime.
`AwsAssumeRole::prefetch_credentials` accepts a `ConfigSet` and reads `AWS_PREFETCH_STS_CONNECT_TIMEOUT` from it to set the connect timeout on the STS client used by the `AssumeRole` credentials prefetcher; values below 1 second are treated as misconfiguration and replaced with the dyncfg default. The `CredentialPrefetcher` receives the resolved timeout so its backstop interval (`CREDENTIAL_FETCH_BACKSTOP_MULTIPLE` * timeout) scales with it.
