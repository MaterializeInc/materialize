---
source: src/kafka-util/src/aws.rs
revision: e757b4d11b
---

# mz-kafka-util::aws

Generates AWS IAM authentication tokens for MSK (Managed Streaming for Kafka) using AWS SigV4 request signing.
`generate_auth_token` derives the Kafka endpoint URL from the SDK region, signs a presigned GET request to `kafka-cluster:Connect`, and returns a base64-encoded URL plus its expiration time in milliseconds.
This module is derived from the open-source `aws-msk-iam-sasl-signer-rs` crate and is used by `TunnelingClientContext`'s `generate_oauth_token` callback for OAUTHBEARER authentication.
