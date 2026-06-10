---
source: src/aws-secrets-controller/src/lib.rs
revision: e757b4d11b
---

# mz-aws-secrets-controller

Implements the `SecretsController` and `SecretsReader` traits backed by AWS Secrets Manager.
`AwsSecretsController` creates, updates, and deletes secrets keyed by `CatalogItemId`, encrypting them with a KMS key alias and tagging them for namespace isolation.
`AwsSecretsClient` handles reads, resolving catalog IDs to secret names via a configurable prefix.
The controller uses tag-based filtering when listing secrets to avoid returning entries from other environments.
