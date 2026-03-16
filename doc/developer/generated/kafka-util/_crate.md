---
source: src/kafka-util/src/lib.rs
revision: b0af9af12c
---

# mz-kafka-util

Utility library for interacting with Kafka, providing topic management, client context abstractions, and AWS IAM authentication.
`admin` wraps rdkafka's admin API with retry-safe topic creation/deletion and config reconciliation; `client` provides `MzClientContext` and `TunnelingClientContext` for logging, error capture, SSH tunneling, and OAuth token generation; `aws` generates MSK IAM auth tokens via SigV4 signing.
The `bin/kgen` binary (not documented here) is a test data generator.
Key dependencies are `rdkafka`, `mz-ssh-util`, and the AWS SDK crates; consumers include Kafka source and sink operators throughout the storage layer.
