---
source: src/testdrive/src/action/kafka/verify_commit.rs
revision: 2a6ac3ab4c
---

# testdrive::action::kafka::verify_commit

Implements the `kafka-verify-commit` builtin command, which checks that a consumer group has committed a specific offset for a given topic and partition.
Retries with exponential backoff until the committed offset matches the expected value or the timeout expires.
