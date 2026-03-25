---
source: src/testdrive/src/action/kafka.rs
revision: 20fcb12ed3
---

# testdrive::action::kafka

Groups the eight Kafka builtin commands under a single module and re-exports their entry points.
Children handle topic lifecycle (create, delete, add partitions, wait), data ingestion, data verification, offset-commit verification, and topic configuration verification.
