---
source: src/testdrive/src/action/kafka.rs
revision: 5b2cefc829
---

# testdrive::action::kafka

Groups the nine Kafka builtin commands under a single module and re-exports their entry points.
Children handle topic lifecycle (create, delete, add partitions, wait), record deletion, data ingestion, data verification, offset-commit verification, and topic configuration verification.
