---
source: src/testdrive/src/action/kafka/delete_records.rs
revision: e757b4d11b
---

# testdrive::action::kafka::delete_records

Implements the `kafka-delete-records` builtin command, which advances the low watermark of a topic partition by deleting records up to a specified offset.
Uses the rdkafka admin client to issue the delete-records request.
