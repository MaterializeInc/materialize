---
source: src/testdrive/src/action/kafka/wait_topic.rs
revision: 2280405a2e
---

# testdrive::action::kafka::wait_topic

Implements the `kafka-wait-topic` builtin command and the internal `check_topic_exists` helper.
`run_wait_topic` polls the Kafka broker until the named topic is visible; `check_topic_exists` is shared with the state-reset logic in `action.rs` to verify topic creation before proceeding.
