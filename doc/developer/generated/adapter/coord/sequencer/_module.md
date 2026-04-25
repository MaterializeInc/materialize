---
source: src/adapter/src/coord/sequencer.rs
revision: 618f264dfb
---

# adapter::coord::sequencer

The sequencer module bridges planned SQL statements and coordinator execution.
`sequencer.rs` dispatches each `Plan` variant to the right `sequence_*` method and provides shared utilities; the `inner` sub-module tree implements every individual statement type.
Together these modules cover the entire SQL execution surface from post-planning to `ExecuteResponse`.
