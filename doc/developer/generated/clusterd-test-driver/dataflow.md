---
source: src/clusterd-test-driver/src/dataflow.rs
revision: 6d4c0fbb2b
---

# mz-clusterd-test-driver::dataflow

Assembly of compute `DataflowDescription`s for the headless test driver.

`DataflowBuilder` is the generic boundary between tests and dataflow assembly. A test describes its dataflow in terms of persist imports, MIR objects, and index exports; the builder handles MIR-to-LIR lowering, `RenderPlan` conversion, `CollectionMetadata` attachment, and `SqlRelationType`/`ReprRelationType` bookkeeping, producing a `DataflowDescription<RenderPlan, CollectionMetadata>` ready to ship as `ComputeCommand::CreateDataflow`.

`index_dataflow` is sugar over `DataflowBuilder` for the common single-index shape.

`PersistSource` and `PersistSink` are helper structs describing a persist-backed source import and a sink export respectively.

`count_over_index` constructs a count-aggregate MIR over an index import, for simple correctness checks.
