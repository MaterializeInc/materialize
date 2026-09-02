---
source: src/clusterd-test-driver/src/dataflow.rs
revision: 5b1466a2b6
---

# mz-clusterd-test-driver::dataflow

Assembly of compute `DataflowDescription`s for the headless test driver.

`DataflowBuilder` is the generic boundary between tests and dataflow assembly. A test describes its dataflow in terms of persist imports, index imports, MIR objects, and index/sink exports; the builder handles MIR-to-LIR lowering via `LirRelationExpr::finalize_dataflow`, `RenderPlan` conversion, `CollectionMetadata` attachment, and `SqlRelationType`/`ReprRelationType` bookkeeping, producing a `DataflowDescription<RenderPlan, CollectionMetadata>` ready to ship as `ComputeCommand::CreateDataflow`.

By default the builder lowers the caller's MIR faithfully without optimization. The `optimize` method enables the MIR dataflow optimizer (`mz_transform::optimize_dataflow`) before lowering, which is required for plans containing a `Join` whose `implementation` is `Unimplemented`. When optimizing, the builder supplies an `ImportedIndexOracle` built from the dataflow's own `index_imports` so the optimizer recognizes imported arrangements.

`DataflowBuilder::explain` renders the lowered LIR plan as `EXPLAIN PHYSICAL PLAN`-style text using a `DummyHumanizer`, without submitting a dataflow. It honors the `optimize` flag identically to `finish`, so the explained plan matches what would be shipped. The `ExplainConfig` has `redacted` pinned to `false` rather than derived from `ExplainConfig::default()`: `default()` derives `redacted` from the build's soft-assertion setting, which anonymizes literals in the release-profile binary and prints them verbatim in debug builds. Pinning `false` makes the golden output independent of the build profile.

`index_dataflow` is sugar over `DataflowBuilder` for the common single-index shape.

`count_over_index` constructs a count-aggregate MIR over an index import, for simple correctness checks.

`PersistSource` and `PersistSink` are helper structs describing a persist-backed source import and a sink export respectively.

`Input` is a handle to an imported or built collection, carrying its `GlobalId` and `ReprRelationType`, used to construct typed `Get` nodes without hand-rolling the type.

`ImportedIndexOracle` is a private `IndexOracle` implementation built from a dataflow's `index_imports`, exposing only the arrangements the dataflow itself imports. It is passed to the MIR optimizer when `optimize` is enabled.

The private `augment` function converts a lowered `DataflowDescription<LirRelationExpr, ()>` into `DataflowDescription<RenderPlan, CollectionMetadata>` by flattening each object's plan via `RenderPlan::try_from` and splicing `CollectionMetadata` into source and materialized-view sink entries, mirroring what `compute-client`'s `Instance::create_dataflow` does.
