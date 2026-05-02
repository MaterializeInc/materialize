---
source: src/expr/src/id.rs
revision: 4267863081
---

# mz-expr::id

Defines the identifier types used throughout the expression IR.
`Id` is an enum that is either `Local(LocalId)` — a `u64`-wrapped identifier for a `Let`-binding within a dataflow — or `Global(GlobalId)` referencing a catalog object.
`SourceInstanceId` pairs a `GlobalId` with a `dataflow_id` to uniquely identify a source instantiation within a specific timely dataflow.
