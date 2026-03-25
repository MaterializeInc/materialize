---
source: src/adapter/src/optimize/dataflows.rs
revision: 2c413b395c
---

# adapter::optimize::dataflows

Provides `DataflowBuilder` and supporting types for assembling `DataflowDescription`s from the catalog.
`DataflowBuilder` traverses catalog entries recursively to import source collections, views, and index arrangements; `ComputeInstanceSnapshot` captures a point-in-time view of a compute instance's installed collections for use during optimization.
`ExprPrep` and its variants (`ExprPrepOneShot`, `ExprPrepWebhookValidation`) implement expression preparation — inlining literal values for unmaterializable functions (`now()`, `mz_logical_timestamp()`) — before expressions enter the dataflow layer.
