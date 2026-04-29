---
source: src/adapter/src/optimize/dataflows.rs
revision: 9d0a7c3c6f
---

# adapter::optimize::dataflows

Provides `DataflowBuilder` and supporting types for assembling `DataflowDescription`s from the catalog.
`DataflowBuilder` traverses catalog entries recursively to import source collections, views, and index arrangements; `ComputeInstanceSnapshot` captures a point-in-time view of a compute instance's installed collections for use during optimization.
`ExprPrep` and its variants (`ExprPrepOneShot`, `ExprPrepMaintained`, `ExprPrepNoop`, `ExprPrepWebhookValidation`) implement expression preparation — inlining literal values for unmaterializable functions — before expressions enter the dataflow layer.
`EvalTime` controls how `mz_now()` is handled during expression preparation: `Time(ts)` substitutes a concrete timestamp, and `NotAvailable` produces an error.
