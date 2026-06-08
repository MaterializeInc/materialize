---
source: src/adapter/src/optimize/dataflows.rs
revision: 261d61df83
---

# adapter::optimize::dataflows

Provides `DataflowBuilder` and supporting types for assembling `DataflowDescription`s from the catalog.
`DataflowBuilder` traverses catalog entries recursively to import source collections, views, and index arrangements; `ComputeInstanceSnapshot` captures a point-in-time view of a compute instance's installed collections for use during optimization.
`ExprPrep` and its variants (`ExprPrepOneShot`, `ExprPrepMaintained`, `ExprPrepNoop`, `ExprPrepWebhookValidation`) implement expression preparation — inlining literal values for unmaterializable functions — before expressions enter the dataflow layer.
`EvalTime` controls how `mz_now()` is handled during expression preparation: `Time(ts)` substitutes a concrete timestamp, and `NotAvailable` produces an error.
The private `eval_unmaterializable_func` function checks the session's `restrict_to_user_objects` flag before evaluating each unmaterializable function; functions that do not pass `allowed_in_restricted_session()` return `OptimizerError::RestrictedFunction`.
