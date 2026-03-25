---
source: src/storage-types/src/time_dependence.rs
revision: 4267863081
---

# storage-types::time_dependence

Defines `TimeDependence`, a recursive description of how a dataflow's output frontier relates to wall-clock time, supporting both direct wall-clock dependence and refresh-schedule-based rounding.
`merge` combines dependencies from multiple inputs, with wall-clock dominance and deduplication rules.
`apply` resolves a concrete wall-clock timestamp to the expected output timestamp, taking the minimum over inner dependencies and then rounding up per the refresh schedule.
Also defines `TimeDependenceError` for missing instances or collections when computing dependence.
