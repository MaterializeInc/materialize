---
source: src/adapter/src/config/params.rs
revision: cb7bca9662
---

# adapter::config::params

Defines `SynchronizedParameters`, a wrapper around `SystemVars` that tracks the subset of variables marked as synchronized (those returned by `SystemVars::iter_synced`) and which of them have been modified by the frontend since the last push.
`ModifiedParameter` pairs a variable name with its new value string and is the unit of change communicated from the frontend to the backend.
