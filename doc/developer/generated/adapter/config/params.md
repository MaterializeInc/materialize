---
source: src/adapter/src/config/params.rs
revision: 7258dad07f
---

# adapter::config::params

Defines `SynchronizedParameters`, a wrapper around `SystemVars` that tracks the subset of variables marked as synchronized (those returned by `SystemVars::iter_synced`) and which of them have been modified by the frontend since the last push.
`ModifiedParameter` pairs a variable name with its new value string and is the unit of change communicated from the frontend to the backend.
`SynchronizedParameters::canonicalize` parses a raw value through the system var and re-formats it to the canonical encoding, bridging spelling differences between encodings (for example, LaunchDarkly serves booleans as `"true"`/`"false"` while the canonical system-var encoding is `"on"`/`"off"`). This is used during scoped override evaluation to determine whether a scoped value differs from the environment-wide baseline.
`SynchronizedParameters::enable_scoped_system_parameters` reads the `ENABLE_SCOPED_SYSTEM_PARAMETERS` dyncfg from the working copy, allowing the sync loop to gate the scoped reconcile path without taking a catalog snapshot.
