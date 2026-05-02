---
source: src/ore-proc/src/instrument.rs
revision: 0ac4a9a3f1
---

# ore-proc::instrument

Implements the `#[instrument]` procedural macro attribute, a safety-hardened wrapper around `tracing::instrument`.
The implementation unconditionally prepends `skip_all` to the attribute arguments and rejects any explicit `skip` argument, forcing callers to use `fields(...)` to opt individual values into tracing spans.
This prevents large or sensitive values (e.g., the entire Catalog) from being accidentally included in trace output.
