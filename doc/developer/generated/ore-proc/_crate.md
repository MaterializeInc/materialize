---
source: src/ore-proc/src/lib.rs
revision: 6dc0b37208
---

# ore-proc

Provides internal procedural macros for the Materialize codebase, split from `mz-ore` because `proc-macro` crates may only export procedural macros.

## Macros

* `#[static_list]` — collects all `pub static` items of a given type from a module tree into a single static slice, with a compile-time count check.
* `#[instrument]` — wraps `tracing::instrument` with `skip_all` enforced by default, preventing accidental tracing of large arguments.
* `#[test]` — wraps test functions to initialize Materialize's logging infrastructure before the test body runs.

## Module structure

| Module | Purpose |
|--------|---------|
| `static_list` | Implementation of the `#[static_list]` attribute macro |
| `instrument` | Implementation of the `#[instrument]` attribute macro |
| `test` | Implementation of the `#[test]` attribute macro |

## Dependencies

Depends on `proc-macro2`, `quote`, and `syn` (with `full` and `extra-traits` features).
The crate is consumed by `mz-ore` and any other Materialize crate that uses `#[mz_ore::instrument]`, `#[mz_ore::test]`, or `#[mz_ore_proc::static_list]`.
