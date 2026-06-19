---
source: src/metrics-catalog/src/main.rs
revision: 82dac06732
---

# mz-metrics-catalog

CLI tool that generates a YAML catalog of every `metric!` definition in the Materialize source tree, used to produce user-facing metrics documentation. Invoke via `bin/gen-metrics-catalog`.

The tool accepts two positional arguments: `<src-root>` (the directory to walk) and `<out-file>` (the YAML output path).

It walks the Rust AST with `syn`, visiting every `metric!` invocation outside of test code (`cfg(test)`, `#[test]`, `#[mz_ore::test]`, etc.). For each invocation it extracts `name`, `help`, `subsystem`, and `visibility`. A literal `subsystem` is prepended to the name (`<subsystem>_<name>`); a runtime `subsystem` expression globs to `*`. A `format!(..)` in a `name` or `help` position has its `{}` placeholders replaced with `*`. `metric!` invocations nested inside `vec![..]` are also discovered by re-parsing the `vec!` body.

Two additional metric sources are appended: metrics registered via wrapping macros in `mz_metrics` (via `mz_metrics::describe_metrics`) and Tokio runtime metrics (via `mz_ore::metrics::describe_runtime_metrics`), both marked `Internal`.

The output YAML is sorted by metric name and written with a "DO NOT EDIT" header.
