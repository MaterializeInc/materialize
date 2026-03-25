---
source: src/mz/src/ui.rs
revision: 1ad121620a
---

# mz::ui

Provides terminal output utilities.
`OutputFormatter` supports three output formats (`Text`, `Json`, `Csv`) and methods `output_scalar`, `output_table`, `output_warning`, `print_with_color`, and `loading_spinner`.
`OptionalStr` is a newtype that renders `None` as `<unset>` in both human-readable and serialized output.
