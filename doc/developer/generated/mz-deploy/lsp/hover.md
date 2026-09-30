---
source: src/mz-deploy/src/lsp/hover.rs
revision: c8a2857de2
---

# mz-deploy::lsp::hover

Hover information for SQL identifiers and variable references.

`resolve_hover` looks up the identifier against the `ProjectCache` (SQLite) and `Types` (types.lock), then formats the result as a Markdown table with columns for name, type, and nullable. Column types are rendered via `humanized_type` so that structural types such as records and lists appear with their field details rather than pseudo-type tokens.

`resolve_variable_hover` returns a tooltip for psql-style variable references (`:name`, `:'name'`, `:"name"`) showing the variable's resolved value and the active profile.

When an identifier resolves to a built-in function rather than a project object, `resolve_hover` falls back to the function registry for a signature listing.
