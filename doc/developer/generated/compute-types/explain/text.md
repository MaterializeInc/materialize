---
source: src/compute-types/src/explain/text.rs
revision: 52002e00c9
---

# compute-types::explain::text

Implements `DisplayText` for `Plan` nodes, rendering the LIR plan tree in the `EXPLAIN AS TEXT` format.
Each `Plan` variant maps to an uppercase header line; sub-plan details follow as indented `key=value` pairs or lowercase field lines.
Supports both a default and a `verbose_syntax` mode; the latter is kept in sync with the `Display` implementation for `Plan`.
Delegates join, reduce, threshold, and top-k sub-plan formatting to helper impls defined in the same file.
