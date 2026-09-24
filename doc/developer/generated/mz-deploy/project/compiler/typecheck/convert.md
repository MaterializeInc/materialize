---
source: src/mz-deploy/src/project/compiler/typecheck/convert.rs
revision: c8a2857de2
---

# mz-deploy::project::compiler::typecheck::convert

Conversions between project AST/types and the in-memory catalog's SQL form.

`create_stub_statements` generates the ordered sequence of SQL statements that restore a cached dependency as a relation in the in-memory catalog. For dependencies with composite columns (`DataType::Record`, `DataType::List`, `DataType::Map`, `DataType::Array`), the `stub` module produces helper type definitions that must be created before the stub relation itself. Column types are derived from `DataType` values rather than bare SQL strings, so structural types are represented faithfully rather than as pseudo-type tokens.
