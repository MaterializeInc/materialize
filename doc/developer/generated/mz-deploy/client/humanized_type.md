---
source: src/mz-deploy/src/client/humanized_type.rs
revision: c8a2857de2
---

# mz-deploy::client::humanized_type

Parses the humanized type syntax that `pg_typeof` produces for composite and container types.

`pg_typeof` describes an anonymous record as `record(field1: type1, field2: type2?)`, where a trailing `?` marks a nullable field. `parse` turns this back into a `DataType` so `mz-deploy lock` can record the real structural type rather than the lossy pseudo-type token.

Field names are rendered unquoted in the `pg_typeof` output, so any name containing a delimiter character (`,`, `:`, `?`, `(`, `)`, `[`, `]`) is genuinely ambiguous. Such types are rejected with `HumanizedTypeError`, and the caller falls back to the catalog's pseudo-type spelling.
