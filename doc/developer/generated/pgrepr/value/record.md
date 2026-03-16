---
source: src/pgrepr/src/value/record.rs
revision: e757b4d11b
---

# mz-pgrepr::value::record

Provides `Record<T>`, a tuple wrapper that implements `FromSql` for PostgreSQL composite types, working around the absence of such an implementation in the upstream `rust-postgres` crate.
A macro generates `FromSql` for tuples of arity 0–4: it reads the field count from the binary composite header, then decodes each field by its OID into the corresponding tuple element type.
