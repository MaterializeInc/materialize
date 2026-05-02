---
source: src/pgrepr/src/value/jsonb.rs
revision: e757b4d11b
---

# mz-pgrepr::value::jsonb

Provides `Jsonb`, a newtype wrapper around `mz_repr::adt::jsonb::Jsonb` that implements `ToSql` and `FromSql` for the PostgreSQL binary JSONB format.
Serialization prefixes the JSON payload with a version byte (0x01) as required by the PostgreSQL JSONB binary protocol; deserialization validates that prefix before decoding.
