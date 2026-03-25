---
source: src/metabase/src/lib.rs
revision: 88b6028c79
---

# mz-metabase

Minimal async REST API client for [Metabase](https://metabase.com), covering only the endpoints required by Materialize.
`Client` wraps `reqwest` and exposes `session_properties`, `login`, `setup`, `databases`, and `database_metadata`, adding a 5-second timeout and an optional `X-Metabase-Session` header on each request.
The crate intentionally limits its scope and documentation to avoid duplicating Metabase's upstream API docs.
