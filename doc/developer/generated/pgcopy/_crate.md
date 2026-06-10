---
source: src/pgcopy/src/lib.rs
revision: 8517bddff2
---

# mz-pgcopy

Encodes and decodes PostgreSQL COPY data formats (text, CSV, binary) for use by the Materialize query engine.
The crate is a thin re-export wrapper over the `copy` module, which contains all logic.
Depends on `mz-repr`, `mz-pgrepr`, `bytes`, and `csv`; consumed wherever COPY ingestion or output is needed.

## Module structure

* `copy` — encoding/decoding logic and format parameter types
