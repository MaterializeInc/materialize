---
source: src/repr/src/adt/system.rs
revision: c0559e3dbe
---

# mz-repr::adt::system

Defines PostgreSQL system type wrappers: `Oid`, `PgLegacyChar` (single-byte `"char"`), `RegClass`, `RegProc`, and `RegType`, all thin newtypes over `u32` or `i8` used in catalog queries.
