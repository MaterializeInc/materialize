---
source: src/repr/src/adt/system.rs
revision: 4267863081
---

# mz-repr::adt::system

Defines PostgreSQL system type wrappers: `Oid`, `PgLegacyChar` (single-byte `"char"`), `RegClass`, `RegProc`, and `RegType`, all thin newtypes over `u32` or `i8` used in catalog queries.
