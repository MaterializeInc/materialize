---
source: src/adapter-types/src/connection.rs
revision: 6d8ef9fb92
---

# mz-adapter-types::connection

Defines `ConnectionId` as a `u32`-backed `IdHandle` (using `IdAllocatorInnerBitSet`) and its underlying `ConnectionIdType = u32` alias.
The `u32` type was chosen for PostgreSQL wire-protocol compatibility.
This module is a thin type definition; allocation and lifecycle are managed by `mz-ore`'s `id_gen` machinery.
