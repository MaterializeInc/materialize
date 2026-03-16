---
source: src/ore/src/id_gen.rs
revision: 4267863081
---

# mz-ore::id_gen

Provides ID generation and allocation utilities used throughout Materialize.
The module offers three strategies: `Gen<Id>` (single-threaded monotonic counter), `AtomicGen<Id>` (lock-free atomic counter for multi-threaded use), and `IdAllocator<A>` (ranged allocator backed by a `BitSet` that randomly distributes IDs and reclaims them via reference-counted `IdHandle`s when all handles are dropped).
Also exposes helpers for encoding organization UUIDs into connection IDs (`org_id_conn_bits`, `conn_id_org_uuid`) and generating short random temporary IDs (`temp_id`).
