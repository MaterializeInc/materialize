---
source: src/timely-util/src/columnar/body.rs
revision: 241f928bf3
---

# timely-util::columnar::body

`ColumnBody<C>`: the at-rest form of a sorted, consolidated columnar chunk.

`Column<C>` is the container on dataflow edges: it is built by pushing, crosses an exchange as channel bytes, and is read on arrival. `ColumnBody<C>` is what lives behind an edge — sorted, consolidated runs that a merge batcher chains, that a spill-backed chunk keeps on the heap or in the pool, and that a batch builder seals. A body is typed while it is written and serialized when a store hands it back. It never holds channel bytes.

## Variants

- **`Typed(C::Container)`** — the mutable form. Fresh bodies and bodies being written to are in this variant.
- **`Words(Vec<u64>)`** — the serialized form. A `u64`-aligned flat `columnar::bytes::indexed` encoding, as a store hands a body back. This is the only serialized form a body has.

## Key methods

- **`borrow`** — borrows the body as a columnar view. The serialized form rebuilds its view from the encoded header on every call, so callers that read multiple records hoist the result out of their loop.
- **`typed_mut`** — returns a mutable reference to the typed containers, materializing a `Words` body first by bulk per-leaf extension into fresh containers.
- **`into_typed`** — consumes the body and returns owned typed containers, copying a `Words` body if needed.
- **`write_into`** — writes exactly `length_in_bytes()` bytes of the serialized form to a writer; a body that went through a store and back round-trips byte-identically.
- **`duplicate`** — produces an owned copy in the same variant: `Words` bodies are cloned, `Typed` bodies are copied via bulk per-leaf extension.
- **`clear`** — empties the body, retaining a `Typed` body's allocations. A `Words` body becomes an empty `Typed` body.
- **`at_capacity`** — true once the body is at the ship size the builder and merger cut chunks at. A `Words` body is always at capacity.

## Conversions

`ColumnBody<C>` converts from `Column<C>`: typed data moves, channel bytes (`Bytes` variant) are relocated into owned words. It converts back to `Column<C>`: both variants map to their edge-container equivalents (`Typed` stays `Typed`, `Words` becomes `Align`).
