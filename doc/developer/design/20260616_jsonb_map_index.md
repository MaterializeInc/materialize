# Faster `jsonb` field access via an in-map index

## Summary

Accessing a field of a `jsonb` value scans the underlying `DatumMap` linearly.
A "JSON to columns" query that pulls `k` fields out of an object with `n` keys
therefore does `O(n * k)` work per row, re-scanning the map once per field.

This change adds a small, deterministic **index** to the in-memory `Row`
encoding of maps so a single key can be found with a binary search. Field
access drops to `O(log n)` and the whole "JSON to columns" pattern from
`O(n * k)` to `O(k * log n)` per row, transparently for every existing
`->`, `->>`, `#>`, and `#>>` expression — no planner or SQL changes.

## Background

`Datum::Map` is a thin view over a byte slice in the `Row` (`src/repr/src/row.rs`).
The bytes are a flat, **key-sorted** sequence of `(key, value)` datum pairs:

```text
[ Tag::Dict ][ u64 byte-length ][ key0 ][ val0 ][ key1 ][ val1 ] ..
```

Decoding a `Datum::Map` is `O(1)` — it just wraps the slice — but the only way
to find a key is `DatumMap::iter().find(..)`, a linear scan. The keys are
already sorted (`adt::jsonb` sorts them at pack time), yet nothing exploited
that, because the variable-length entries gave no way to jump to the middle of
the sequence.

## Goals

* Sub-linear single-key access on `jsonb` objects.
* No on-disk migration, no SQL-visible behavior change.
* Preserve the invariants the rest of the system relies on.

## The two ideas considered

**1. Multi-valued scalar functions** — a `jsonb_access` that takes a list of
field paths and returns many columns in one decode (`O(n)` per row). This is the
asymptotically best option for "all fields", but `MirScalarExpr` is
single-`Datum`-out by construction; producing multiple columns needs new
expression/relation infrastructure (a multi-output operator or an optimizer
transform that fuses sibling accesses) and only helps queries rewritten to use
it.

**2. An index in the `Row`/`Datum`** — store enough information in the map
encoding to binary search by key (`O(log n)` per access). Contained to
`src/repr`, and it speeds up **every** existing field access with zero query
changes.

We chose **idea 2**. It is the smaller, lower-risk change and delivers the win
transparently. Idea 1 remains a viable future step for the extreme
"extract every field" case (where `k ≈ n` makes a single `O(n)` decode beat
`O(k log n)`); the two are complementary.

## Why idea 2 is safe

The in-memory `Tag`-based encoding is constrained by exactly three properties,
all of which this change respects:

1. **It is not persisted.** Durable storage uses `ProtoRow` (protobuf) and a
   separate Arrow/Parquet columnar encoding (`src/repr/src/row/encode.rs`); the
   `Tag` byte layout never reaches S3. So there is **no migration** and no
   format-version concern.
2. **`Row` sort order is implementation-defined.** `RowRef::cmp` compares raw
   bytes purely as an arbitrary-but-consistent total order; no correctness
   depends on map bytes sorting in any particular logical order. Changing the
   map layout is therefore free of ordering-semantics fallout.
3. **`Row` equality is byte equality.** This is the one hard constraint: two
   equal map values *must* encode to identical bytes. The index is a pure,
   deterministic function of the (already sorted) entries, so equal maps still
   produce equal bytes.

## Design

### Encoding

For a map with `n > 0` entries the payload (the bytes counted by the existing
`u64` length) becomes:

```text
[ entries.. ][ offset_1: W ] .. [ offset_{n-1}: W ][ count_word: u32 ]
```

* Entries are unchanged: `(key, value)` datum pairs sorted ascending by key.
* `offset_i` is the start of entry `i` relative to the first entry. Entry 0 is
  always at offset 0 and is omitted.
* `count_word` packs `n` in its low 30 bits and the offset-width class in its
  top two bits.

Offsets are stored at width `W` ∈ {1, 2, 4} bytes, the smallest that holds any
offset, selected from the entries' total byte length (`≤256 → u8`, `≤65536 →
u16`, else `u32`). `W` is a deterministic function of the entries' bytes, so
equal maps select the same width and stay byte-identical. The index is a
**suffix** of `W * (n - 1) + 4` bytes, so the entries occupy the first
`len - 4 - W * (n - 1)` bytes. Empty maps keep an **empty** payload (no
suffix), so they stay byte-identical to `DatumMap::empty()` and the encoding of
every value remains canonical.

The index is built once, when a map is packed, as a suffix that is **appended**
rather than spliced in front (no memmove). There are two builders:

* `RowPacker::push_dict_with` (`finish_dict`) handles the general closure that
  pushes arbitrary datums. It cannot know entry boundaries up front, so it walks
  the just-written entries once to recover them — an `O(n)` re-pass.
* `RowPacker::push_indexed_dict_with` (`DictBuilder` + `finish_dict_from_offsets`)
  is used by the callers that already iterate entry-by-entry: JSON packing
  (`jsonb::pack_dict`) and the columnar/proto decode paths. Each `push_entry`
  records the entry's start offset as it writes it, so the suffix is built with
  **no re-walk**. Offsets accumulate in an inline `SmallVec` (no heap allocation
  for typical objects). The two builders emit byte-identical suffixes, so the map
  encoding stays canonical regardless of which path produced it.

The re-walk matters because Jsonb persists as JSON **text** (Arrow `Utf8`), and
every decode out of persist re-parses it through `jsonb::pack_dict`; routing that
path through the push-time builder removes the redundant pass entirely (see
Performance). Pushing an existing `Datum::Map` copies its bytes verbatim, so the
index is never rebuilt or duplicated.

`DatumMap::iter()` skips the header (so the columnar encoder, proto conversion,
equality, hashing, and ordering — all iter-based — are unaffected), and the new
`DatumMap::get()` binary searches the header.

### Limits

The index encoding caps a single map at:

* **2³⁰ − 1 (~1.07 billion) entries**, since the count occupies 30 bits of the
  count word (the other two are the width class).
* **4 GiB of entry bytes**, since offsets are at most `u32`.

Exceeding either panics during packing. Both are far above any practical `jsonb`
value, and other limits (message size, memory, persist batch size) bind long
before. Before this change the `Tag` encoding's `u64` dict-length prefix allowed
(impractically) larger maps; the index makes these bounds explicit. These are
in-memory-`Row` limits only and are not persisted.

### Why not a new `Tag` or a runtime feature flag?

A second tag (`DictIndexed` alongside `Dict`) would let indexed and legacy maps
coexist — but coexistence is exactly what byte-equality forbids: if the same
logical map could be emitted under either tag, two equal maps could differ in
bytes and break dedup/join/arrangement keys. To stay canonical you would have to
stop emitting plain `Dict` anyway, so the second tag buys nothing over changing
`Dict` in place (and costs a discriminant plus dual read paths everywhere). The
`Tag` `u8` space is not the constraint (~94 of 256 used); canonicalness is.

For the same reason, a flag that *toggles the encoding* at runtime is unsafe:
while both encodings are live in arrangements, equal maps have unequal bytes. If
a kill-switch is ever wanted, the safe form gates only the **read** path
(`get` falling back to a linear `iter().find`) — the index is always written and
always canonical, so flipping the switch can never affect correctness, only
which lookup algorithm runs. We did not add one: the change is correctness-safe,
and threading dynamic config into these hot, stateless `Datum` accessors is
awkward. Reverting is a redeploy.

## Performance

* Single access: `O(n)` → `O(log n)`.
* "JSON to columns" (`k` fields): `O(n * k)` → `O(k * log n)` per row.
* Cost: a `W * (n - 1) + 4`-byte suffix per non-empty map (memory only;
  appended, no memmove), where `W` is 1, 2, or 4 bytes. A small object's index
  is `n - 1 + 4` bytes; a 50-key object near 1 KB uses `u16` offsets (~100 bytes
  vs ~200 with fixed `u32`). On the hot decode/JSON-pack paths the index is built
  without a re-walk and without a heap allocation for typical objects.

The `JsonbToColumns` Feature Benchmark scenario
(`misc/python/materialize/feature_benchmark/scenarios/benchmark_main.py`) reads
50 fields back out of a 50-key object per row and aggregates, exercising exactly
this path. The `jsonb_to_columns` group in `src/repr/benches/row.rs` isolates the
same workload at the `repr` layer (decode, indexed access, and a linear-scan
baseline). On a 50-key object × 10k rows it measures indexed access at ~7× the
linear scan, and decode + access at ~3× the pre-index path; the push-time builder
removes the decode re-walk (~13% off decode alone). Note the decode cost is
dominated by re-parsing the JSON text, not by field access — so the index's win
is largest when access dominates (large objects, many lookups).

## Follow-ups

* The generic `finish_dict` (closure-based `push_dict_with`) still re-walks; if a
  hot write path is found to use it, give it a push-time builder too.
* Idea 1 (multi-output `jsonb_access`) for the `k ≈ n` "explode everything"
  case, where a single `O(n)` decode wins.
* The same index trick could extend to list element access if profiling shows
  it matters.
