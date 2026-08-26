# Alternatives to binary search for the `DatumMap` in-map index

## Summary

The in-map index added by the `jsonb` field access work stores map entries in
key order and appends a table of entry offsets, so a single key is found by
binary searching the entries and comparing keys as strings. Two obvious
alternatives were raised: build a **hash table** in the suffix instead, or keep
a binary search but **sort the entries by hash** so the comparisons are on
fixed-width integers rather than variable-length strings.

Neither wins. Hashing is slower than binary search over the range of map sizes
that actually occurs, costs about twice the index bytes, and puts a hash
function into the `Row` byte-equality contract. Sorting by hash is faster to
probe but pays for it on the scan paths, which are hotter than lookup, and it
changes an iteration order that is user visible in at least three places and
that the durable Arrow encoding inherits.

What does win is the *instinct* behind the second idea, applied to the key
rather than to a hash: store a few discriminating bytes of each key beside its
offset. The comparisons become integer comparisons on a contiguous array, the
descent never touches the entries region, and iteration order, canonicality,
and the persist encoding are all untouched. Stripping the map's longest common
key prefix before taking those bytes makes the trick robust to the key sets
where it would otherwise degenerate, and choosing the width from the data (one,
two, or four bytes) keeps the cost at one byte per entry for narrow key sets.

Measurements are in [Results](#results). They come from
`doc/developer/design/static/datum_map_index_probe.rs`, a standalone layout
probe, calibrated against the real `Row` encoding by
`src/repr/benches/map_index.rs`. See [Reproducing](#reproducing).

## What constrains the design

Three constraints do most of the work in ruling options out. All three are
easy to miss when thinking about the problem as "which lookup structure is
fastest".

### 1. The operation surface is scan-dominated, not lookup-dominated

Grouping every operation the tree performs on a `DatumMap` by access pattern:

| Access pattern | Operations |
| --- | --- |
| **Single-key lookup** | `->` and `->>` and `#>` and `#>>` and `?` on `jsonb`, then `->` and `?` and `?&` and `?\|` on `map`, then the map arms of both `@>` implementations (`src/expr/src/scalar/func.rs`) |
| **Full scan in key order** | Arrow/persist encode (`src/repr/src/row/encode.rs`), proto encode, pgwire text and binary (`src/pgrepr/src/value.rs`), `jsonb_stringify`, `Display` and `Debug`, `jsonb_each`, `jsonb_object_keys`, `map_length` |
| **Scan then repack** | `jsonb_concat` (`\|\|`), `jsonb_delete_string` (`-`), `map_build`, `MapAgg`, `JsonbObjectAgg`, the `text` to `map` cast, and the Avro, Parquet, proto, and JSON decode paths |
| **Whole value** | `DatumMap`'s `PartialEq`, `Ord`, and `Hash` (all via `iter()`), `Row` and `RowRef` byte equality, `Row::hashed()`, `datum_size` |

Only the first group benefits from an index. The second and third groups are
the ones a persist write, a `SELECT` of a `jsonb` column, or a decode goes
through, and for them the index is dead weight that has to be skipped. So a
layout that speeds up lookup by making scans or packs slower is trading a hot
path for a warm one.

The third group matters more than it looks: every map-producing operation
rebuilds the index for its output row. A layout with an expensive build pays
that cost on each of them.

### 2. `Row` equality is byte equality, so the encoding must be canonical

`RowRef` compares and hashes raw bytes, so two logically equal maps must
encode to identical bytes. The encoding therefore has to be a function of the
logical map value and nothing else. In particular it must not depend on:

* the order entries were pushed in (hence the ascending-key contract),
* which packer built the row (two builders exist, and a test pins that they
  agree byte for byte),
* allocator state, platform, endianness, or pointer values,
* any runtime-tunable setting, which is why an opt-out flag is not available,
* the compiler or library version.

For the sorted-offsets index this is self-evident: the index is a function of
the entries' order and byte lengths, with no constants involved. Every
hash-based layout adds obligations:

* the hash function is frozen for the lifetime of the format, which rules out
  `ahash` (per-process random seeds) and `DefaultHasher` (unspecified, may
  change between Rust releases), leaving something like `seahash` pinned by
  contract,
* the bucket count must be a pure function of `n`,
* collision resolution must be deterministic, so insertion has to happen in a
  canonical order anyway,
* the empty-slot sentinel must be unambiguous, since offset 0 is a legal entry
  offset, so slots store `offset + 1`,
* for a perfect-hash construction, the seed search must be deterministic, must
  take the first success, and needs a deterministic fallback when it fails,
  which makes the attempt cap part of the format contract too.

None of these is hard. Together they mean a hash index turns a property that is
currently obvious by inspection into one maintained by a set of frozen
conventions. The prefix-based layouts below need none of them: a key prefix is
a pure function of the key bytes, the common prefix is a pure function of the
sorted key set, and an adaptive width is a pure function of the key set.

### 3. Iteration order is user visible, and the durable encoding inherits it

This is what rules out sorting entries by hash, independently of performance.

* `jsonb_object_keys` returns keys in ascending order, pinned by a golden with
  the comment `# Keys are sorted.` in `test/sqllogictest/jsonb.slt`.
* The text rendering of a `jsonb` object is key sorted: `jsonb_object_agg`
  over the rows `('b', 2), ('a', 1)` has the golden output `{"a":1,"b":2}`.
* `DatumMap::Ord` is `self.iter().cmp(other.iter())` and `Datum` derives `Ord`,
  so `ORDER BY` on a `jsonb` or `map` column, `MIN`, `MAX`, and any sort or
  join key containing a map all follow iteration order.

On top of that, the Arrow encoder writes map entries in `iter()` order into
`ListArray`s that persist stores, and the code notes that `DatumMap` is always
sorted. Today the in-memory order and the durable order agree, so encode and
decode are copies. Hash-ordering the entries would either change the persisted
byte order, which turns an in-memory-only change into a format migration and
gives up the "no migration" property the index was designed around, or require
a sort on both sides of the persist boundary on every row.

## The design space

Twenty-five layouts were implemented and measured. All of them keep the payload
self-describing, so `iter()` works, and all but `E` and `H` keep entries in
ascending key order.

| | Layout | Suffix beyond the entries | Lookup |
| --- | --- | --- | --- |
| `L` | linear scan (today's `main`) | none | scan entries |
| `A` | **the index as written** | `offsets[1..n]` at width `W` | binary search, string compares |
| `B` | open-addressed hash table | `buckets[2n]` at `W`, holding `offset+1` | hash, probe |
| `C` | fingerprint scan | `A` plus one hash byte per key | SWAR scan 8 bytes at a time, verify |
| `I` | wider fingerprint | `A` plus two hash bytes per key | SWAR scan 4 lanes at a time |
| `D` | minimal perfect hash | `slots[n]` at `W` plus a seed | one hash, one slot |
| `E` | **entries sorted by hash** | `offsets` plus `u32` hash per entry | integer binary search, verify |
| `H` | `E` plus a key-order permutation | `E`'s suffix plus `perm[n]` | `E`'s lookup, iteration via `perm` |
| `F` | hash-sorted side index | `offsets` plus sorted `(h16, entry_index)` | integer binary search, verify |
| `G` | key-prefix index | `offsets` plus a 4-byte big-endian key prefix | integer binary search, full compare on ties |
| `K` | `G` interleaved | `(prefix, offset)` pairs | as `G`, one cache line per probe |
| `M` | sparse prefix index | one prefix per group of 8 | binary search groups, scan the group |
| `N` | **`K` over an LCP-stripped prefix** | `(prefix, offset)` pairs plus the LCP length | as `K` |
| `P` | `N` with a 2-byte prefix | as `N`, narrower | as `N` |
| `T` | `N` with a 1-byte prefix | as `N`, narrowest | as `N` |
| `X` | **`N` with the width chosen from the data** | as `N`, width in {1, 2, 4} | as `N`, width read from the payload |
| `Q` | `N` searched by interpolation | as `N` | interpolate, then bisect |
| `V` | branchless rank over `N`'s prefixes | contiguous prefixes plus offsets | count prefixes below the probe |
| `Y` | branchless bisection over `V`'s array | as `V` | branch-free `partition_point`, verify |
| `Z` | branch-free scan of `X`'s discriminator | contiguous adaptive-width prefixes plus offsets | SWAR scan, at most one candidate |
| `R` | `Z`'s bytes, search chosen from `n` | as `Z` | scan when small, branch-free bisection when large |
| `J` | **`Z`'s bytes, one search** | contiguous adaptive-width prefixes plus offsets | branch-free bisection at every size |
| `O` | keys and values in separate regions | a key-offset and a value-offset array | binary search, string compares, never touching a value |
| `U` | `O` with a linear scan | as `O` | scan the key region only |
| `S` | **the floor** | as `A` | the slot is already known, one offset read and one verify |

Two of these are reference points rather than proposals. `L` is what `main`
does. `S` is the speed of light: it models a caller that resolved the key to a
slot once per map *shape* rather than once per row, which is what JSON
shredding does, and it exists to show how much of the remaining gap an index of
any kind can close.

The big-endian key prefix is what makes `G` and its descendants work: byte
order on a zero-padded big-endian prefix agrees with lexicographic order on the
key, so the prefix array is sorted exactly when the keys are. Keys that tie on
the prefix fall through to a full comparison, and because a `true` is only ever
returned after a full comparison, a probe that ties spuriously cannot produce a
wrong answer.

Stripping the longest common prefix first, as `N` does, is what makes it robust.
Keys are sorted, so the whole map's common prefix is the common prefix of the
first and last key, which is O(1) to find at pack time. Every key shares it, so
dropping it preserves order, and the bytes that remain are the ones that
actually discriminate. A key set like `com.example.service.metrics.dimension_NNN`
defeats a plain 4-byte prefix completely and is handled by an LCP-stripped one.

## Results

### Calibration against the real encoding

`src/repr/benches/map_index.rs` measures the two strategies that exist in the
tree, over a 32 MiB corpus of `Row`s so the maps are not sitting in L2. This is
the authoritative before-and-after for the index itself, and it is what the
model's numbers are read against.

| keys | n | `DatumMap::get` | `iter().find` | scan / index | `push_dict_with` | `push_indexed_dict_with` |
| --- | --- | --- | --- | --- | --- | --- |
| typical | 3 | 30.0 | 68.3 | 2.3x | 116.8 | 72.6 |
| typical | 8 | 41.3 | 146.0 | 3.5x | 276.4 | 191.3 |
| typical | 16 | 48.6 | 270.7 | 5.6x | 544.8 | 347.4 |
| typical | 32 | 62.3 | 516.4 | 8.3x | 1141.6 | 727.2 |
| typical | 50 | 68.1 | 786.2 | 11.6x | 1823.5 | 1106.0 |
| typical | 100 | 91.1 | 1599.7 | 17.6x | 3346.9 | 2225.3 |
| typical | 250 | 124.1 | 4020.7 | 32.4x | 9348.1 | 5837.8 |
| typical | 500 | 164.8 | 7996.8 | 48.5x | 17754.0 | 11040.0 |
| prefixed | 3 | 31.8 | 72.4 | 2.3x | 116.1 | 73.9 |
| prefixed | 16 | 55.2 | 282.7 | 5.1x | 541.1 | 353.2 |
| prefixed | 50 | 85.9 | 824.5 | 9.6x | 1741.7 | 1104.1 |
| prefixed | 500 | 205.1 | 7830.2 | 38.2x | 17247.0 | 11362.0 |

Times are ns per lookup for the first two columns and ns per map for the last
two. Two things to take from it.

**The index is worth more than the model suggests.** A linear scan through
`iter()` materializes a `Datum` for every value it walks past, not just the
keys, so the baseline it replaces is much more expensive than a bytes-only model
implies. The real win is 2.3x at three keys and 48x at five hundred, which is
consistent with the index PR's own end-to-end figure.

**The re-walk in the closure builder costs about 14 ns per entry.**
`push_dict_with` walks the entries a second time to recover their offsets, and
`push_indexed_dict_with` captures them as it writes. The gap is 36 to 40 percent
of pack time at every size measured, or roughly 14 ns per entry at n = 50. That
number is the price of any index that cannot be built in one streaming pass,
which is the central practical objection to the prefix layouts below.

Reading the model against this: absolute times in the model are 2 to 4 times
optimistic, because its key decode is a two-arm match rather than the real
`read_datum`, and its scan skips values instead of decoding them. That bias is
not uniform. It compresses the gap between layouts that differ in *how many*
full key decodes they perform, which means the model *understates* how much a
layout doing one key decode beats one doing `log n` of them. Every conclusion
below that favours a discriminator over a plain binary search is therefore
conservative. The hash-versus-prefix comparison is unaffected, since both do
exactly one key decode. Pack *deltas* in the model transfer; pack *ratios* do
not, because the shared entry-writing cost they divide by is understated.

### The layout probe

Geometric means over four key styles and two value sizes, best of seven, on a
32 MiB corpus per cell. `A` is the index as written, so read every column
against it.

**Finding a key that is present**, ns per lookup. `J` is the discriminator
layout described below, `S` the shape-dictionary floor.

| n | `L` scan | `A` PR | `B` hash table | `E` hash order | `F` hash index | `H` hash + perm | `C` fingerprint | `J` discriminator | `S` floor |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | 10.4 | 10.9 | 19.8 | 14.5 | 16.4 | 14.6 | 13.4 | **9.8** | 5.2 |
| 8 | 18.3 | 18.6 | 21.6 | 18.3 | 20.6 | 18.6 | 23.3 | **13.7** | 5.8 |
| 16 | 34.7 | 29.2 | 30.6 | 22.3 | 22.6 | 23.4 | 30.2 | **19.2** | 7.2 |
| 32 | 79.8 | 42.4 | 41.6 | 28.3 | 29.8 | 28.6 | 39.7 | **23.5** | 8.9 |
| 50 | 129.2 | 51.0 | 45.8 | 36.5 | 38.5 | 36.7 | 41.2 | **28.1** | 10.3 |
| 100 | 272.2 | 71.9 | 60.4 | 48.6 | 49.9 | 50.4 | 55.1 | **31.2** | 12.3 |
| 250 | 693.0 | 104.4 | 53.3 | 63.4 | 62.4 | 62.2 | 95.2 | **42.2** | 10.0 |
| 500 | 1358 | 133 | 53 | 65 | 68 | 65 | 143 | **55** | 9 |

The hash table never beats the binary search until well past a hundred keys,
and is 1.2 to 1.8 times *slower* below thirty-two. Hash-ordered entries (`E`)
and the hash side index (`F`) do better, because their comparisons are on
integers, but they do not beat a discriminator taken from the key itself at any
size.

**Finding a key that is absent**, ns per lookup. `?`, `?&`, `?|` and both `@>`
implementations spend much of their time here.

| n | `L` scan | `A` PR | `B` hash table | `E` hash order | `C` fingerprint | `J` discriminator | `S` floor |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | 10.7 | 13.1 | 17.1 | 13.1 | 13.6 | **7.4** | 3.1 |
| 8 | 25.5 | 20.7 | 23.1 | 14.8 | 14.4 | **10.2** | 3.1 |
| 16 | 53.9 | 29.5 | 31.5 | 17.1 | 17.3 | **15.0** | 3.1 |
| 32 | 124.2 | 41.2 | 38.7 | 20.9 | 22.2 | **16.6** | 3.0 |
| 50 | 225.0 | 47.0 | 43.2 | 24.1 | 26.5 | **17.4** | 3.1 |
| 100 | 453.4 | 63.9 | 57.4 | 26.9 | 37.6 | **20.4** | 3.1 |
| 500 | 2380 | 97 | 64 | 25 | 143 | **22** | 3.0 |

The discriminator is 1.8 to 4.4 times faster than the binary search on a miss,
and at three keys it answers a miss faster than a hit. A probe whose
discriminating bytes are not in the map cannot be any of its keys, so the answer
comes out of the suffix without decoding a key at all.

**Emitting entries in key order**, ns per map. This is the persist write, the
pgwire text form, `jsonb_stringify`, and `jsonb_object_keys`.

| n | `L` scan | `A` PR | `B` hash table | `E` hash order | `H` hash + perm | `J` discriminator |
| --- | --- | --- | --- | --- | --- | --- |
| 3 | 10.5 | 10.3 | 10.4 | **20.6** | 11.4 | 10.5 |
| 16 | 68.4 | 69.7 | 70.6 | **303.8** | 80.6 | 70.7 |
| 50 | 230.2 | 231.3 | 234.7 | **1352.9** | 266.8 | 231.3 |
| 500 | 2399 | 2413 | 2428 | **20364** | 2595 | 2419 |

Everything that keeps entries in key order is free here. Hash-ordered entries
cost 2x at three keys and 8.4x at five hundred, because the scan has to sort
before it can emit. Carrying a stored key-order permutation instead (`H`) brings
that back to roughly free, at the price of the permutation's bytes and an
indirection, which is why `H` exists in the table: it is the strongest form of
the hash-ordering idea, and it still loses.

**Building the payload**, relative to `A`, geometric mean over n up to 32. Every
decode pays this on every row.

| | `L` | `A` | `B` | `E` | `F` | `H` | `C` | `T` | `J` | `Y` | `S` |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pack | 0.81 | 1.00 | 2.08 | 2.26 | 2.17 | 2.50 | 1.57 | 1.01 | 1.65 | 1.44 | 0.99 |

Hashing roughly doubles the index build. A minimal perfect hash (`D`, not shown)
costs 4.3 times, and only for the sizes where a seed was found at all: with a cap
of a hundred thousand attempts it stops finding one somewhere between eight and
sixteen keys, which is what the perfect-hashing literature predicts for a
single-seed search and why that family is built for static sets rather than
per-row construction.

### The cost-benefit frontier

Three layouts are worth naming, with lookup time in ns and index bytes per
entry. `T` stores one byte of the key per entry, `J` stores the narrowest of
one, two, or four bytes that separates the keys, `Y` always stores four.

| n | `A` PR | `T` one byte | `J` adaptive | `Y` four bytes | `S` floor |
| --- | --- | --- | --- | --- | --- |
| 3 | 11 / 2.1 | 8 / 3.8 | 10 / 3.8 | 12 / 6.4 | 5 / 2.1 |
| 8 | 19 / 1.9 | 10 / 3.2 | 14 / 3.2 | 14 / 6.0 | 6 / 1.9 |
| 16 | 29 / 2.0 | 22 / 3.2 | 19 / 3.9 | 16 / 6.1 | 7 / 2.0 |
| 32 | 42 / 2.1 | 32 / 3.2 | 24 / 4.6 | 21 / 6.1 | 9 / 2.1 |
| 50 | 51 / 2.0 | 35 / 3.1 | 28 / 4.6 | 25 / 6.1 | 10 / 2.0 |
| 100 | 72 / 2.0 | 46 / 3.0 | 31 / 4.5 | 28 / 6.0 | 12 / 2.0 |
| 500 | 133 / 2.0 | 120 / 3.0 | 55 / 6.0 | 49 / 6.0 | 9 / 2.0 |

`T` is the striking one. One byte per entry, a pack cost indistinguishable from
the current index, and lookups 1.2 to 1.9 times faster everywhere up to a
hundred keys. It gives most of the benefit for a fifth of the extra bytes, and
gives it up only on large maps, where a single byte stops discriminating.

Two further observations from the wider table. Keeping keys and values in
separate regions (`O`, PostgreSQL's `jsonb` layout) costs a second offset array
and a buffered copy at pack time and buys nothing for an indexed lookup, since a
binary search already touches only `log n` keys. It halves the cost of a *linear*
scan, which is not enough to matter. And interpolation search over the prefix
array (`Q`) loses to bisection: after the common prefix is stripped, the
discriminating bytes of a machine-generated key set are ASCII digits, clustered
rather than uniform, which is the distribution interpolation search is worst on.

## What the measurement does and does not establish

The twenty-four layouts are modelled, not implemented in `mz-repr`. The model
reproduces the payload byte shape, reads keys through the same tag-and-length
decode, uses the same offset width rule, and cross-checks that every layout
answers every probe identically, so the *relative* numbers are meaningful.
Three caveats bound how far to push them.

**Pack costs for the prefix layouts are optimistic.** The model computes each
key's prefix from the caller's key slice. A real packer has the keys only in
the buffer it just wrote, and the LCP is not known until the last key has been
seen, so it must either retain the leading bytes of every key while packing or
re-walk the keys once at the end. Hashing does not have this problem: a hash can
be computed as each key is pushed, which is what makes `C` and `B` streamable
and the LCP-based layouts not. The honest bound on a real prefix layout's pack
cost is the model's pack cost plus at most the `iter` cost in the same row, and
both numbers are in the tables. A builder that keeps the first and last key,
plus the leading 8 bytes of each key, avoids the re-walk whenever the LCP plus
the prefix width fits in those 8 bytes, which covers every key style measured
here except the 37-byte shared prefix one.

**The machine is a shared VM.** Run-to-run variation of 20 to 30 percent on a
single cell is normal here, which is why the tables are geometric means over
eight key-style and value-size combinations, best-of-seven within each cell.
Read the shape, not the third digit. The one place this matters for a
conclusion is the small-`n` crossover between a linear scan and an index, which
sits inside the noise band for a couple of cells.

**`S` is not implementable as a `Row` change.** It models a caller holding a
per-shape dictionary, which needs mutable state that scalar expression
evaluation does not currently have. It is in the tables as a bound, to show how
much of the gap any in-payload index leaves on the table.

## Recommendation

**1. Do not use a hash table, and do not sort entries by hash.** The first is
slower than the current binary search across the whole range of map sizes that
occurs in practice, costs about twice the index bytes, and adds a frozen hash
function to the `Row` byte-equality contract. The second is a semantic change:
it moves `jsonb_object_keys`, the text rendering of `jsonb`, and `ORDER BY` on
map-typed columns onto an arbitrary order, and it turns an in-memory-only change
into a persist format change, because the Arrow encoder writes entries in
iteration order. Keeping a key-order permutation alongside hash-ordered entries
(`H`) buys back the semantics but is measurably worse than the prefix layouts on
every axis, so there is no version of hash ordering worth the trouble.

**2. Land the index as written.** It is a strict improvement over a linear scan
from the point where map sizes stop being trivial, it is the most compact index
measured, its canonicality argument needs no frozen constants, and every
alternative below is a refinement of it rather than a correction to it.

**3. Two follow-ups, in cost-benefit order.**

*Compact the trailing count word.* The suffix is `W * (n - 1) + 4` bytes, and
for a small map the four-byte count word is most of it. Two bits are already
spent on the offset-width class, so a one-byte trailer holding the class in its
top two bits and the count in its low six covers `n < 63`, with the current
four-byte form kept behind a sentinel for larger maps. That is a pure memory
win with no change to the lookup path, and it lands where the current overhead
is worst, on small and nested objects:

| Object (from the index PR's cost table) | Index now | With a 1-byte trailer | Overhead now | After |
| --- | --- | --- | --- | --- |
| `{"a":1,"b":2,"c":3}` | 6 B | 3 B | +16.7% | +8.3% |
| 12-key event object | 15 B | 12 B | +7.7% | +6.2% |
| 3 nested 3-key objects | 24 B | 12 B | +19.0% | +9.5% |
| 50 keys, short values | 102 B | 99 B | +11.2% | +10.9% |
| 500 keys, integer values | 1002 B | 1003 B | +13.3% | +13.3% |

*If lookup latency matters more than bytes, store a discriminator.* Keep
everything the current design has and add, per entry, a slice of the key taken
after the map's common prefix is dropped. The descent then compares integers out
of a contiguous array and touches the entries region once, at the end.

Start with **one byte** (`T`). It is +1.1 bytes per entry, a pack cost within
noise of the current index, and 1.2 to 1.9 times faster lookups up to a hundred
keys, which covers the map sizes a `jsonb` column actually holds. Widening to an
adaptive one, two, or four bytes (`J`) buys another 1.4x at fifty keys and 2.4x
at five hundred, for roughly double the index bytes and 65 percent more pack
time, and is only worth it if large maps turn out to be a real workload rather
than a benchmark.

Either needs:

* one byte for the common prefix length,
* for the adaptive form, the prefix width class in two more bits of the count
  word, alongside the offset width class already there,
* a builder that can produce the prefixes, per the caveat above,
* a lookup that walks the run of equal discriminators rather than assuming one
  candidate, since a fixed width does not guarantee uniqueness. Skipping this is
  a search that returns a fast wrong answer, and it is what the probe's
  cross-check caught during this work.

Nothing about it touches iteration order, the Arrow encoding, the ascending-key
contract, or the canonicality argument, and it needs no hash function. The
search strategy is not part of the encoding either: a scan and a bisection over
the same bytes agree, so the choice can be made from `n` at read time. Measured,
the crossover is around eight keys and the gain from switching is one to three
nanoseconds, so a single branch-free bisection everywhere is the better trade.

**4. Note for later, not now.** The floor row shows that resolving a key to a
slot once per map *shape* rather than once per row is several times faster than
the best in-payload index, and that gap does not close with better index
layouts. For a workload that is really "JSON to columns", the shape-aware
representation is the answer and the index is a stopgap. That is a much larger
change than this one and it does not argue against landing the index.

## Prior art

The literature is unambiguous that sorted keys plus a search structure is the
right shape for a canonical, immutable, build-once-read-few map, and that hash
tables belong to the mutable build-once-read-many case.

* **PostgreSQL `jsonb`** stores object keys sorted, by length and then
  bytewise, and finds a key by binary search over the `JEntry` array in
  `findJsonbValueFromContainer`. It spends roughly 8 bytes per entry on that
  array, so the index here is the same shape and more compact. PostgreSQL has
  never adopted hashing for `jsonb`.
* **Canonical serialization formats** all sort keys for exactly the
  byte-equality reason above: CBOR's deterministic encoding (RFC 8949 section
  4.2), JCS (RFC 8785), DER `SET OF` ordering. None of them uses a hash order,
  because a hash order is canonical only relative to a frozen hash.
* **Swiss tables** (Abseil `flat_hash_map`) and **F14** (Folly) scan one-byte
  control fingerprints with SIMD, which is layout `C`. They are mutable
  in-memory tables, not canonical byte images, and they carry the load-factor
  slack that makes `B` expensive here.
* **Minimal perfect hashing** (CHD, BDZ, BBHash, RecSplit, PTHash) reaches 2 to
  4 bits per key, but construction is randomized with retries and is designed
  for static sets queried many times. Layout `D` is the crude version of this
  and the measurements show why it is not viable per row.
* **Key prefixes in the index** is the standard trick for turning
  variable-length comparisons into integer comparisons: PostgreSQL's
  abbreviated keys for sortsupport, prefix-compressed SSTable and LMDB blocks,
  and Arrow's `StringView`, which inlines a 4-byte prefix for exactly this
  reason. Layouts `G`, `K`, `N`, `T`, and `X`.
* **Front coding / LCP stripping** is standard in inverted indexes and
  prefix-compressed blocks, and is what layout `N` borrows.
* **Array layouts for search** (Khuong and Morin) finds that for small arrays a
  branchless search beats cache-conscious layouts like Eytzinger, which matches
  `Y` being competitive but not transformative at these sizes.
* **Shredding** is the real answer for JSON-to-columns workloads and is what
  layout `S` models: ClickHouse's JSON type, Snowflake's VARIANT, Parquet
  variant shredding, and Dremel's record shredding all resolve a field to a
  position once per shape rather than once per row.

## Reproducing

`src/repr/benches/map_index.rs` measures the two strategies that exist in the
tree, `DatumMap::get` and a linear scan over `iter()`, plus both packers, over
a corpus large enough that the maps do not sit in L2:

```
cargo bench -p mz-repr --bench map_index
```

The twenty-layout probe is a standalone crate, because seventeen of the layouts
do not exist in `mz-repr` and implementing them there to benchmark them would
be the change this document is trying to decide on. It models the payload byte
shape (tag, length, key, value encoding, the {1, 2, 4} offset widths, the
trailing count word) and cross-checks that every layout resolves every probe
identically before reporting timings.
