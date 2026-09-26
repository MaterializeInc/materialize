# External payloads for out-of-core arrangements

- Associated: upsert v2 hydration investigation (September 2026), DNM PR #38719
  (payload-separated upsert prototype)

## The Problem

Out-of-core arrangements merge their batches geometrically, so every update is
rewritten about log(state / batch) times over its life. The rewrite cost is
proportional to the bytes in each update. When updates carry wide values, most
of those bytes are values that no merge ever inspects: merges order and
consolidate by key, time, and value equality, and a 2 KB value contributes its
bytes to every rewrite while contributing only an equality bit to the merge.

Measured on a skewed 105-source upsert hydration of 192M rows with 1,900-byte
incompressible values (about 407 GB of state, 400cc, 8 workers):

| Configuration | Hydration | CPU seconds | Extents written |
| --- | ---: | ---: | ---: |
| RocksDB-backed upsert | 47m | 21,517 | n/a |
| Synchronous merges, funded exertion | about 54m pace | about 21,000 | 2.5 TB |
| Values stored outside the arrangement (prototype) | 21 to 23m | 9,575 to 10,703 | 0.49 TB |

Moving the values out of the merge path cut extent traffic by 5x and CPU by
half. It is the only change measured that beats the RocksDB implementation in
the regime where state is many times the memory budget.

The prototype is shaped around upsert, and it has a per-record cost that makes
small-replica snapshots 3x to 4.5x slower than the plain arrangement: a
CPU-bound phase after the merges finish, about 50 microseconds per record,
which scales with chunk count. The prototype's handles are physical locations,
so two copies of the same value are distinguishable only by reading their
bytes. Every consolidation that meets two updates with equal keys resolves
payloads through a synchronous comparator, outside the shared read budget.
That comparator is the leading hypothesis for the per-record cost, pending a
profile.

This document sketches a version of payload separation that is independent of
upsert, so any arrangement with wide opaque values can use it, and that never
reads payload bytes during trace maintenance.

## Success Criteria

- Trace maintenance (merge, advance, consolidate, settle) reads no payload
  bytes. Payload bytes are read only by consumers that need the value.
- Any `Chunk`-based arrangement can store a chosen column out of line without
  changes to the spine, the batcher, or the merge kernels.
- Payload reads use the buffer pool's existing read path and one shared decoded
  byte budget, with no async runtime dependency.
- Memory held by payloads is bounded by live payload bytes times a constant
  fragmentation factor, not by the history of rewrites.
- Upsert v2 plugs in as one consumer, and its small-replica snapshot is no
  slower than the plain chunked arrangement.

## Out of Scope

- Persisting external payloads. Handles are valid only within the process that
  wrote them, and arrangements are process-local, so nothing here reaches
  persist.
- Deduplicating equal values across writers. Two writes of the same bytes
  produce two copies. Consolidation still treats them as equal.
- Columns whose ordering consumers observe. See the ordering contract below.

## Solution Proposal

### Summary

A value stored out of line is replaced in the arrangement by a fixed-size
handle: a keyed 128-bit content digest plus a physical locator. Handles order
and compare by digest alone. Because equal bytes have equal digests, the native
columnar merge and consolidation over handles give the same result they would
over the bytes, and they never follow the locator. Chunks carry the set of
payload blocks their handles reference, and that set is the only thing trace
maintenance adds to its work.

### Components

The pieces live in `mz_timely_util::columnar::external` and depend only on the
buffer pool and the column chunk.

**Handle.** `Blob` is a columnar struct of a 128-bit digest and a locator
(block, offset, length). The columnar derive generates a reference type for
it. Its ordering is written by hand on both the owned and the reference type
and compares only the digest. The derive also generates a comparison between
the reference type and the owned type that reads every field, so code must
compare references with references.

**Digest.** SHA-256 keyed with 32 random bytes chosen once per process,
truncated to 128 bits. SHA-256 is already a workspace dependency, and the
aarch64 and x86 cores this runs on have SHA-256 instructions. Keying means
colliding inputs cannot be constructed without the secret. Accidental
collisions among n live values occur with probability about n^2 / 2^129.

**Writer and blocks.** `BlobWriter::push` hashes a byte string and appends
it to an open block, returning its handle. The open block persists across
calls, so small writes share blocks. A block lives on the heap until it fills
at 2 MiB, then moves to the buffer pool at the deepest eviction band, where
the pool compresses and evicts it like any chunk body. A value larger than a
block gets a block of its own. `take_owners` returns a `BlockSet` owning
every block written since the previous call, including the open one, so
records can reference a block before it fills.

**Block sets.** A `BlockSet` is a shared, sorted slice of reference-counted
blocks. A block is freed when the last set naming it drops. Sets union
cheaply and restrict to a sorted list of block identifiers. Restricting to an
identifier the set does not hold panics, since a record would then point at
bytes nothing keeps alive.

**Resolution.** `Resolver::load` takes the handles a consumer needs and the
sets that own them, and reads each referenced block once. Heap blocks are
borrowed in place and pool blocks are read through `ChunkHandle::read_async`,
which runs nonresident reads on the pool's spill threads and needs no async
runtime. `load_sync` does the same on the calling thread. `with` lends the
bytes of one handle to a closure.

**Layout.** The only hook an arrangement provides:

```rust
pub trait BlobLayout: 'static {
    type Data: Columnar;
    type Diff: Columnar;
    /// Appends the block of every blob `data` and `diff` reference.
    fn blocks(data: Ref<'_, Self::Data>, diff: Ref<'_, Self::Diff>, out: &mut Vec<u64>);
}
```

It has no comparison method. Ordering belongs to the columnar types.

**Chunk wrapper.** `ExternalChunk<L, T>` pairs a `ColumnChunk<L::Data, T,
L::Diff>` with a `BlockSet` and delegates every `Chunk` operation to the
column chunk. Chunks produced by merge, extract, and advance get the union of
their inputs' sets, which is always sufficient and costs no body reads. Each
chunk leaving `settle` has its set trimmed to the blocks its records
reference, by walking `L::blocks` over the chunk's metadata. A chunk remembers
whether its set is exact, so chunks that pass through `settle` unchanged are
not walked again. `ExternalChunk` also implements the probe unload interface.
Its staging carries the sets of every chunk that contributed hits, so probe
results stay resolvable after the batches they came from are released.

**Externalizing chunker.** `ExternalChunker<X, T>` is a chunker for
`arrange_core`. For each incoming column it maps every record's data through
`X::externalize`, which writes the chosen bytes to the chunker's writer, then
sorts, consolidates, and bounds the result as the column chunker does, and
wraps each output chunk with the writer's new set.

```rust
pub trait Externalize: 'static {
    type Input: Columnar;
    type Layout: BlobLayout;
    fn externalize(input: Ref<'_, Self::Input>, writer: &mut BlobWriter)
        -> <Self::Layout as BlobLayout>::Data;
}
```

### Ordering contract

Values that are externalized sort by digest, not by content. That is correct
for any consumer that only tests values for equality, which covers upsert
state, key-value lookups, and join payloads. It is wrong for any consumer that
reads the order of values within a key through a cursor, such as a `min`,
`max`, or top-k on the value column implemented over the arrangement. Only
columns whose order is opaque to every reader may be externalized.

### Collisions

A collision cannot be caught when a value is read back. If two different
byte strings share a digest, consolidation cancels or combines them and keeps
one locator. The surviving record then points at bytes that hash to its
digest, so any check made at retrieval passes. Catching a collision requires
keeping both locators and their blocks until a later pass compares the bytes,
and holding every output that depends on the unverified pair until the pass
finishes, since an upsert output reaches persist and a restart does not undo
it. That machinery is not in the prototype. The prototype relies on the keyed
cryptographic digest alone.

### Reclamation

A block stays alive while any set names it, so one live value keeps its whole
block. Blocks are filled in arrival order and a chunk holds a key range, so
under random keys a block's values spread across the whole arrangement and a
block is freed only when all of its values are dead. Compaction of partially
live blocks is designed but not built. A rewrite changes only locators, never
digests, so it cannot change any ordering. At `settle`, when a block's live
fraction in an output falls below a threshold, the live values are copied to a
fresh block and the output's handles are rewritten, charged as merge fuel.

### How upsert plugs in

The prototype adds a third upsert v2 flavor behind
`enable_upsert_payload_stash`, used when the chunked stash is on.

- The feedback arrangement stores `(UpsertKey, Blob)`. `FeedbackBlobs`
  implements both the layout and the externalizing projection, writing each
  feedback row's bytes.
- The source stash stores each command's value as a blob too. Its batcher
  writes values after the chunker's offset consolidation, so only the winning
  command per `(key, time)` within a flush pays for a write, and the batcher's
  chain merges move handles. Consolidation keeps the greatest offset as
  before, moving only the handle.
- Commands a drain finds ahead of the persist frontier return to the stash as
  handle chunks with their owners. A source running ahead of persist never
  reads or rewrites their values across drains. The drain resolves values only
  for eligible commands, which it emits.
- The bulk-probe drain is generic over how it reads prior values. The external
  implementation consolidates probe hits by digest, so a retraction that came
  back from persist as a fresh copy cancels the stored value without a read.
  It then resolves only the surviving values, grouped by block, because the
  drain emits their retraction.

### Composition with int-proxy

The int-proxy operators in Differential run join and reduce over `Copy + Ord`
proxies and leave interpretation to a backend. A `Blob` is a valid value proxy
without minting: equal content gives an equal handle in every run and window.
A backend presents handles straight from `ExternalChunk` metadata, and
resolves bytes only where user logic needs them, for join output or for a
reduce group's inputs. Because int-proxy already treats proxy order as
meaningless outside consolidation, the ordering contract above does not
restrict it. Value proxies must be distinct, which the digest provides with
the probability above. A backend holding deferred join work must also hold the
block sets of the handles in that work.

### Other candidate consumers

- Compute arrangements of wide `Row` values keyed on a narrow key, where joins
  and lookups read the value only on output.
- Any arrangement whose value column is large relative to its key and is read
  by equality or on output only.

## Minimal Viable Prototype

Built on top of the landing stack as three commits:

1. `mz_timely_util::columnar::external`: the handle, digest, writer, block
   sets, resolver, chunk wrapper, and externalizing chunker, with tests for
   digest semantics, round trips through heap and pool blocks, block release
   after restriction, cancellation of copies without reading bytes, and
   release of unreferenced blocks at settle.
2. The upsert flavor with an external feedback arrangement. Every operator
   scenario runs it with synchronous and asynchronous reads and requires
   output identical to the paged and chunked flavors. One scenario uses 20 KB
   values so blocks fill and move to an evicting pool before the drain
   resolves them.
3. The external source stash, with a scenario that runs the source three
   timestamps ahead of persist so commands are re-stashed across drains.

Not built: block compaction, a decoded-byte budget for resolution, and memory
accounting for block sets.

Validation reuses the September batteries. The incompressible hydration
should approach the earlier prototype's 21 to 23 minutes. The 25cc and 50cc steady-state snapshots must match the
plain chunked arrangement, which is where the earlier prototype lost 3x to
4.5x.

## Alternatives

- **Physical handles with a logical comparator**, as in the prototype. It
  works, but every consolidation involving equal keys reads payload bytes,
  those reads are synchronous and outside the shared budget, and the layout
  trait has to carry an operator-specific comparison. That trait method is what
  ties the prototype to upsert.
- **A persistent interner** mapping content to one canonical copy. It makes
  locators comparable, but its memory grows with every distinct value ever
  seen and needs its own reclamation protocol. The prototype considered and
  dropped it for that reason.
- **Verifying digests against bytes on equality.** It removes the collision
  assumption but brings back the payload reads this design exists to remove.
- **Deferring verification to retrieval.** It catches nothing, as described
  under Collisions. Deferring it to an asynchronous pass works only with the
  evidence kept and outputs held, which is a follow-up if the digest alone is
  judged insufficient.

## Open questions

- Digest cost. Keyed SHA-256 over a 2 KB value has not been measured on the
  benchmark hosts.
- Multi-process replicas. Handles must not cross a process boundary. Timely
  exchange of `ExternalChunk` needs either a check that fails loudly or a
  resolve-and-reexternalize step at the exchange.
- Inline threshold. Small values pay 32 bytes of handle for nothing. A
  per-record inline arm is possible but complicates the columnar layout.
- Where `BlockSet`s and heap blocks are accounted. Both are resident and
  outside the pool budget. A set can name every block of a worker's
  arrangement when keys are random, so sets are the first thing to measure.
- Whether the writer and block sets belong in `mz-timely-util` or next to the
  pool in `mz-ore`.
