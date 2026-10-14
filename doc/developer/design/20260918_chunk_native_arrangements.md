# Chunk-native arrangements with encoded whole-value columns

- Status: proposal, with implementation and performance validation outstanding.
- Associated: [Buffer-managed dataflow state](20260610_buffer_managed_state.md).
- Baseline: upstream `main` at
  [`3c8858e2611bb1f50e03ff94a008118f1d3707c2`](https://github.com/MaterializeInc/materialize/commit/3c8858e2611bb1f50e03ff94a008118f1d3707c2),
  fetched on 2026-09-18. No tracking issue is assigned to this proposal.

## The Problem

Compute can maintain its arrangement batcher input as spillable chunks, but
sealing converts those chunks into resident `ord_neu` batches. The long-lived
arrangement consequently has a different storage and maintenance unit from its
batcher. Extending the buffer pool to sealed compute state requires a batch
representation whose independently readable and replaceable units remain chunks.

Replacing the existing representation with flat update arrays would discard
useful compression and navigation properties. Keys are shared across values,
values across their timestamp histories, and consecutive identical singleton
updates can share their time and diff. Materialize also compresses regular
offset sequences and dictionary-encodes row payloads. Chunk-native arrangements
must preserve these opportunities without assuming a trie is the only way to
represent them.

This proposal uses independent dense, constant, or run-end encoded columns for
whole keys, values, timestamps, and diffs. It reuses Differential's chunk batch
and spine machinery, with explicit extensions for bounded reads and resumable
compaction. Performance benefits are hypotheses to validate, not measurements
established by this document.

### What exists on trunk

| Component | Baseline behavior | Consequence |
| --- | --- | --- |
| [`ArrangementBatcher`](../../../src/compute/src/extensions/arrange.rs) and [`render/context.rs`](../../../src/compute/src/render/context.rs) | Columnation, columnar, and chunked batchers all produce `RowRowSpine` at the main row arrangement site. The chunked arm uses `UnchunkBuilder<RowRowColPagedBuilder<...>>`. | A new batcher choice alone cannot change sealed storage. |
| [`UnchunkBuilder`](../../../src/timely-util/src/columnar/chunk.rs) | Reads chunks into columns and feeds a downstream builder, with chain-level statistics for row dictionaries. | Sealing avoids loading all input bodies at once but still builds a resident output trie. |
| [`mz_row_spine`](../../../src/row-spine/src/lib.rs) | `RowRowSpine`, `RowValSpine`, and `RowSpine` use `OrdValBatch` or `OrdKeyBatch`. `DatumContainer` supplies row compression and `OffsetOptimized` compresses regular offsets. | The comparison baseline includes these optimizations, not just upstream's default vector layout. |
| [`ColumnChunk`](../../../src/timely-util/src/columnar/chunk.rs) | Sorted flat columnar updates, resident fence metadata, byte-oriented packing, pool-backed bodies, and scoped reads. | Reuse the storage lifecycle and metadata-only skipping principles. |
| [`UnloadChunk` / `UnloadBatch`](../../../src/timely-util/src/columnar/unload.rs) | Sorted probes select matching updates into owned staging. A key at a chunk's upper boundary remains eligible in the next chunk. | Existing bulk-read semantics handle straddling keys, but the accumulating interface does not bound staging size. |
| [`upsert_continual_feedback_v2`](../../../src/storage/src/upsert_continual_feedback_v2.rs) | Its chunked feedback flavor already uses a `ChunkSpine<ColumnChunk<...>>`. | A chunk-native trace is an existing integration pattern, not a proposed new spine. |
| [`ArcBatch`](../../../src/row-spine/src/arc_batch.rs) | Shares suitable sealed batches across runtimes. | New compute batches must preserve `Send + Sync` payloads and shared ownership. |

The [locked dependency](../../../Cargo.lock) is Differential 0.25.1. Its
[`trace::chunk` implementation](https://docs.rs/crate/differential-dataflow/0.25.1/source/src/trace/chunk/mod.rs)
already contains `ChunkBatch`, `ChunkBatchBuilder`, `ChunkBatchMerger`,
`NavigableChunk`, and a cursor that continues groups across chunk boundaries.
Its convenience spine and builder aliases use `Rc`. Compute should instead
compose the underlying batch and builder with its `ArcBatch` and `ArcBuilder`
wrappers, subject to the new chunk body's thread-safety requirements.

## Success Criteria

- The arrangement builder retains encoded chunk handles rather than reconstructing a
  batch-wide trie. Bodies whose encoding is already final need no payload copy.
- A scan or merge of state larger than the pool budget uses explicitly bounded
  body windows and scratch. Resident metadata, retained snapshots, concurrent
  readers, and oversized individual records are separately accounted for.
- Key and value seeks skip groups without decoding every update. Sparse probes
  read only chunks whose fence ranges intersect the probes.
- Results and frontier behavior match the existing arrangements, including
  retractions, product timestamps, compaction, recursive scopes, and key-only
  arrangements with non-scalar diff containers.
- Encoding retains the existing useful compression cases. The prototype reports
  bytes per logical update, metadata bytes, allocation and copy volume, merge
  cost, and read latency against the actual row-spine baseline.
- Switching representations preserves shared arrangements and cross-thread
  snapshots. Changing configuration does not reinterpret an existing trace.

Adoption requires agreed regression limits for resident workloads and a measured
benefit for larger-than-memory state. Those numerical limits remain a review
decision. Correctness and bounded body residency are mandatory independently of
the performance comparison.

## Out of Scope

- Decomposing SQL rows into independently encoded datum columns. `K` and `V`
  remain whole values. Existing dictionary encoding inside a row is preserved.
- Replacing the spine's batch scheduling or the process buffer pool.
- Durability, recovery from chunk files, or changes to persist's storage format.
- Requiring asynchronous I/O throughout compute. Scoped synchronous reads can
  establish the ownership contract, with asynchronous requests added separately.

## Solution Proposal

### Storage and ownership

Use an encoded chunk family for both batcher chains and sealed batches:

```text
Spine<ArcBatch<ChunkBatch<EncodedChunk<K, V, T, D>>>>
  batch description: lower, upper, since
  ordered chunk handles
    resident metadata
    resident shared body OR pool handle
      encoded K, V, T, D columns
```

The names above describe responsibilities, not finalized Rust declarations.
Generic encoding and chunk operations belong in `mz_timely_util`. Row payload
codecs and row-specialized aliases belong in `mz_row_spine`. `mz_compute` chooses
the representation and adapts consumers without owning a second storage engine.

Chunks are immutable once published. Metadata contains logical update count,
encoded byte length, first and last `(K, V, T)`, and lifecycle hints used by the
pool. Fences own their values independently of the body. They cannot borrow
evictable bytes. Generic large keys and values make fence memory material, so
it is included in arrangement accounting.

The resident body uses shared ownership compatible with `Send + Sync` for the
compute types that require it. An outer `ArcBatch` does not make the current
`Rc`-backed `ColumnChunk` payload thread-safe. Copy-on-write construction must
finish before publication. Readers own scratch and navigation state, never
mutate shared decoded caches, and retain batches through the existing snapshot
and frontier-hold mechanisms.

### Column representation

Each column describes the same `N` logical updates:

```rust
// Conceptual representation. C is a whole-value payload container.
enum EncodedColumn<C> {
    Constant { value: C, len: usize },
    Dense(C),
    Runs { values: C, ends: RunEnds },
}
```

`Constant` stores one value. `Dense` stores `N` values and no run metadata.
`Runs` stores distinct adjacent run values and strictly increasing exclusive
ends, with the last end equal to `N`. Regular ends may use a stride descriptor.
Explicit ends use a checked width sufficient for the chunk's logical length.
Unit-valued columns, including the value of a key-only arrangement, need no
payload bytes.

Run encoding requires lawful equality on the payload's semantic values. A
generic diff type that supplies only the required algebra can remain dense
rather than acquire an additional equality or ordering requirement solely for
compression.

For example:

```text
logical index   0  1  2  3  4  5
K               a  a  a  b  b  c
V               x  x  y  y  z  z
T               2  3  3  3  3  3
D               1  1  1  1  1  1

K: values [a, b, c], ends [3, 5, 6]
V: values [x, y, z], ends [2, 4, 6]
T: values [2, 3],    ends [1, 6]
D: constant 1,      length 6
```

Encoding selection compares serialized payload and metadata sizes. Builders
recognize constants, track run lengths, and fall back to dense storage when
run metadata outweighs payload savings. Finalization may recode one bounded
chunk. It must not collect whole-batch statistics or rewrite existing bodies
solely because a batch was sealed. Young and deep chunks may eventually use
different encoding policies, but the prototype starts with one deterministic
policy so results are attributable.

### Dictionary placement and construction

Dictionary compression belongs below the structural column encoding, in the
whole-row payload container for `K` or `V`. Each has its own optional codec.
RLE removes adjacent equal whole rows. The dictionary removes repeated datum
bytes inside the rows that remain, including common datum values across
otherwise distinct rows. This preserves row encoding without introducing
independent SQL datum columns into the execution format.

```text
K run ends -> stored whole-key rows -> optional K row dictionary
V run ends -> stored whole-value rows -> optional V row dictionary
T and D   -> their structural encodings
```

The dictionary scope is one chunk's payload column. A finalized chunk owns the
decoder tables needed to interpret it, independent of any batch or batcher.
Serialize those tables with the body so evicting it also releases their resident
storage. Resident fences store semantic values without depending on a fetched
dictionary. Encoding lookup tables and training state belong to the builder,
not to permanently resident chunk metadata. This requires separating the
current `ColumnsCodec`'s encoder, decoder, and statistics responsibilities and
adding a serializable frozen decoder representation.

Pool block compression remains a separate layer over the serialized encoded
body, including its decoder tables. Reading a pool extent undoes that block
compression. It does not expand the structural runs or row dictionary entries.

Trunk instead trains K/V dictionaries across the readied chain in
`RowRowColPagedState`, then installs them in the `ord_neu` builder. With
compression enabled, `UnchunkBuilder::seal` may read a spilled body for those
statistics and again when pushing its rows. The proposed builder never makes
that chain-wide observation pass.

Train and freeze a dictionary when constructing a new settled chunk, before
publishing or pooling its body. Use a bounded staging buffer of semantic packed
rows, with structural run information. For each candidate structural encoding,
count the payload entries it would physically store, then choose a dictionary
and compare final payload plus dictionary and run-metadata bytes against the
raw alternative. In a run column, a key repeated for a million timestamps
contributes one stored key, not a million dictionary observations. A dense
column has different weights. Selecting the two compression layers must account
for that difference rather than reuse logical-update frequencies blindly.

The prototype can make two passes over this resident staging: one to choose a
codec, one to encode. Scratch reservations bound the staged uncompressed
payload as well as the encoded output, since a small encoded body can expand
substantially. Evaluate candidates with reusable scratch rather than retaining
every candidate body. Store raw payloads when compression does not pay for its
dictionary. Comparisons and merges consume semantic borrowed values, not
dictionary tag order or necessarily allocated `Row`s.

Dictionary correctness requires complete tag-usage information. The existing
codec uses exact literal first-byte bitmaps as well as approximate heavy-hitter
statistics. Sampling may choose dictionary entries but cannot prove a tag is
unused. Observe all candidate output values before using data-dependent spare
tags. Reusing a codec on a strict subset of its original values is safe, but
appending previously unseen literals requires a new safety proof, structurally
safe tags, or a new codec. Raw and encoded source rows must each be interpreted
with their own codec before transcoding. Approximate merged statistics alone
are not permission to apply one source's decoder to another source's bytes.

Whole-chunk forwarding preserves the body and codec without any lookup or
transcoding. Copying a subset under the same codec can preserve encoded bytes,
at the cost of retaining unused dictionary entries. Combining incompatible
codecs normally decodes borrowed datum slices into bounded destination staging
and builds a new codec. Byte-range copying is legal only when the destination
uses the same encoding and decoder. Unused dictionaries are reclaimed on a
rewrite, not by modifying published chunks.

Dictionary training is therefore a chunk-construction policy, not an
arrangement-sealing requirement. The initial policy attempts it for new settled
chunks when compression is enabled. If that costs too much in young merge
batcher generations, a later policy can leave those payloads raw. Raw chunks
remain valid arrangement chunks and become candidates for compression at their
next rewrite. A flag change or a transition to an arrangement must not force
otherwise reusable chunks to change dictionaries. Avoiding young-generation
training can leave long-lived untouched chunks raw, so any optional background
re-encoding policy needs its own measured benefit and budget.

Chunk-local dictionaries may duplicate entries or compress less effectively
than a chain-wide dictionary. Measure those costs explicitly. An immutable
dictionary shared by a bounded cohort of chunks is an alternative optimization,
but needs independent lifetime and memory accounting. A mutable batch-wide or
trace-wide dictionary would couple otherwise independent chunks and is not the
initial design.

### Merge batcher to arrangement handoff

Use the same `EncodedChunk` type and body format on both sides. Ownership and
batch descriptions change at this boundary, not the meaning of the payload.
There are two distinct operations currently called sealing:

```text
incoming columns
  -> sort and consolidate into chunks
  -> merge batcher chains of finalized chunks
  -> MergeBatcher::seal(upper)
       merge chains, then partition by the upper frontier
       kept chunks    -> remain in the merge batcher
       readied chunks -> Builder::seal(chain, description)
                          -> ArcBatch<ChunkBatch<EncodedChunk>>
                          -> arranged stream and trace
```

`MergeBatcher::seal` must still merge its chains into one sorted consolidated
chain and extract readied updates. For a timestamp `t`, keep it when the upper
antichain is less than or equal to `t`, and ship it otherwise. Temporal order is
not the sequence's primary sort order, so a mixed chunk can produce many
disjoint readied and retained ranges. This is not generally a prefix split.

| Source chunk after chain merging | Extraction work | Destination |
| --- | --- | --- |
| All updates readied | Inspect times unless metadata already proves readiness, then move the existing handle. | Arrangement builder |
| All updates retained | Inspect times unless metadata already proves retention, then move the existing handle. | Merge batcher |
| Mixed readied and retained updates | Read the body and append qualifying ranges into bounded output chunks. Reuse a valid source codec for subsets or train a destination codec when combining them. | Both |

A constant timestamp recorded in resident metadata can certify a whole chunk's
classification without a body read. First/last `(K, V, T)` fences alone cannot.
Chunks rewritten during merging or extraction finalize their codecs while the
output is already resident. Settling packs each destination independently. Do
not keep partial slices that indefinitely retain a much larger parent body in
the initial implementation.

`Builder::seal` then adopts the finalized readied handles and attaches the
batch's lower, upper, and since description. Differential's current
`ChunkBatchBuilder::seal` runs `settle_all`, so our settling contract must make
already finalized, adequately packed chunks a no-op. Only unfinished tails or
newly underfilled neighbors may need bounded repacking. No batch-wide dictionary
selection, row reconstruction, new pool insertion for an unchanged body, or
time advancement is required at this handoff. Later trace compaction may
advance timestamps and rewrite chunks under the usual frontier rules.

The handoff can allocate a vector of handles and a shared batch wrapper. It is
payload-copy-free when the readied chunks already satisfy the packing contract.
It does not make all of `MergeBatcher::seal` zero-copy. Record handle forwards,
body reads, frontier-partition rewrites, dictionary transcodes, and tail repacks
separately so this distinction is visible in measurements.

The arranged stream and trace may share the resulting batch. Pool ownership
continues through its existing handles, while accounting moves from batcher
ownership to arrangement ownership without charging the same allocation twice.
No consumer may mutate a chunk merely because its last batcher owner released
it. Its codec and body remain frozen for all shared readers.

### Ordering and navigation

A batch is one globally sorted and consolidated sequence in `(K, V, T)` order.
An equal `(K, V, T)` cannot occur twice in that sequence, including across a
chunk boundary. Zero diffs are removed. Different batches may contain updates
that require further consolidation when read or merged.

A local navigator maintains a key interval and a KV interval in logical update
coordinates. `seek_key` searches ordered key values, then establishes the key
interval. `seek_val` searches only inside that interval. `step_key` and
`step_val` jump to the corresponding interval ends. Value runs can cross key
boundaries: a KV group is the intersection of a value run and a key interval.
The example's `y` run therefore describes two separate KV groups.

For a run-encoded column, navigation searches run values and run ends. For a
dense column, bounded searches find equal-value intervals. Sequential readers
retain run positions. A dense fallback must not introduce a fresh binary search
for every timestamp in an already identified group.

`T` and `D` runs need not align with each other or with a KV group. Readers walk
their interval intersections. Equal adjacent `(T, D)` updates can still be
represented across distinct KV groups, preserving the intent of `ord_neu`'s
singleton-sharing optimization without per-value update offsets.

Batch-level navigation uses resident fences and continues keys and KV groups
across chunks. Within one batch this is concatenation. Across batches it is an
ordered merge with frontier semantics. Reuse Differential's resident chunk
cursor where its reference contract applies, and preserve its boundary behavior
in the scoped reader described below.

### Bounded reads and consumer compatibility

The [pool](../../../src/ore/src/pool.rs) reads bodies into caller-owned memory.
Its plain read leaves pool residency unchanged. A chunk read should expose a
view only while that caller-owned buffer is valid:

```text
read chunk into a reader-owned scratch reservation
visit the encoded view
copy selected ranges into owned staging, if they must outlive the visit
release or reuse scratch
```

Use cursor compatibility as the first consumer migration path. A resident
adapter implements the existing cursor trait over encoded chunks or stable,
bounded staging. Key/value operations navigate logical intervals, `map_times`
walks time/diff runs, and returned values interpret the current chunk's
dictionary. Existing operator algorithms need not know the physical encoding.

That adapter alone cannot provide bounded access to spilled bodies. The current
reference contract is effectively:

```rust
fn key<'a>(&self, storage: &'a Storage) -> Key<'a>;
```

It permits a caller to retain a key while advancing the cursor:

```rust
let key = cursor.key(&storage);
cursor.step_key(&storage);
use_key(key);
```

The reference remains tied to storage, so advancing cannot invalidate its body.
Caching every fetched body permanently satisfies that lifetime but makes scans
unbounded. Keeping scratch inside the cursor does not satisfy the signature.

Prototype a windowed cursor whose returned references borrow the cursor as well
as storage:

```rust
fn key<'a>(&'a self, storage: &'a Storage) -> Key<'a>;
```

Apply the same ownership principle to values and related accessors. The cursor
owns a budgeted encoded read window, including its dictionary. Rust prevents
mutable navigation while a returned reference is still in use, allowing the
cursor to replace the window on advancement. Rewind and seeks use logical
bookmarks against a held batch snapshot, not pointers into discarded scratch.
Resident fences identify candidate chunks before loading them.

This preserves `seek_key`, `step_key`, `seek_val`, `step_val`, and `map_times` as
the algorithm-facing operations. It is a proposed trait change, not a drop-in
implementation of the current trait. Audit cursor combinators, batch/trace
wrappers, and operator call sites that hold keys or values across mutable
navigation or `map_times`. Shorten those borrows where possible, and otherwise
copy a semantic key/value for the operation. Measure copying for wide rows and
joins. Prototype whether to evolve the upstream trait or introduce a parallel
windowed trait before committing to the consumer migration.

The windowed cursor must preserve groups across chunk and dictionary boundaries
and merge contributions from the relevant batches. It can walk one large
history through successive windows during `map_times`. Bounded memory does not
make that synchronous traversal bounded in work. Callbacks cannot retain
references into replaced windows, and consumer accumulation requires its own
budget. Multi-batch cursors must respect the transient fan-in limit rather than
hold an unrestricted number of decoded bodies.

Keep bulk probes and encoded-range reads as an additional capability for
consumers that benefit from amortized reads or run-aware execution:

```text
encoded chunks
  resident cursor: existing trait, stable storage
  windowed cursor: cursor-borrowed references, bounded read windows
  bulk reader: sorted probes and bounded encoded fragments
```

The existing `UnloadChunk` semantics supply the bulk-probe starting point.
Extend its accumulating staging contract with a byte limit and continuation
result. Fragments carry explicit key/KV continuation markers, and bookmarks
record batch, chunk, logical position, and probe position. Neither fetching a
whole batch nor staging an entire hot key is a bounded compatibility bridge.
Consumers combine contributions from all relevant batches before declaring a
group complete or a probe absent.

Validate the windowed cursor before rewriting consumers around fragments.
Consumers that require a complete, arbitrarily large group still need an
incremental algorithm or separately budgeted spillable state. Neither cursor
compatibility nor bulk reads alone bound a join's output or a reduction's
working state. Join, reduce, and peek integrations remain explicit work.

### Range operations and maintenance

The shared construction primitive is `append_range(source, logical_range)`.
It copies payload ranges, clips and rebases run ends, and joins equal boundary
runs. Compatible constants remain constants. Splits preserve semantic values
and codecs. The builder does not require an intermediate array of owned update
tuples. Finished destination chunks are settled as they are produced.

**Merge.** Compare keys once per key group and values once per KV group. For
equal KV groups, merge timestamp updates, combine equal timestamps using the
diff's algebra, and discard zeros. Transfer non-overlapping chunk handles where
ordering permits. Partial overlap requires copying or rewriting only the
affected ranges. Respect the chunk driver's shared merge horizon: do not emit
updates that could still collide with an unseen continuation in the other
input. A transferred chunk must still pass through any required time
advancement, packing, and lifecycle accounting.

**Extract.** Partition by the timestamp antichain, not by lexicographic timestamp
comparison. Constant-time ranges can be classified together. Otherwise visit
time runs and append qualifying logical ranges to the respective outputs,
maintaining the residual frontier. Extraction cannot treat a chunk's first and
last sort keys as a temporal min/max summary.

**Advance.** Apply lattice advancement and reconsolidate each KV history.
Advancement need not preserve timestamp sort order, especially for product
timestamps. Updates on opposite sides of a chunk boundary can become equal.
Do not publish a completed advanced history until all potentially colliding
input has been considered.

Trunk's `ColumnChunk::advance` concatenates a withheld KV group and sorts its
advanced times in memory. Its output chunks are bounded, but the input carry
and sort scratch can grow with the full history. This proposal requires an
external consolidation path: keep the withheld input as chunk handles, advance
and sort bounded windows into temporary sorted runs, then merge those runs with
bounded fan-in and settle output incrementally. The common total-order case may
use a cheaper proven specialization. Generic correctness cannot depend on it.

This also requires resumable advancement state in the maintenance driver.
Differential 0.25.1's static `Chunk::advance` has no explicit state or fuel, and
`ChunkBatchMerger` treats the final `done=true` call as completion. Extend that
interface to distinguish end of input from completion and retain spillable
temporary runs across fueled steps. Keep that state in the merger, not in a
public `Chunk` variant that violates the sorted/consolidated invariant. This is
an upstream dependency of bounded generic compaction. Memory-bounded external
sorting may require multiple passes, so this path cannot promise linear work
for arbitrary timestamp histories.

**Settle.** Finalize encoding, combine underfilled neighbors when worthwhile,
cut oversized bodies, and transfer committed bodies to the pool. Preserve
generation and compression policy on forwarded chunks. Sizing uses encoded
bytes and a logical-update ceiling, so very compressible runs cannot create
unbounded single-step work. Repacking must avoid repeatedly decoding settled
bodies or undoing the handle-forwarding benefit.

The dependency documents grading in terms of `len()` and `TARGET`, while the
existing `ColumnChunk` packs by bytes. Specify a shared byte/work grading
contract before reusing packing helpers. `len()` must remain the logical update
count for batch statistics and accounting, not become an encoded-byte count.

### Memory and accounting

The body-residency goal is a bound on pool slots plus admitted read windows,
builders, and sort scratch. Compressed-resident pool backing has its own budget.
Resident fences and handle lists scale with chunk count. Large individual
records have a separately reported exception. A claim that total process RSS
is bounded solely by the pool-slot budget would therefore be incorrect.

Readers and mergers reserve transient memory before opening bodies. A
multi-batch scan must cap its loaded fan-in or use temporary merge passes rather
than allocate one full body for every batch indefinitely. Concurrent readers
share a transient budget. Output backpressure keeps pending fragments bounded.
Snapshots can retain old batches, but retaining a snapshot must not retain a
decoded copy of every body it has visited.

Arrangement metrics distinguish logical updates, encoded payload bytes,
resident metadata, pool residency, and transient reservations. Shared bodies
must not be double-counted as independent physical allocations. Key/value
cardinality counters must count semantic groups, including boundary
continuations, rather than independently encoded runs.

### Integration and rollout

The first production target is the whole-row key/value compute arrangement.
The representation remains generic enough for key-only batches and non-scalar
diffs. In particular, [reduce](../../../src/compute/src/render/reduce.rs) already
uses specialized diff containers, so replacing every diff with an `i64` column
would be a regression in capability.

`ArrangementFlavor` and `CollectionBundle` currently carry concrete trace
types. Add a representation choice at that boundary with backend-specific
arranged streams and trace handles. Dispatch whole read or operator setup
operations through that facade, keeping hot inner loops monomorphized. An
imported arrangement retains its producer's representation. Consumers of mixed
backends use semantic cursor operations, with bulk fragments where beneficial.
The same facade must cover exported trace handles, snapshots, and entered-scope
timestamp wrappers.
Changing the existing batcher enum alone is insufficient.

Implement in these reviewable stages:

1. Encoded payload, range builder, and resident compatibility cursor, with
   round-trip and algebraic checks against flat updates and `ord_neu`.
2. Thread-safe encoded chunks, shared chunk batches, row dictionaries, and pool
   serialization. Keep their input encoded through sealing and maintenance.
3. Prototype the cursor lifetime change through wrappers and representative
   consumers. Add bounded windows, bulk probe/scan support, and resumable generic
   advancement. Validate skew, product timestamps, memory reservations, and
   frontier wrappers before claiming larger-than-memory support.
4. Compute arrangement facade and join, reduce, and peek consumers, retaining
   cursor-based algorithms where practical. Resident compatibility adapters do
   not qualify a consumer for spill-enabled rollout if they collect an unbounded
   group. Add bulk execution selectively after the windowed cursor evaluation.
5. Integrate remaining arrangement families and measure the adoption criteria.

A new arrangement-representation flag defaults off in production and on in
the relevant test/CI configuration through `system_parameter_default`. Keep
coverage for both backends and mixed imported arrangements. Resolve the backend
at arrangement construction. Disabling the flag affects new dataflows and does
not convert live batches. Retain `ord_neu` until correctness and performance
gates pass, with recreation of dataflows as the rollback mechanism.

## Minimal Viable Prototype

No prototype is included in this documentation change. The first experiment
should establish whether the payload and reader boundary justify the consumer
changes before integrating an entire compute graph.

Build one generic chunk implementation supporting `Row` keys and values,
ordinary and product timestamps, and representative scalar and compound diffs.
Exercise it through a real chunk batcher and fueled spine. Compare with the
current `DatumContainer`/`OffsetOptimized` row-spine layout, a dense flat chunk,
and a chunk-local `ord_neu` payload where practical. Match chunk sizes, pool
budgets, dictionary settings, input ordering, and compaction frontiers.

Implement the resident adapter against today's cursor trait first, then carry
the cursor-borrowed lifetime through a merged trace cursor, timestamp wrappers,
and representative join, reduce, and peek call sites. Record which algorithms
remain intact, which need owned keys/values, and their allocation cost. This
experiment decides the compatibility API before broad fragment-based rewrites.

| Workload | Property to establish |
| --- | --- |
| Unique keys, one value/time per key | Dense and constant encodings avoid per-update run metadata and compete with implicit trie offsets. |
| Many values per key, long KV histories | Payload sharing and group navigation survive chunking. |
| Repeated time/diff values across keys | Independent column runs retain singleton-sharing opportunities. |
| Irregular values, timestamps, and diffs | Dense fallback limits encoding overhead. |
| Sparse probes and full scans over spilled batches | Fence skipping, bounded staging, and explicit continuation work. |
| One KV history larger than the read budget | Compaction and reads avoid a whole-history allocation. |
| Product timestamps with advancement collisions | Reordering and cross-chunk consolidation are correct. |
| Narrow keys, wide rows, incompatible dictionaries | Fence overhead and dictionary transcoding costs are visible. |
| All-readied, all-retained, and alternating frontier partitions | Unchanged handles survive the handoff, mixed chunks are rewritten correctly, and no batch-wide dictionary pass is hidden in the builder. |
| Huge whole-row runs alongside frequent datums in distinct rows | Dictionary selection reflects physical payload entries and includes decoder-table cost. |
| Concurrent readers retaining snapshots during merges | Cross-thread ownership and transient accounting remain valid. |
| Cursor seeks, rewind, and histories crossing different dictionaries | Window replacement preserves navigation and semantic values without retaining previously visited bodies. |

Property-based checks should vary every legal chunk split, encoding combination,
and read pause boundary. Compare decoded updates and timestamped answers after
merge, frontier extraction, advancement, and serialization. Include empty
batches, unit columns, cancellation at boundaries, seeks followed by rewind,
and a value run crossing two keys. Compile-time checks cover required
`Send + Sync` batch types.
For the windowed API, add compile-fail coverage for retaining a borrowed key
across cursor mutation. Exercise long scans while checking that reader-owned
decoded memory does not grow with the number of visited chunks.

Measure retained bytes and peak scratch separately, bytes copied/decoded,
chunk reads and forwards, comparisons per group, hydration time, merge
throughput, and probe/scan latency. Use Feature Benchmark for individual SQL
operations and Parallel Benchmark for sustained mixed reads and writes. SQL
coverage should exercise joins, reductions, peeks, recursion, and retractions
with both representations. The bounded-state case must exceed the configured
pool and transient budgets, not merely fit in a smaller synthetic fixture.

## Alternatives

### Chunked containers underneath `ord_neu`

This preserves its builder, cursor, merger, and layout customizations. However,
the trie still spans the batch, with key/value/update layers that need not have
matching physical boundaries. It does not directly provide independently
transferable whole-update chunks, and its borrowed cursor still needs a
residency solution. Prefer this option if allocator-level chunking proves
sufficient and end-to-end chunk maintenance is no longer a requirement.

### A small `ord_neu` trie inside each chunk

This is the strongest alternative. It preserves local navigation, singleton
sharing, and compressed offsets while exposing chunks to the pool. Splitting,
range copying, cross-chunk compaction, and bounded reads still require work.
The existing merger builds trie output rather than forwarding encoded ranges.
It is a valid fallback if the new payload's navigation or dictionary costs
outweigh its maintenance advantages. The outer ownership and read contracts
should support either payload without a new compute-wide migration.

### Dense flat chunks with block compression

This reuses the current `ColumnChunk` payload and lets the pool compress repeated
bytes. It is a useful control and possibly a first integration step. It retains
duplicate K/V/T/D values in decoded windows and does not expose run boundaries
for navigation or merge comparisons. Block compression remains complementary
to structural encoding rather than a replacement for the proposed range API.

### RLE for every column

Unique values would gain an offset for every update. Require constant and dense
cases and include metadata in the selection policy.

### Permanently cache bodies for legacy cursors

This minimizes consumer changes but accumulates decoded memory as scans visit
chunks. It fails the body-residency requirement. A bounded ownership-aware
reader or an explicitly budgeted pinning API is necessary.

## Open questions

1. What resident throughput, memory, and probe-latency regression limits decide
   between independent column runs and a chunk-local trie?
2. What exact upstream API should carry resumable advancement state, fuel, and
   completion, and how should byte/work grading be specified?
3. Should cursor-borrowed references replace the current upstream trait or use a
   parallel windowed trait? How many consumers need owned keys/values across
   cursor mutation, and where does bulk execution justify an algorithm change?
   Which consumers need additional spillable state for a single large group?
4. What byte and logical-update targets balance fence overhead, sparse-read
   amplification, and merge work? How should oversized individual records be
   admitted and reported?
5. Does chunk-local dictionary construction pay for itself in young batcher
   generations, and does sharing an immutable dictionary across a bounded
   cohort save enough space to justify its ownership and accounting costs?
6. What representation facade gives local, imported, and entered arrangements
   one coherent integration point without excessive dispatch or code growth?

## Decisions

This proposal recommends owning the encoded chunk payload while retaining the
existing chunk batch/spine architecture and pool. Whole-value columns are the
encoding boundary. Bounded reads and generic compaction are required parts of
the implementation. Prototype resident and windowed cursor compatibility before
broad consumer rewrites, retaining bulk reads as an additional capability.
Choosing this payload over chunk-local `ord_neu` remains subject to the prototype
and review. No implementation approval or performance result is implied by this
document.
