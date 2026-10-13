# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.25.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.25.0...differential-dataflow-v0.25.1) - 2026-07-15

### Other

- assert arriving batches span neither preload nor acknowledged
- Restore strongly_connected_at; add prioritized SCC regression tests
- process in-flight batches overtaken by advance_upper

## [0.25.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.24.0...differential-dataflow-v0.25.0) - 2026-07-15

### Added

- `operators::common`: shared building blocks for differential operators — `TimeHistory`, `bilinear_wave`, `tile_descriptions`, `KeyView`, and `discover_times`. ([#794](https://github.com/TimelyDataflow/differential-dataflow/pull/794))
- `TraceReader::batches_through`, exposing the batches up to a given frontier for the tactic-driven operators. ([#773](https://github.com/TimelyDataflow/differential-dataflow/pull/773))
- `formal/`: machine-checked Lean models of differential's coverage and compaction arguments, plus a model-derived `reference` reduce tactic used as a differential oracle in tests. ([#776](https://github.com/TimelyDataflow/differential-dataflow/pull/776), [#780](https://github.com/TimelyDataflow/differential-dataflow/pull/780))

The following surface is new and **experimental**: it is public to enable experimentation, but the API is unstable and expected to change.

- `Chunk` trait abstracting the container that backs collections, merge batching, and batches, with the `columnar` module retargeted onto it. ([#744](https://github.com/TimelyDataflow/differential-dataflow/pull/744), [#769](https://github.com/TimelyDataflow/differential-dataflow/pull/769), [#778](https://github.com/TimelyDataflow/differential-dataflow/pull/778))
- Public `JoinTactic` and `ReduceTactic` traits, letting callers supply per-batch join and reduce strategies; documented with asserted contracts. ([#789](https://github.com/TimelyDataflow/differential-dataflow/pull/789), [#790](https://github.com/TimelyDataflow/differential-dataflow/pull/790), [#791](https://github.com/TimelyDataflow/differential-dataflow/pull/791))
- Integer-proxy chunks: a backend-agnostic join and reduce over `(key_hash, value_id)` pairs. ([#781](https://github.com/TimelyDataflow/differential-dataflow/pull/781))

### Changed

- Update to timely 0.31. ([f7c799c](https://github.com/TimelyDataflow/differential-dataflow/commit/f7c799c9))
- Move layout onto the `Cursor`. Cursors now carry the key, value, time, and diff types and their containers as associated types; `BatchReader` and `TraceReader` retain only `Time`. Navigation becomes an optional cursor capability rather than a universal one. ([#771](https://github.com/TimelyDataflow/differential-dataflow/pull/771), [#772](https://github.com/TimelyDataflow/differential-dataflow/pull/772))
- Generalize `join` and `reduce` into navigation-free drivers over pluggable tactics. `join_core`, `reduce_abelian`, and `reduce_core` gain type parameters for the tactic and output container. ([#773](https://github.com/TimelyDataflow/differential-dataflow/pull/773), [#790](https://github.com/TimelyDataflow/differential-dataflow/pull/790))
- Rework `MergeBatcher` merge effort: weigh the geometric ladder by update counts, split `Merger::account`, and add `Merger::len`. ([#767](https://github.com/TimelyDataflow/differential-dataflow/pull/767))
- Name the shared key container in `join` and `reduce` to fix HRTB normalization for non-reference key containers. ([#797](https://github.com/TimelyDataflow/differential-dataflow/pull/797), [#798](https://github.com/TimelyDataflow/differential-dataflow/pull/798))
- Reorganize the experimental `columnar` module: the 0.24 `spill`, `batcher`, `builder`, `exchange`, and `arrangement` submodules give way to `layout`, `updates`, `trie_merger`, `collection`, and `trace`. Many 0.24 public types (`ValBuilder`, `ValPact`, `ValChunker`, `RecordedUpdates`, `Coltainer`, the `Spill` / `Fetch` / `SpillPolicy` traits) are removed or relocated. ([#769](https://github.com/TimelyDataflow/differential-dataflow/pull/769), [#781](https://github.com/TimelyDataflow/differential-dataflow/pull/781))

### Removed

- The `LayoutExt` trait, and its supertrait bound on `BatchReader`, `TraceReader`, and `Cursor`. ([#772](https://github.com/TimelyDataflow/differential-dataflow/pull/772))
- `TraceReader::Cursor`, `TraceReader::Storage`, and `BatchReader::Cursor` associated types, along with `BatchReader::cursor`; `CursorList<Batch::Cursor>` is hard-wired instead. ([#771](https://github.com/TimelyDataflow/differential-dataflow/pull/771))

This is a heavily breaking release.
Layout information moves off the trace and batch types onto the `Cursor`, which now carries the key, value, time, and diff types and their containers as associated types; `BatchReader` and `TraceReader` retain only `Time`.
Navigation becomes an optional cursor capability, and `join` and `reduce` are rebuilt as navigation-free drivers parameterized by the public `JoinTactic` / `ReduceTactic` traits, so `join_core`, `reduce_abelian`, and `reduce_core` gain tactic and container type parameters.
Downstream cursor, trace, and batch implementations must migrate to the associated-type form and drop the removed `LayoutExt` supertrait.
The experimental `columnar` module is reorganized and most of its 0.24 public surface relocated.
The `Chunk` trait, the `JoinTactic` / `ReduceTactic` traits, and the integer-proxy chunks are new and explicitly experimental: they are public to enable experimentation, but their APIs are unstable and expected to change.
Dependency tracking follows timely 0.31.

## [0.24.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.23.0...differential-dataflow-v0.24.0) - 2026-05-29

### Added

- New experimental `columnar` module exposing public (but explicitly unstable) columnar arrangement and container infrastructure: `UpdatesTyped`, `RecordedUpdates`, `ValPact`, `ValBatcher`, `ValBuilder`, `ValChunker`, `ValSpine`, plus dynamic-scope helpers (`DynTime`, `leave_dynamic`). See the module docs for the current rough edges. ([#730](https://github.com/TimelyDataflow/differential-dataflow/pull/730))
- Columnar serialization for trace transport: `UpdatesView` reader, `UpdatesTyped` writer, and a `Stash`-backed `RecordedUpdates` with a `ContainerBytes` impl. ([#733](https://github.com/TimelyDataflow/differential-dataflow/pull/733))
- Columnar spilling merge batcher with pluggable `Spill` / `Fetch` / `SpillPolicy` traits parameterized over a chunk type. The `columnar_spill` example demonstrates a disk-backed, lz4-compressed spill path. ([#741](https://github.com/TimelyDataflow/differential-dataflow/pull/741))
- `Cursor::populate_key` for key-positioned bulk load into an `EditList`, with an optional time `meet` to advance times before consolidation. Used by `reduce` and `ValueHistory`. ([#725](https://github.com/TimelyDataflow/differential-dataflow/pull/725))

### Changed

- Update to timely 0.30 and columnar 0.13. (bf9b36f)
- Separate the chunker from the batcher. The `Batcher` trait drops its `Input` associated type and `push_container` method and now accepts pre-chunked input via `PushInto<Self::Output>`. `arrange_core` gains `C` (input container) and `Chu: ContainerBuilder` (chunker) type parameters so chunking lives in the operator. The high-level `.arrange::<Ba, Bu, Tr>()` callsite is unchanged; cross-container callers (columnar, interactive, spill) drop to `arrange_core` directly. ([#625](https://github.com/TimelyDataflow/differential-dataflow/pull/625))
- `Batcher::seal` no longer takes a `B: Builder` type parameter; the builder is supplied at the call site. ([#745](https://github.com/TimelyDataflow/differential-dataflow/pull/745))
- Replace the `Cursor` generic argument with associated types (`Key`, `Val`, `Time`, `Diff`), simplifying trait bounds throughout. ([#726](https://github.com/TimelyDataflow/differential-dataflow/pull/726))
- Reduce moments of owned `Time` in `join`; improve use of `join_assign`; add the missing `PointStamp::meet_assign`. ([#727](https://github.com/TimelyDataflow/differential-dataflow/pull/727))
- Restore the pre-#725 division of labor in `EditList::load`: the join inner loop again walks vals inline from the cursor's current position rather than re-seeking on every key, recovering a ~3s regression in the spines benchmark. `Cursor::populate_key` retains the seek for callers (`reduce`, `ValueHistory`) that need it. ([#732](https://github.com/TimelyDataflow/differential-dataflow/pull/732))

### Removed

- `InternalMerge*` types and traits; `VecMerger` moves to its own independent module. ([#740](https://github.com/TimelyDataflow/differential-dataflow/pull/740))
- Spring cleaning: remove `ShutdownDeadmans`, `TraceRc` and `trace/wrappers/rc.rs`, `chainless_batcher.rs`, `RHH`, the Huffman container, the `Clone` constraint on traces, redundant trait bounds, dead and commented code, and needless borrows; convert `::max_value()` to `::MAX`. ([#724](https://github.com/TimelyDataflow/differential-dataflow/pull/724))

### Other

- Spine benchmark example improvements. `examples/spines.rs` is restored as a `key` / `val` / `col` arrangement bake-off using the new `columnar` module via an in-dataflow `ToRecorded` repacker. ([#732](https://github.com/TimelyDataflow/differential-dataflow/pull/732))
- New `examples/scc.rs` for comparative benchmarking; use `meet_assign` in `ValueHistory::replay`. ([#731](https://github.com/TimelyDataflow/differential-dataflow/pull/731))

This is a heavily breaking release driven by tracking timely 0.30 and introducing the experimental `columnar` module.
The `Batcher` trait no longer knows about input containers; chunking moves into `arrange_core`, which gains two new type parameters. Direct callers of `arrange_core` need to supply a chunker; users of `.arrange::<Ba, Bu, Tr>()` are unaffected.
The `Cursor` trait swaps its generic argument for associated types — downstream cursor implementations need to migrate from `impl<K, V, T, D> Cursor for ...` to associated-type form.
`TraceRc`, `RHH`, `HuffmanContainer`, and the `InternalMerge*` family are gone; consumers should move to the columnar-backed equivalents (when applicable) or hold their own `Rc<TraceBox<_>>`.

## [0.23.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.22.0...differential-dataflow-v0.23.0) - 2026-04-13

### Changed

- Update to timely 0.29 ([#714](https://github.com/TimelyDataflow/differential-dataflow/pull/714), [#718](https://github.com/TimelyDataflow/differential-dataflow/pull/718), [#720](https://github.com/TimelyDataflow/differential-dataflow/pull/720))
- Remove scope generic from `Arranged` (now `Arranged<'scope, Tr>` instead of `Arranged<G, Tr>` where `G: Scope`), simplifying trait bounds throughout ([#714](https://github.com/TimelyDataflow/differential-dataflow/pull/714))
- Add explicit `'scope` lifetime parameter to `VecCollection`, `Arranged`, and related types ([#718](https://github.com/TimelyDataflow/differential-dataflow/pull/718))
- Scopes are now passed by value rather than by reference (`enter(scope)` instead of `enter(&scope)`) ([#720](https://github.com/TimelyDataflow/differential-dataflow/pull/720))
- `reduce_abelian` and `reduce_core` accept a container/builder parameter for output, removing the `KeyOwn` requirement from trace bounds ([#710](https://github.com/TimelyDataflow/differential-dataflow/pull/710))

### Removed

- `TimelyStack` container and all dependent types: `TStack` layout, `ColumnationChunker`, `ColInternalMerger`, and `Col*` type aliases (`ColValSpine`, `ColKeySpine`, etc.) ([#715](https://github.com/TimelyDataflow/differential-dataflow/pull/715))

### Other

- Substantial cleanup of `reduce.rs`: remove ~300 lines of dead code, simplify conditional logic, replace silent error swallowing with panics ([#709](https://github.com/TimelyDataflow/differential-dataflow/pull/709))
- Columnar support refinements ([#704](https://github.com/TimelyDataflow/differential-dataflow/pull/704))

This is a heavily breaking release driven by tracking timely 0.29.
The `Arranged` type loses its scope generic and gains an explicit scope lifetime.
Scope parameters throughout the API shift from generic `S: Scope` to concrete types, and scopes are passed owned rather than borrowed.
Users of `Col*` type aliases should migrate to columnar-backed equivalents.
The `reduce` family of functions now takes an explicit output container parameter, which removes the need for `KeyOwn` bounds on traces.

## [0.22.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.21.2...differential-dataflow-v0.22.0) - 2026-04-07

### Other

- Allow `InternalMerger::extract` to yield when full buffer ([#711](https://github.com/TimelyDataflow/differential-dataflow/pull/711))

## [0.21.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.21.0...differential-dataflow-v0.21.1) - 2026-03-26

### Other

- InternalMerger adopts tail chunks rather than copy them
- Tidy up structure of logic
- Dial in buffer sizing
- Introduce VecMerger to efficiently merge owning vectors

## [0.21.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.20.0...differential-dataflow-v0.21.0) - 2026-03-25

### Added

- `Arranged::as_container` convenience method for extracting non-`Vec` containers from arrangements ([#695](https://github.com/TimelyDataflow/differential-dataflow/pull/695))
- `Columnation` and `Columnar` implementations for the `Present` zero-sized difference type ([#682](https://github.com/TimelyDataflow/differential-dataflow/pull/682))

### Changed

- Update to timely 0.28 and columnar 0.12 ([#698](https://github.com/TimelyDataflow/differential-dataflow/pull/698))
- Adopt internal iteration for consolidation, merge batching, and trace building, replacing external iteration that leaked internal types through abstractions ([#689](https://github.com/TimelyDataflow/differential-dataflow/pull/689))

### Fixed

- Eliminate a redundant `arrange` in `propagate` and a double scope-import in `scc` ([#692](https://github.com/TimelyDataflow/differential-dataflow/pull/692))

The main theme of this release is internal iteration: consolidation, merging, and trace building now happen behind trait methods rather than exposing container internals outward.
Dependency updates track timely 0.28 and columnar 0.12.

## [0.20.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.19.1...differential-dataflow-v0.20.0) - 2026-03-05

### Other

- Update release notes for 0.20 ([#680](https://github.com/TimelyDataflow/differential-dataflow/pull/680))
- Pull in probe changes ([#677](https://github.com/TimelyDataflow/differential-dataflow/pull/677))
- Break `Variable` into `(Variable, Collection)` ([#674](https://github.com/TimelyDataflow/differential-dataflow/pull/674))
- Update github workflows ([#673](https://github.com/TimelyDataflow/differential-dataflow/pull/673))
- Set MSRV to 1.86 ([#672](https://github.com/TimelyDataflow/differential-dataflow/pull/672))
- Differential corrected atop candidate timely 0.27 ([#671](https://github.com/TimelyDataflow/differential-dataflow/pull/671))
- Test mdbook like a crate ([#669](https://github.com/TimelyDataflow/differential-dataflow/pull/669))
- Migrate Join logic away from traits ([#668](https://github.com/TimelyDataflow/differential-dataflow/pull/668))
- More `VecCollection` demotion ([#667](https://github.com/TimelyDataflow/differential-dataflow/pull/667))
- Implement utility traits for Rc collections ([#666](https://github.com/TimelyDataflow/differential-dataflow/pull/666))
- Deprioritize the `Vec` container ([#664](https://github.com/TimelyDataflow/differential-dataflow/pull/664))

Substantial superficial but breaking changes due to the adoption of timely `0.27`.
Primarily that the `Stream` contained by `Collection` is not `Clone` by default, and is now conventionally passed owned rather than by reference.
The necessary fix-ups are to convert to owned collections, and where needed to clone the collection, which requires that its container implement `Clone`.

Related, the `Variable` types is broken into a pair `(Variable, Collection)`, where the latter is the usable collection the type would previously `Deref` to, and the former is the connection point to bind the variable's definition.
The `SemigroupVariable` was removed, as it is now redundant with `Variable::new()`, which does not require `Negate` (the main difference between the two variables).

Several steps were taken to "deprioritize" collections with vector containers.
Many traits defined only on vector collections were removed in favor of inherent methods.
Traits and methods that produce vector collections were (and continue to be) reconsidered to avoid routing users into that opinion.
All prior functionality is available, though in some cases it requires explicit indication of preferred container types (internally, only in "demo" cases).

The `probe` operator was updated to track timely changes that have it return a pair of handle and collection, rather than only a handle.
This aligns more with the "owned collection" idiom.


## [0.19.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.19.0...differential-dataflow-v0.19.1) - 2026-02-06

### Other

- Update to timely 0.26 ([#662](https://github.com/TimelyDataflow/differential-dataflow/pull/662))

## [0.19.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.18.0...differential-dataflow-v0.19.0) - 2026-02-06

### Other

- Back Pointstamp with SmallVec ([#661](https://github.com/TimelyDataflow/differential-dataflow/pull/661))
- Correct chainless lower bound determination ([#660](https://github.com/TimelyDataflow/differential-dataflow/pull/660))
- Exchange columns more efficiently ([#656](https://github.com/TimelyDataflow/differential-dataflow/pull/656))
- fix broken example link ([#654](https://github.com/TimelyDataflow/differential-dataflow/pull/654))

## [0.18.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.17.0...differential-dataflow-v0.18.0) - 2025-10-23

### Other

- Updates for timely 0.25 ([#647](https://github.com/TimelyDataflow/differential-dataflow/pull/647))
- Replace IndexMap with BTreeMap ([#652](https://github.com/TimelyDataflow/differential-dataflow/pull/652))
- Update columnar; introduce mimalloc
- Extract `VecCollection` from `Collection` ([#651](https://github.com/TimelyDataflow/differential-dataflow/pull/651))
- Introduce traits for collection containers ([#650](https://github.com/TimelyDataflow/differential-dataflow/pull/650))
- Correct some clippy nit ([#649](https://github.com/TimelyDataflow/differential-dataflow/pull/649))
- Batcher implementation that has no opinions about chains, and columnar. ([#626](https://github.com/TimelyDataflow/differential-dataflow/pull/626))

## [0.17.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.2...differential-dataflow-v0.17.0) - 2025-09-15

### Other

- Adjust Differential to recent Timely changes ([#643](https://github.com/TimelyDataflow/differential-dataflow/pull/643))
- Remove `Filter` and `Freeze` wrappers ([#644](https://github.com/TimelyDataflow/differential-dataflow/pull/644))

## [0.16.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.1...differential-dataflow-v0.16.2) - 2025-08-28

### Other

- update Cargo.toml dependencies

## [0.16.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.16.0...differential-dataflow-v0.16.1) - 2025-08-16

### Other

- update Cargo.toml dependencies

## [0.16.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.4...differential-dataflow-v0.16.0) - 2025-08-07

### Other

- Simplify type arguments to reduce, make Upds fields pub ([#632](https://github.com/TimelyDataflow/differential-dataflow/pull/632))

## [0.15.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.3...differential-dataflow-v0.15.4) - 2025-08-05

### Other

- Inline consolidate fast-path ([#629](https://github.com/TimelyDataflow/differential-dataflow/pull/629))
- Remove `BatchContainer::borrow_as()` ([#628](https://github.com/TimelyDataflow/differential-dataflow/pull/628))
- `Layout` extension trait ([#627](https://github.com/TimelyDataflow/differential-dataflow/pull/627))
- Remove `IntoOwned` (phase 1) ([#624](https://github.com/TimelyDataflow/differential-dataflow/pull/624))
- Remove redundant bounds ([#623](https://github.com/TimelyDataflow/differential-dataflow/pull/623))
- Update comment
- Use trie abstractions for batch implementations ([#616](https://github.com/TimelyDataflow/differential-dataflow/pull/616))
- Bump columnar to 0.8.0 ([#620](https://github.com/TimelyDataflow/differential-dataflow/pull/620))
- Use BTreeMap to avoid sorting ([#615](https://github.com/TimelyDataflow/differential-dataflow/pull/615))
- Respect singleton counts in merge effort ([#614](https://github.com/TimelyDataflow/differential-dataflow/pull/614))
- Demonstrate `Columnar` batch builder ([#602](https://github.com/TimelyDataflow/differential-dataflow/pull/602))

## [0.15.3](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.2...differential-dataflow-v0.15.3) - 2025-06-24

### Other

- Update columnar to 0.6 ([#611](https://github.com/TimelyDataflow/differential-dataflow/pull/611))
- Remove use of borrow_as from trace wrappers ([#609](https://github.com/TimelyDataflow/differential-dataflow/pull/609))
- Modernize many where constraints ([#610](https://github.com/TimelyDataflow/differential-dataflow/pull/610))
- Added example commands to examples/multitemporal.rs ([#607](https://github.com/TimelyDataflow/differential-dataflow/pull/607))

## [0.15.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.1...differential-dataflow-v0.15.2) - 2025-05-19

### Other

- Make module ord_neu::val_batch public ([#603](https://github.com/TimelyDataflow/differential-dataflow/pull/603))

## [0.15.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.15.0...differential-dataflow-v0.15.1) - 2025-05-09

### Other

- Update columnar to 0.5 ([#600](https://github.com/TimelyDataflow/differential-dataflow/pull/600))

## [0.15.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.2...differential-dataflow-v0.15.0) - 2025-05-09

### Other

- Update timely dependence to `0.21`. ([#599](https://github.com/TimelyDataflow/differential-dataflow/pull/599))
- Document the collection invariant ([#384](https://github.com/TimelyDataflow/differential-dataflow/pull/384))
- Modernize `Cursor` API ([#596](https://github.com/TimelyDataflow/differential-dataflow/pull/596))

## [0.14.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.1...differential-dataflow-v0.14.2) - 2025-04-06

### Other

- Make builder::result members public ([#591](https://github.com/TimelyDataflow/differential-dataflow/pull/591))

## [0.14.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.14.0...differential-dataflow-v0.14.1) - 2025-03-28

### Other

- Update columnar to 0.4 ([#589](https://github.com/TimelyDataflow/differential-dataflow/pull/589))

## [0.14.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.7...differential-dataflow-v0.14.0) - 2025-03-28

### Other

- Test against TD with linear reachability ([#588](https://github.com/TimelyDataflow/differential-dataflow/pull/588))
- Remove deprecated functions and use statements ([#582](https://github.com/TimelyDataflow/differential-dataflow/pull/582))

## [0.13.7](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.6...differential-dataflow-v0.13.7) - 2025-02-28

### Other

- Move changelog to differential-dataflow crate ([#581](https://github.com/TimelyDataflow/differential-dataflow/pull/581))
- Move differential crate from . to directory ([#575](https://github.com/TimelyDataflow/differential-dataflow/pull/575))

## [0.13.6](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.5...differential-dataflow-v0.13.6) - 2025-02-12

### Other

- Update Timely, Columnar versions ([#569](https://github.com/TimelyDataflow/differential-dataflow/pull/569))
- Variable supports container streams ([#564](https://github.com/TimelyDataflow/differential-dataflow/pull/564))
- Fix Timely master check, pin dependency version in mdbook ([#568](https://github.com/TimelyDataflow/differential-dataflow/pull/568))
- Provide prescriptive versions in example

## [0.13.5](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.4...differential-dataflow-v0.13.5) - 2025-01-24

### Other

- Derive columnar for point stamp ([#562](https://github.com/TimelyDataflow/differential-dataflow/pull/562))

## [0.13.4](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.3...differential-dataflow-v0.13.4) - 2025-01-23

### Other

- Update to timely 0.17 ([#561](https://github.com/TimelyDataflow/differential-dataflow/pull/561))
- Define columnation chunker for all (D,T,R) ([#559](https://github.com/TimelyDataflow/differential-dataflow/pull/559))

## [0.13.3](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.2...differential-dataflow-v0.13.3) - 2025-01-09

### Other

- Incorporate breaking changes from Timely's logging update ([#558](https://github.com/TimelyDataflow/differential-dataflow/pull/558))
- Derive columnar for log events ([#557](https://github.com/TimelyDataflow/differential-dataflow/pull/557))
- Correct capacity logic
- Demonstrate container input batching ([#556](https://github.com/TimelyDataflow/differential-dataflow/pull/556))
- Work towards `Batcher` unification ([#553](https://github.com/TimelyDataflow/differential-dataflow/pull/553))

## [0.13.2](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.1...differential-dataflow-v0.13.2) - 2024-12-18

### Other

- Update to track timely changes ([#554](https://github.com/TimelyDataflow/differential-dataflow/pull/554))
- Consolidation consolidation ([#552](https://github.com/TimelyDataflow/differential-dataflow/pull/552))
- Pass description itself to builder ([#551](https://github.com/TimelyDataflow/differential-dataflow/pull/551))
- Simplify ContainerChunker::push_into ([#549](https://github.com/TimelyDataflow/differential-dataflow/pull/549))
- Remove time from MergeBatcher ([#550](https://github.com/TimelyDataflow/differential-dataflow/pull/550))
- Changes to track timely master ([#542](https://github.com/TimelyDataflow/differential-dataflow/pull/542))
- Remove (key, val) structure from merge batchers ([#548](https://github.com/TimelyDataflow/differential-dataflow/pull/548))
- Merge batcher for flat container without key and value ([#547](https://github.com/TimelyDataflow/differential-dataflow/pull/547))
- Move `Batcher::seal` to `Builder` ([#546](https://github.com/TimelyDataflow/differential-dataflow/pull/546))
- Extract `Builder` from `Trace` ([#545](https://github.com/TimelyDataflow/differential-dataflow/pull/545))
- Remove batcher from Trace ([#544](https://github.com/TimelyDataflow/differential-dataflow/pull/544))
- Build against timely master ([#539](https://github.com/TimelyDataflow/differential-dataflow/pull/539))

## [0.13.1](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.13.0...differential-dataflow-v0.13.1) - 2024-11-11

### Other

- Changes to track timely's [#597](https://github.com/TimelyDataflow/differential-dataflow/pull/597) ([#538](https://github.com/TimelyDataflow/differential-dataflow/pull/538))

## [0.13.0](https://github.com/TimelyDataflow/differential-dataflow/compare/differential-dataflow-v0.12.0...differential-dataflow-v0.13.0) - 2024-10-29

Changelog bankruptcy declared.
