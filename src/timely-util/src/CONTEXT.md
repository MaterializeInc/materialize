# timely-util::src

See [`../CONTEXT.md`](../CONTEXT.md) for crate-level overview.

## Module surface (LOC ≈ 5,820)

| Module | LOC | Purpose |
|---|---|---|
| `reclock.rs` | 1,239 | Timestamp translation (`FromTime` → `IntoTime`) via remap collection; formal correctness invariants in doc comments |
| `builder_async.rs` | 895 | Async timely operator construction: `OperatorBuilder`, `AsyncInputHandle`, `Button` shutdown token |
| `operator.rs` | 774 | `StreamExt` / `CollectionExt` extension traits; `consolidate_pact`, `ConcatenateFlatten`, stream expiry |
| `columnation.rs` | 666 | `ColumnationStack`, `ColInternalMerger` — region-based storage for differential merging |
| `order.rs` | 437 | `Partitioned<P,T>`, `Interval<P>`, `Reverse<T>`, `refine_antichain` |
| `temporal.rs` | 348 | `BucketChain` — future-update storage bucketed by timestamp |
| `columnar.rs` | 299 | `Column<C>` container; `Col2ValBatcher` / `Col2KeyBatcher` type aliases |
| `probe.rs` | 207 | `Handle<T>` — async and activator-based frontier observation |
| `replay.rs` | 169 | `MzReplay` — replays captured event streams with periodic re-activation |
| `activator.rs` | 140 | `RcActivator` — threshold-gated external wakeup |
| `columnar/batcher.rs` | 116 | `Chunker` — batched aligned allocations for columnar containers |
| `columnar/builder.rs` | 112 | `ColumnBuilder` — builder for `Column<C>` |
| `pact.rs` | 85 | Round-robin and hash distribution pacts |
| `antichain.rs` | 71 | Pretty-printing and utilities for antichains |
| `containers.rs` | 52 | `alloc_aligned_zeroed`, `AccountedStackBuilder` (lgalloc-aware) |
| `containers/stack.rs` | 62 | Stack allocator used by containers |
| `capture.rs` | 41 | Tokio capture adapters |
| `panic.rs` | 41 | Graceful panic handling within timely workers |
| `scope_label.rs` | 33 | Profiling scope labels |

## Key interfaces

- **`OperatorBuilder` / `AsyncInputHandle`** — async operator seam; primary
  way compute/storage builds timely operators that do async I/O.
- **`reclock` module** — the mathematical heart of source timestamp mapping;
  callers must preserve `t1 ≤ t2 ⇒ R(t1) ⪯ R(t2)`.
- **`StreamExt::map_fallible`** / **`CollectionExt::consolidate_named`** —
  most-used convenience extensions across compute and storage operators.

## Cross-references

- `timely` / `differential-dataflow` — substrate
- `mz-ore` — async utilities, metrics
- `columnar` / `columnation` / `lgalloc` — memory management
- Generated docs: `doc/developer/generated/timely-util/`
