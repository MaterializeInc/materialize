# adapter (mz-adapter crate)

The `mz-adapter` crate is Materialize's coordinator: the central component that
receives SQL commands from clients, plans and optimizes them, assigns timestamps,
drives catalog transactions, and dispatches work to the storage and compute layers.
Approximately 71,323 LOC (Rust source + tests).

## Structure

```
src/adapter/
  src/                  # crate source root; see src/CONTEXT.md
    coord/              # Coordinator struct + all sub-modules (~28,576 LOC)
    catalog/            # Adapter catalog layer (~16,458 LOC)
    optimize/           # Optimizer pipelines (~2,731 LOC)
    explain/            # EXPLAIN support (~1,116 LOC)
    config/             # System-param sync (~621 LOC)
    *.rs                # Top-level modules (~21,166 LOC)
  tests/                # Integration tests
  benches/              # Benchmarks
```

## Key public surface

- **`serve(Config) -> Handle`** — starts the coordinator; called by `mz-environmentd`.
- **`Client` / `SessionClient`** — consumed by `mz-pgwire` for all per-connection SQL operations.
- **`ExecuteResponse`** — the coordinator's per-statement result type.
- **`AdapterError`** — unified error type.
- **`CollectionIdBundle`, `ReadHolds`, `TimestampContext`** — re-exported for downstream consumers.

## Subsystem summary

| Subsystem | Role |
|---|---|
| `coord` | Single-threaded event loop; owns all mutable state; dispatches `Command`/`Message` |
| `catalog` | Adapter catalog: durable transactions, in-memory state, system-table row generation |
| `optimize` | `Optimize<From>` trait + per-statement pipelines; no `Coordinator` dependency |
| `explain` | `EXPLAIN` output at all IR stages; optimizer trace capture |
| `client` | External `Client`/`SessionClient` API |
| `session` | Per-connection `Session` state, transaction management |
| `config` | LaunchDarkly / JSON-file system-parameter synchronization |

## Architectural observations (from inner review)

### Verified findings worth surfacing to `src/`

1. **`Message` enum grows with staged statement types** (`src/adapter/src/coord.rs:363-406`).
   Each new staged statement type (peek, subscribe, create index, etc.) requires a new
   `Message::XxxStageReady` variant in `coord.rs` and a new (structurally identical)
   match arm in `message_handler.rs`. Currently 9 such variants; all 9 dispatch identically
   (`self.sequence_staged(ctx, span, stage)`). Collapsing to a single polymorphic
   `Message::StagedReady(Box<dyn Staged>)` would remove this per-type coupling.
   See `src/coord/sequencer/ARCH_REVIEW.md`.

2. **Parallel match arms in `Staged` impls** (`src/adapter/src/coord/sequencer/inner/peek.rs` etc.).
   Each stage enum (`PeekStage`, `SubscribeStage`, etc.) has two parallel match-on-all-variants
   methods (`validity()` and `stage()`). Adding or renaming a variant requires lockstep edits
   in both. See `src/coord/sequencer/inner/ARCH_REVIEW.md`.

### Honest skips

- Catalog dual-layer (`mz_catalog` durable + `adapter::catalog` transact/apply): boundary is clean, no friction.
- `apply_catalog_implications` size (2,090 LOC): large by coverage (one case per catalog object type), not by structural complexity; deletion test fails.
- `optimize/` independence from `Coordinator`: a strength, not a friction.

## Key dependencies

`mz-catalog`, `mz-controller`, `mz-compute-client`, `mz-storage-client`, `mz-sql`,
`mz-expr`, `mz-transform`, `mz-persist-client`, `mz-timestamp-oracle`.

## Downstream consumers

`mz-environmentd` (calls `serve`), `mz-pgwire` (uses `Client`/`SessionClient`), `mz-balancerd`.

## Cross-references

- Full subsystem detail: [`src/CONTEXT.md`](src/CONTEXT.md)
- Coord detail: [`src/coord/CONTEXT.md`](src/coord/CONTEXT.md)
- Catalog detail: [`src/catalog/CONTEXT.md`](src/catalog/CONTEXT.md)
- Generated developer docs: `doc/developer/generated/adapter/_crate.md`
