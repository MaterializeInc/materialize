# storage::source::generator

Load-generator source implementations: seven built-in datasets that exercise
the ingestion framework without an external system.

## Files (LOC ≈ 6,155 including generator.rs one level up)

| File | What it owns |
|---|---|
| `generator.rs` (parent) | `SourceRender` impl for `LoadGeneratorSourceConnection`; `GeneratorKind` dispatch; `render_simple_generator`; `synthesize_probes` |
| `auction.rs` (4,347 LOC) | `Auction` — seeded multi-table auction dataset (organizations, users, accounts, auctions, bids, wins); implements `Generator` |
| `key_value.rs` (645 LOC) | `KeyValueLoadGenerator` — dedicated render path (bypasses `render_simple_generator`), exposes raw key/value pairs with configurable partition count and snapshot size |
| `tpch.rs` (602 LOC) | `Tpch` — TPC-H schema (suppliers, parts, customers, orders, lineitem) |
| `marketing.rs` (337 LOC) | `Marketing` — B2B marketing dataset (customers, leads, etc.) |
| `datums.rs` (81 LOC) | `Datums` — one row per SQL scalar type; exercises decoding |
| `counter.rs` (73 LOC) | `Counter` — monotonically incrementing integer with optional max cardinality |
| `clock.rs` (70 LOC) | `Clock` — wall-clock rows at a tick interval |

## Key concepts

- **`Generator` trait** *(defined in `mz_storage_types::sources::load_generator`)* — one method: `by_seed(now_fn, seed, resume_offset) -> impl Iterator<Item=(LoadGeneratorOutput, Event)>`. Implementations are pure iterators; all timely/dataflow concerns live in `generator.rs`.
- **`GeneratorKind`** — two variants: `Simple { Box<dyn Generator>, tick_micros, as_of, up_to }` and `KeyValue(KeyValueLoadGenerator)`. `KeyValue` bypasses `render_simple_generator` and has its own timely operator in `key_value.rs`.
- **`synthesize_probes`** — fallback for generators that have no upstream system frontier; derives probe timestamps from a progress stream at the configured `timestamp_interval`.
- **`as_of` / `up_to`** — generators support bounded replay: fast-forward data before `as_of`, stop at `up_to`. Used for backfill and oneshot ingestion.
- **Seeded randomness** — all generators use `rand_8::SmallRng::seed_from_u64`; seeds are derived from the now-fn at startup, enabling reproducibility.

## Cross-references

- `mz_storage_types::sources::load_generator::{Generator, Event, LoadGeneratorOutput}` — the trait and protocol types.
- `generator.rs` is the module entry point one directory above; this subdir contains only the per-dataset impls.
- Generated developer docs: `doc/developer/generated/storage/source/generator/`.
