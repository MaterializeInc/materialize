---
source: src/timely-util/src/columnar/consolidate.rs
revision: a766b48485
---

# timely-util::columnar::consolidate

Provides `ConsolidatingColumnBuilder<D, T, R>`, a `ContainerBuilder` that consolidates `(D, T, R)` update triples and emits `Column<(D, T, R)>` containers.

The builder uses two-level buffering:

1. **AoS staging buffer** (`Vec<(D, T, R)>` with capacity computed by `default_staging_cap()` as `(2 * 8 KiB / size_of::<(D, T, R)>()).max(2)`, stored in `staging_cap`): incoming items are pushed here. When the buffer fills, `consolidate_and_drain` sorts and consolidates in-place using `consolidate_updates`, then drains a `staging_cap / 2` prefix so that the last in-progress consolidated key remains in staging for potential merging with the next batch.

2. **SoA accumulator** (three separate columnar sub-containers, one per column): consolidated rows are drained from staging in chunks of `DRAIN_CHUNK_ROWS` = 16 rows, with three sequential per-column passes per chunk to enable autovectorization. After each chunk the serialized size is checked via `indexed::length_in_words`; once it reaches `FLUSH_THRESHOLD_WORDS` (90% of `OUTPUT_TARGET_WORDS` = 2 MiB), `flush_aligned` serializes the accumulator into an aligned `Vec<u64>` via `indexed::encode` and enqueues it as `Column::Align`.

`ContainerBuilder::finish` drains any remaining staging entries with grain 1 (so no remainder is held back) and ships the partial accumulator as a `Column::Typed` to avoid an extra serialization copy.

`ContainerBuilder::extract` pops finished `Column::Align` containers from the pending queue one at a time.

The builder does not preserve FIFO ordering because consolidation reorders updates.
It is generic over `(D, T, R)` where `D: Data + Columnar`, `T: Data + Columnar`, `R: Semigroup + Columnar`, and the tuple decomposition `(D, T, R): Columnar<Container = (D::Container, T::Container, R::Container)>` holds.
