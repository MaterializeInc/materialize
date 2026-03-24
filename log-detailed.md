# Columnar Rendering: Detailed Work Log

<!-- Each completed prompt gets a section here with: what was done, key decisions, files changed, issues encountered -->

## Prompt 0.1: Introduce ColumnarCollection type alias and ColumnarBundle

### What was done
- Added `ColumnarCollection<S, D, R>` type alias in `typedefs.rs`, defined as `Collection<S, Column<(D, <S as ScopeParent>::Timestamp, R)>>`.
- Added `columnar_collection` field to `CollectionBundle<S, T>` (initially always `None`).
- Added `from_columnar_collections(oks, errs)` constructor.
- Added `columnar_collection()` accessor returning `Option<&(ColumnarCollection, VecCollection)>`.
- Added `ensure_vec_collection()` escape hatch that converts columnar → Vec using `timely::dataflow::operators::core::Map` and `columnar::Columnar::into_owned`.
- Updated `enter_region`, `leave_region`, `scope()`, and `update_id` to propagate the new field.
- All existing constructors (`from_collections`, `from_expressions`, `from_columns`) set `columnar_collection: None`.

### Key decisions
- Error streams remain `VecCollection<S, DataflowError, Diff>` even in the columnar variant, since `DataflowError` is not suited for columnar layout (per project notes).
- `ensure_vec_collection()` uses timely's `Map::map` to convert each columnar ref to owned `(Row, T, Diff)` tuples. This is item-at-a-time but is only the escape hatch.
- The columnar field is `Option` so it's fully backwards-compatible; no existing code paths are affected.

### Files changed
- `src/compute/src/typedefs.rs` — Added `ColumnarCollection` type alias, `Column` and `Collection` imports.
- `src/compute/src/render/context.rs` — Added field, constructors, accessors, escape hatch, updated region/scope/update methods.

### Issues
- Initial `flat_map` approach for `ensure_vec_collection` failed because timely's `Map::flat_map` operates per-item, not per-container. Switched to `Map::map` which correctly takes each `Ref<'_, (Row, T, Diff)>` item and converts to owned.

## Prompt 0.2: Columnar ↔ Vec conversion utilities

### What was done
- Implemented `vec_to_columnar` and `columnar_to_vec` free functions in new module `src/compute/src/render/columnar.rs`.
- `vec_to_columnar` uses `StreamCore::unary` with `ColumnBuilder<(Row, T, Diff)>` as the output container builder. Iterates Vec input and pushes each `(row, time, diff)` into the columnar session. ColumnBuilder handles batch sizing automatically (~2MB aligned containers).
- `columnar_to_vec` uses `StreamCore::unary` with `CapacityContainerBuilder<Vec<...>>` as output. Iterates the columnar container via `data.borrow().into_index_iter()` and converts each ref to owned via `Columnar::into_owned`.
- Added two unit tests: `round_trip_vec_columnar_vec` (4 diverse rows including empty, multi-type, and multi-datum rows) and `round_trip_multiple_timestamps` (verifies timestamp preservation across time advances).

### Key decisions
- Functions take collections by value (not reference) since `StreamCore::unary` consumes `self`.
- Batch sizing is delegated to `ColumnBuilder`'s built-in ~2MB alignment logic rather than a configurable constant, matching the existing codebase pattern.
- Used `Pipeline` pact (no exchange/repartitioning) since these are pure format conversions.
- Created a new module `render/columnar.rs` rather than putting utilities in `context.rs`, keeping conversion logic separate from the bundle management code.

### Files changed
- `src/compute/src/render/columnar.rs` — New file with `vec_to_columnar`, `columnar_to_vec`, and tests.
- `src/compute/src/render.rs` — Added `pub(crate) mod columnar;` module declaration.

### Issues
- Required `columnar::Index` trait import for `into_index_iter()` — the trait was implemented but not in scope.
- `Diff` is `Overflowing<i64>`, not `i64`, so tests needed `input.update(row, Diff::from(1))` instead of `input.insert(row)`.
- `Probe::probe()` returns a tuple `(ProbeHandle, Stream)`, requiring destructuring.
