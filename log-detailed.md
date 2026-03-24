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
