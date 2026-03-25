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

## Prompt 1.1: Persist source emits columnar collections

### What was done
- Investigated `persist_source::persist_source()` — it returns `StreamVec<G, (Row, Timestamp, Diff)>` (Vec-based containers). It does not use `ColumnBuilder` natively, so a `vec_to_columnar` conversion at the boundary is the practical approach.
- Modified both import loops in `render.rs` (recursive and non-recursive dataflow paths) to create columnar collections alongside the existing Vec collections.
- At each import boundary: clone the entered Vec collection, convert the clone to columnar via `vec_to_columnar`, and set both `collection` and `columnar_collection` fields on the `CollectionBundle`.
- All downstream operators continue to work unchanged since they access `self.collection` (the Vec variant), which is always populated.

### Key decisions
- Set **both** `collection` and `columnar_collection` fields rather than only columnar. This avoids needing to modify `as_specific_collection` (which takes `&self`, not `&mut self`) and all downstream operators. The columnar collection is available for future operators to use.
- Applied `vec_to_columnar` **after** `enter`/`enter_region`, not before, because `Column<(Row, T, Diff)>` does not implement the `Enter` trait required by differential_dataflow's `enter()` method.
- The conversion cost is the overhead of `vec_to_columnar` at every import, even though no downstream operator uses the columnar variant yet. This will pay off as operators are converted in later prompts.

### Files changed
- `src/compute/src/render.rs` — Modified both import loops (recursive at ~line 378, non-recursive at ~line 487) to create `CollectionBundle` with both Vec and columnar collections populated.

### Issues
- `Column<(Row, T, Diff)>` does not implement `differential_dataflow::collection::containers::Enter`, so `enter()` cannot be called on a `ColumnarCollection`. Solved by converting to columnar after entering the region scope.
- Test binary linking OOMs in the constrained CI environment, but `cargo check` passes cleanly, confirming type correctness.

## Prompt 2.1: Negate operator propagates columnar collections

### What was done
- Added `negate_columnar` function in `render/columnar.rs` that takes a `ColumnarCollection<S, Row, Diff>` and produces a new one with all diffs negated.
- Modified the `Negate` match arm in `render.rs` to check for a columnar collection first. If present, uses `negate_columnar` to produce a columnar output bundle. Otherwise falls back to the existing Vec path.
- Added unit test `negate_columnar_flips_diffs` that verifies diffs are correctly negated through Vec→Columnar→Negate→Columnar→Vec round-trip.

### Key decisions
- Used a `unary` operator with `ColumnBuilder` output (same pattern as `vec_to_columnar`) rather than trying to call `Collection::negate()` directly, since the columnar `Collection` type may not support `negate()` out of the box.
- When columnar is available, produces only a columnar output (no Vec). Downstream operators that need Vec will use `ensure_vec_collection()`.
- The `into_owned` conversion for the diff is necessary to apply `Neg`, but row and timestamp refs are passed directly to the `ColumnBuilder` session without conversion.

### Files changed
- `src/compute/src/render/columnar.rs` — Added `negate_columnar` function and test.
- `src/compute/src/render.rs` — Modified Negate match arm to use columnar path when available.

### Issues
- `Columnar::into_owned(r)` required explicit type annotation (`: Diff`) because the compiler couldn't infer the type through the `-` negation operator.

## Prompt 2.2: Union operator propagates columnar collections

### What was done
- Modified the `Union` match arm in `render.rs` to check if all inputs have columnar collections. If so, uses `differential_dataflow::collection::concatenate` on the columnar collections directly.
- When `consolidate_output` is true and inputs are columnar, converts to Vec for consolidation (which requires Vec-based batchers), then converts back to columnar.
- Falls back to the existing Vec path when any input lacks a columnar collection.
- Added unit test `union_columnar_concatenates` that verifies columnar concatenation preserves all rows from two input streams, including duplicate rows with different diffs.

### Key decisions
- Used an "all or nothing" strategy: if all inputs have columnar, use columnar; otherwise fall back to Vec for all. This avoids the overhead of converting individual inputs.
- For `consolidate_output`, the consolidation uses `KeyBatcher` which requires Vec-based collections. Rather than implementing a columnar-native consolidation, we round-trip through Vec→consolidate→columnar. This is acceptable because consolidation is already expensive and the conversion overhead is small relative to the sort/merge.
- `concatenate` from `differential_dataflow` is generic over container types and works with `Column<...>` containers directly.

### Files changed
- `src/compute/src/render.rs` — Modified Union match arm to support columnar path.
- `src/compute/src/render/columnar.rs` — Added `union_columnar_concatenates` test.

### Issues
- None. `concatenate` worked out of the box with columnar collections since `Column` implements the required `Container` trait.

## Prompt 2.3: Constant operator emits columnar collections

### What was done
- Modified the `Constant` match arm in `render.rs` to also produce a columnar collection alongside the existing Vec collection.
- After creating the `ok_collection` (Vec-based), clones it and converts via `vec_to_columnar` to produce a columnar variant.
- Sets both `collection` and `columnar_collection` fields on the resulting `CollectionBundle`, so downstream operators can use either path.
- Added unit test `constant_rows_to_columnar` that simulates the Constant operator pattern: creates rows from an iterator, converts to stream, then round-trips through columnar.

### Key decisions
- Set **both** Vec and columnar fields (same pattern as persist source in Prompt 1.1) rather than columnar-only, since downstream operators may still need the Vec path.
- Constants are typically small, so the `vec_to_columnar` overhead is negligible. This change is primarily for uniformity so that all source operators produce columnar collections.

### Files changed
- `src/compute/src/render.rs` — Modified Constant match arm to also produce columnar collection.
- `src/compute/src/render/columnar.rs` — Added `constant_rows_to_columnar` test.

### Issues
- None. Straightforward application of the established pattern.

## Prompt 3.1: Columnar `as_collection_core` with Get/Mfp wiring

### What was done
- Added `as_columnar_collection_core` method on `CollectionBundle` in `context.rs`. It delegates to the existing `as_collection_core` (row-at-a-time MFP evaluation) and converts the output to columnar via `vec_to_columnar`.
- When the bundle has only a columnar collection (no Vec), the method converts columnar→Vec first by creating a temporary bundle with Vec populated, then delegates to `as_collection_core`.
- Wired `render_plan_expr` in `render.rs` for `Get::Collection` and `Mfp` to check for columnar input and use `as_columnar_collection_core` when available, producing columnar-only output bundles.

### Key decisions
- When only columnar input is available (no Vec collection), a temporary `CollectionBundle` is constructed with the Vec conversion so `as_collection_core` can proceed. This avoids modifying the immutable `&self`.
- Only `Get::Collection` and `Mfp` are wired to the columnar path. `Get::Arrangement` always uses arrangement access (key_val is Some) so columnar input is irrelevant there.
- The `Get::PassArrangements` path is unchanged since it only passes through arrangements.
- This is the incremental step: all MFP evaluation is still row-at-a-time. Vectorized evaluation will replace the inner loop in Prompt 3.2.

### Files changed
- `src/compute/src/render/context.rs` — Added `as_columnar_collection_core` method.
- `src/compute/src/render.rs` — Modified `Get::Collection` and `Mfp` match arms to prefer columnar path.

### Issues
- None. The temporary bundle clone is slightly wasteful but only occurs when the bundle lacks a Vec collection (columnar-only). Arrangement clones are cheap (reference-counted).

## Prompt 4.1: Columnar FlatMap

### What was done
- Modified `render_flat_map` to accept columnar input and emit columnar output.
- At the start of the method, checks if the input has a columnar collection (`has_columnar`). If the input has no Vec collection (columnar-only), calls `ensure_vec_collection()` to convert for the row-at-a-time table function evaluation.
- At the end, if the input was columnar, converts the Vec ok output to columnar via `vec_to_columnar` and returns a columnar-only bundle. Otherwise returns Vec as before.

### Key decisions
- Table functions are inherently row-at-a-time (variable output rows per input), so no attempt is made to vectorize the table function evaluation itself. The columnar conversion is purely at the input/output boundaries.
- Used `ensure_vec_collection()` (the escape hatch from Prompt 0.1) to handle columnar-only inputs, which converts columnar→Vec in-place on the mutable bundle.
- Output follows the same pattern as Negate/Union: columnar-only when input was columnar, Vec-only otherwise.

### Files changed
- `src/compute/src/render/flat_map.rs` — Modified `render_flat_map` to handle columnar input/output.

### Issues
- None. Straightforward boundary conversion.

## Prompt 5.1: Columnar input to arrangements

### What was done
- Modified `ensure_collections` in `context.rs` to handle columnar-only inputs when creating arrangements.
- When the method needs a Vec collection (`form_raw_collection && self.collection.is_none()`) and the non-arrangement path is used (`input_key.is_none()`), it now calls `ensure_vec_collection()` to convert columnar→Vec before delegating to `as_collection_core`.
- This prevents a panic in `as_collection_core`/`as_specific_collection` when only columnar is available and no arrangement key is provided.

### Key decisions
- Only convert columnar→Vec when `input_key.is_none()`, because when `input_key` is Some, `as_collection_core` uses the arrangement path which doesn't need `self.collection`.
- The arrangement spines (`RowRowSpine`) use `DatumContainer` which accepts Row input, so unpacking columnar→Row at this boundary is required regardless. The existing `arrange_collection` method works on the Vec collection unchanged.
- No changes needed to `extensions/arrange.rs` — the plumbing is entirely in `ensure_collections`.

### Files changed
- `src/compute/src/render/context.rs` — Added columnar→Vec conversion guard in `ensure_collections`.

### Issues
- None. Minimal change since the arrangement infrastructure already works with Vec collections.

## Prompt 6.1: Columnar Reduce input

### What was done
- Modified `render_reduce` in `reduce.rs` to handle columnar-only inputs.
- After `input.enter_region(inner)`, checks if the entered bundle has only columnar (no Vec collection). If so, calls `ensure_vec_collection()` to convert before the `flat_map` call that extracts keys and values.
- The key extraction, value extraction, and aggregation logic remain row-at-a-time. The columnar→Vec conversion happens at the operator boundary.

### Key decisions
- Same pattern as FlatMap (Prompt 4.1) and ensure_collections (Prompt 5.1): convert at the boundary, process row-at-a-time internally.
- The conversion happens after `enter_region` since the entered bundle is what `flat_map` operates on.
- The reduce output (arrangements) remains unchanged — reduce inherently produces arrangements which are already efficient.

### Files changed
- `src/compute/src/render/reduce.rs` — Added columnar→Vec conversion guard before `flat_map` call.

### Issues
- None. Straightforward boundary conversion.

## Prompt 6.2: Columnar TopK input

### What was done
- Modified `render_topk` in `top_k.rs` to handle columnar-only inputs.
- Before calling `as_specific_collection(None, ...)`, checks if the bundle has only columnar (no Vec). If so, calls `ensure_vec_collection()` to convert.

### Key decisions
- Same boundary conversion pattern as Reduce (6.1), FlatMap (4.1), and ensure_collections (5.1).

### Files changed
- `src/compute/src/render/top_k.rs` — Added columnar→Vec conversion guard.

### Issues
- None.

## Prompt 6.3: Columnar Threshold input (verification only)

### What was done
- Reviewed `render_threshold` and `build_threshold_basic` in `threshold.rs`.
- Confirmed that threshold operates entirely on arrangements via `input.arrangement(&key)` (line 84-86). It never accesses `self.collection` or calls `as_specific_collection`.
- No code changes needed. Columnar-only bundles work correctly because arrangements are stored in the separate `arranged` field of `CollectionBundle`.

### Key decisions
- Verification-only prompt. The threshold operator is arrangement-native and does not interact with unarranged collections at all.

### Files changed
- None (verification only).

### Issues
- None.

## Prompt 7.1: Columnar Linear Join input

### What was done
- Modified `render_join_inner` in `linear_join.rs` to handle columnar-only inputs in the fallback path (no matching arrangement or initial closure present).
- Before calling `as_specific_collection`, checks if the source bundle has only columnar (no Vec) and `source_key` is None (no arrangement path). If so, clones the bundle, calls `ensure_vec_collection()`, and uses the converted bundle.
- Join stages continue to operate on arrangements unchanged. The columnar→Vec conversion only affects the initial collection extraction.

### Key decisions
- Since `inputs` is borrowed by index (`&inputs[linear_plan.source_relation]`), we clone when conversion is needed rather than mutating in-place. The clone is cheap (stream handles are reference-counted).
- Only convert when `source_key` is None, because when a source_key is present, `as_specific_collection` uses the arrangement path which doesn't need `self.collection`.
- Did not implement columnar output accumulation for `LinearJoinImpl` — join stages inherently produce row-at-a-time output via closure application, and the existing Vec output path is sufficient. Columnar output can be added when downstream operators specifically benefit.

### Files changed
- `src/compute/src/render/join/linear_join.rs` — Added columnar→Vec conversion guard in the fallback path of `render_join_inner`.

### Issues
- None.

## Prompt 7.2: Columnar Delta Join output

### What was done
- Modified `render_delta_join` in `delta_join.rs` to produce columnar output when any input has a columnar collection.
- Delta joins operate entirely on arrangements for both inputs and join stages, so no input conversion is needed.
- At the output boundary, if any input had columnar, converts the Vec ok collection to columnar via `vec_to_columnar`.

### Key decisions
- Used "any input has columnar" as the heuristic for producing columnar output, maintaining columnar flow through the dataflow graph.
- No changes to join internals — closure application is inherently per-result-tuple and stays Vec-based.

### Files changed
- `src/compute/src/render/join/delta_join.rs` — Added columnar output conversion at the end of `render_delta_join`.

### Issues
- None.

## Prompt 8.1: Columnar sink input

### What was done
- Modified `export_sink` in `sinks.rs` to handle columnar-only bundles at the sink boundary.
- Added a new branch: when `bundle.collection` is None but `bundle.columnar_collection` is Some, clones the bundle, calls `ensure_vec_collection()`, and uses the resulting Vec collection.
- The sink/persist export boundary requires Row-format data, so columnar→Vec conversion is necessary here.

### Key decisions
- Sinks are the terminal boundary of the dataflow — no need to produce columnar output. The conversion to Vec is the correct behavior for writing to persist.
- Clone the bundle rather than mutating in-place since `bundle` is obtained from `lookup_id` which returns an owned value but the code structure requires the `if let` pattern.
- The arrangement fallback path (else branch) remains unchanged for bundles that have neither collection nor columnar.

### Files changed
- `src/compute/src/render/sinks.rs` — Added columnar→Vec conversion branch in `export_sink`.

### Issues
- None.

## Prompt 9.1: Audit and remove dead Vec paths

### What was done
- **Audit**: Identified all 19 `.collection` accesses across render modules. Found that all operators now handle columnar-only bundles via `ensure_vec_collection()` guards (Reduce, TopK, FlatMap, Linear Join, Sinks, ensure_collections).
- **Logging**: Added `tracing::debug!` in `ensure_vec_collection` to track when the Vec fallback is used at runtime.
- **Removed redundant Vec from persist source imports**: Both import loops (recursive and non-recursive) now produce columnar-only bundles via `from_columnar_collections` instead of setting both Vec and columnar. The Vec stream is still created (needed as input to `vec_to_columnar`) but not retained in the bundle.
- **Removed redundant Vec from Constant operator**: Now produces columnar-only bundle instead of both Vec and columnar.
- **Fixed assertion**: Updated `Get::PassArrangements` assertion to accept columnar collections as valid raw collections (line 1244: `keys.raw <= (collection.is_some() || columnar_collection.is_some())`).

### Key decisions
- Source operators (persist, constant) now produce **columnar-only** bundles. All downstream operators already handle this via `ensure_vec_collection()` guards added in earlier prompts.
- The `collection` field in `CollectionBundle` is NOT removed yet — it's still needed as a transient state (populated by `ensure_vec_collection()` and used by arrangement creation, sinks, etc.). Removal is Prompt 9.2.
- Used `tracing::debug!` rather than metrics counters for simplicity. This can be upgraded to proper metrics if needed.

### Files changed
- `src/compute/src/render/context.rs` — Added debug logging in `ensure_vec_collection`.
- `src/compute/src/render.rs` — Removed redundant Vec from persist source imports (both loops) and Constant operator; fixed PassArrangements assertion.

### Issues
- The `Get::PassArrangements` assertion `keys.raw <= collection.collection.is_some()` would have panicked with columnar-only bundles. Fixed to also accept `columnar_collection.is_some()`.

## Prompt 9.2: Remove `collection` field and `ensure_vec_collection`

### What was done
- **Removed `collection` field** from `CollectionBundle` struct. Data now flows exclusively through `columnar_collection`.
- **Removed `ensure_vec_collection`** method entirely.
- **Modified `from_collections`** to automatically convert Vec→columnar via `vec_to_columnar`, so callers producing Vec output (TopK, Linear Join, LetRec, etc.) seamlessly convert to columnar.
- **Added `as_vec_collection`** method that converts columnar→Vec on demand, replacing the role of `ensure_vec_collection` + `.collection.clone()`.
- **Updated `as_specific_collection(None)`** and `flat_map(None, ...)` to use `as_vec_collection()` internally.
- **Simplified `as_columnar_collection_core`** — no longer needs special-case handling since there's only one collection type.
- **Rewrote `ensure_collections`** (ArrangeBy) to use a local `cached_vec` variable instead of `self.collection` for arrangement creation. After the loop, stores the passthrough as columnar if `collections.raw` is demanded.
- **Updated all callers**: removed `ensure_vec_collection()` calls from flat_map.rs, top_k.rs, reduce.rs, sinks.rs, linear_join.rs.
- **Fixed LetRec** code in render.rs to use `as_vec_collection()` instead of `.collection.unwrap()`.
- **Fixed hydration logging** to operate on `columnar_collection` instead of `collection`.
- **Updated `enter_region`/`leave_region`/`scope`/`update_id`** to only handle `columnar_collection`.

### Key decisions
- `from_collections` auto-converts Vec→columnar, making the transition invisible to callers. This means operators that produce Vec output (TopK, reduce, join) don't need individual changes.
- `as_vec_collection()` converts on every call (no caching). This is acceptable because the conversion is only used at operator boundaries (arrangements, sinks, etc.) where the cost is amortized.
- The `from_expressions` constructor (arrangement-only bundles) sets `columnar_collection: None`, which is correct since arrangement-only bundles have no unarranged collection.

### Files changed
- `src/compute/src/render/context.rs` — Struct field removal, method updates, new `as_vec_collection`.
- `src/compute/src/render.rs` — Fixed LetRec, PassArrangements assertion, hydration logging, debug prints.
- `src/compute/src/render/flat_map.rs` — Removed `ensure_vec_collection` call.
- `src/compute/src/render/top_k.rs` — Removed `ensure_vec_collection` call.
- `src/compute/src/render/reduce.rs` — Removed `ensure_vec_collection` call.
- `src/compute/src/render/sinks.rs` — Simplified to use `as_vec_collection`.
- `src/compute/src/render/join/linear_join.rs` — Removed `ensure_vec_collection` guard.

### Issues
- The LetRec code (`render.rs` ~line 956, 1007) directly accessed `.collection.unwrap()` — needed conversion to `as_vec_collection()`.
- Hydration logging (`render.rs` ~line 1481) modified `.collection` in-place — updated to modify `columnar_collection` instead.
- `ensure_collections` loop used `self.collection.take()`/`self.collection = Some(...)` pattern — replaced with local `cached_vec` variable.
