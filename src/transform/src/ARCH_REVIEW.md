# Architecture review — `transform::src`

Scope: `src/transform/src/` (≈ 23,397 LOC, ~35 `Transform` implementors).

## 1. `Transform` trait: two-method split creates invisible dead surface

**Files**
- `src/transform/src/lib.rs:213-264` — `Transform` trait with `actually_perform_transform` (implement) / `transform` (call).

**Issue**
The trait has two methods where implementors write `actually_perform_transform` and callers invoke `transform`. This split exists to inject timing/metrics uniformly. However:
- Every `impl crate::Transform for Foo` must remember to implement the *non-obvious* name `actually_perform_transform`, not the method callers use.
- `debug()` on the trait returns `format!("{:?}", self)`, so it is redundant with `name()` in practice; `debug()` appears unused in the codebase.
- No mechanism prevents an implementor from accidentally overriding `transform` instead of `actually_perform_transform`, silently bypassing metrics.

**Deepening opportunity**
Rename `actually_perform_transform` → `apply` (or `run`) and seal `transform` with `#[doc(hidden)]` or a wrapper newtype, so the public surface matches the mental model. Remove the dead `debug()` method. Depth: Interface.

## 2. `Optimizer` factory methods: implicit pipeline order coupling, and a live deprecated entry

**Files**
- `src/transform/src/lib.rs:736-980` — `logical_optimizer` (deprecated), `physical_optimizer`, `logical_cleanup_pass`, `fast_path_optimizer`, `constant_optimizer`.
- `src/transform/src/dataflow.rs:53-93` — `optimize_dataflow` calls them in a fixed pipeline order.
- `src/adapter/src/optimize.rs:346` — still calls the deprecated `logical_optimizer` directly.

**Issue**
Each factory returns `Vec<Box<dyn Transform>>` assembled by ad-hoc `transforms![]` macro invocations. The ordering constraints between passes (e.g., `LiteralConstraints` must precede `JoinImplementation`; `RelationCSE` before `JoinImplementation` must use `inline_mfp = true`; `NormalizeLets` required as last step before rendering) are documented only in comments, not enforced structurally. A caller assembling a custom pipeline silently violates these constraints.

The deprecated `logical_optimizer` is still called live from `src/adapter/src/optimize.rs:346`, so the deprecation notice is not actionable without a follow-through in adapter.

**Deepening opportunity**
Introduce typed pipeline builder stages (`LogicalStage`, `PhysicalStage`) with an explicit ordering guarantee via typestate or an enum-indexed array. At minimum, lift the ordering constraints into `#[doc]` on each transform struct. Complete the `logical_optimizer` deprecation by migrating the one remaining call site. Depth: Seam.

## 3. `ordering.rs` is a reserved stub occupying namespace

**File**
- `src/transform/src/ordering.rs:1-12` — 12-line stub with only a module docstring; no types, no `impl Transform`.

**Issue**
The module is `pub mod ordering;` exported in `lib.rs`. It contributes a pub module to the crate surface that is entirely empty. This is dead surface that new contributors will explore and find nothing.

**Deepening opportunity**
Either fill it (add the canonical-ordering pass it was reserved for) or delete it and remove the `pub mod ordering` line. Deletion test passes — no other code references `transform::ordering`. Depth: Locality.

## 4. `Fixpoint` oscillation detection is thorough but expensive for large plans

**File**
- `src/transform/src/lib.rs:475-510` — hash-collision detection re-runs the full transform prefix from `original`.

**Issue**
Hash collisions trigger a full replay from the original plan starting at iteration 0. For large plans (`prev_size > 100000` already triggers a `tracing::warn!`) this replay can be quadratic in iteration count. The warn threshold and the hash-collision replay are not co-tuned.

**Deepening opportunity**
Lower-cost option: compare structural equality only on the colliding subtree rather than re-running from scratch. Or: accept that hash collisions are rare and document the O(n^2) worst case. Depth: Implementation.

## 5. (Honest skip) 35 Transform implementors are flat, not layered

With ~35 independent transform structs, one per file or submodule, the layout is well-organized — each concern is in its own module with a `.spec` test file. The deletion test passes for each: no cross-module coupling that cannot be justified by the pipeline sequencing. No deepening here; the flat layout is correct for a plug-in architecture.

## What this review did not reach

- Per-pass correctness invariants (e.g., whether `EquivalencePropagation` is idempotent) — would require reading each pass end-to-end.
- `analysis/` lattice framework: the `Derived`/`DerivedBuilder` type-erasure machinery deserves its own review.
- Interaction between `DataflowMetainfo` side-channel and pass ordering — downstream in `dataflow.rs`.
