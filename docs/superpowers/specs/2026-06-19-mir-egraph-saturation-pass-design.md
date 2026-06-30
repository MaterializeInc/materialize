# Equality-saturation MIR pass: milestone 1 design

> This is the frozen milestone-1 design, kept for history.
> For the current architecture (the system as built, sufficient to rebuild from scratch), see `2026-06-24-mir-egraph-architecture.md`.
> For the roadmap and per-transform coverage, see `2026-06-19-mir-egraph-status.md`.

## Goal

Wire the `misc/mir-rewrite-dsl` equality-saturation prototype into Materialize end to end, incrementally.
The strategy is to bail on constructs we do not yet support, optimizing the supported envelope around them, and to grow coverage one operator at a time.
Milestone 1 lands an in-tree crate that runs the saturating optimizer on real `MirRelationExpr`, exercised by the existing transform test harness, but not yet registered in the live optimizer pipeline.
Later milestones register it behind a feature flag, add a scalar optimizer, and demonstrate a plan the current greedy pipeline misses.
This document specifies milestone 1 only.

## Scope

Milestone 1 is an offline, test-only integration.
The pass is reachable from the datadriven transform harness via `apply=EqSat`, never from `Optimizer::physical_optimizer` or any production path.
Scalars are opaque: the pass moves whole scalar lists around but never rewrites inside a scalar.
Statistics are accepted but ignored, because statistics are currently disabled and the common case is blank.

## Background

The prototype (`misc/mir-rewrite-dsl`) is a self-contained workspace with its own `Rel` enum modeling a subset of `MirRelationExpr`, an e-graph with worst-case-optimal-join e-matching, an abstract cardinality-free cost model, an e-class analysis framework, a rule DSL, and a Lean 4 specification.
The real optimizer (`src/transform/src/lib.rs`) assembles `Vec<Box<dyn Transform>>` into `Fixpoint` groups; `Transform::transform(&mut MirRelationExpr, &mut TransformCtx)` is the unit of work.
`MirRelationExpr` (`src/expr/src/relation.rs`) has 15 variants; `Rel` is a faithful subset, and crucially `Join` already carries `equivalences: Vec<Vec<MirScalarExpr>>` exactly as the prototype models it, so rule shapes transfer directly.
The real tree also has a bottom-up analysis framework (`src/transform/src/analysis.rs`: `Arity`, `UniqueKeys`, `NonNegative`, `Cardinality`, `Equivalences`), most of the prototype's relational rules already exist as production transforms, and the prototype's unique leverage is escaping local minima via saturation plus join reshaping.

## Architecture

A new workspace crate `src/transform-egraph` (crate name `mz-transform-egraph`), a member of the `mz-*` workspace.

* Depends on `mz-expr` (`MirRelationExpr`, `MirScalarExpr`), `mz-repr`, and `mz-transform` (for the `Transform` trait and `TransformCtx`).
* `mz-transform` gains a dev-dependency back on `mz-transform-egraph` so the test harness can dispatch `apply=EqSat`.
  This is a dev-dependency cycle, which Cargo permits.

Public surface:

```rust
pub struct EqSatOptions<'a> {
    pub stats: Option<&'a dyn StatisticsOracle>, // ignored when blank (today's reality)
    pub max_iters: usize,
}

pub fn optimize(expr: MirRelationExpr, opts: &EqSatOptions) -> MirRelationExpr;

/// Thin `Transform` wrapper over `optimize`, for the test harness. Not registered
/// in the live `Optimizer` pipeline in milestone 1.
pub struct EqSatTransform { /* ... */ }
impl Transform for EqSatTransform { /* calls optimize */ }
```

The prototype engine modules are ported in, not re-derived: `egraph`, `cost`, `matcher`, `parser`, `dsl`, `analysis`, `cse`, and `ir` (the `Rel` subset), plus `rules/relational.rewrite` via `include_str!`.

## Components (new code)

* `intern.rs` builds bidirectional tables during lowering.
  * `MirScalarExpr <-> ScalarId`, each `ScalarId` caching `MirScalarExpr::support()` (the referenced column set) and literal flags, so relational side conditions evaluate without destructuring scalars.
  * An opaque-leaf table `MirRelationExpr <-> LeafId`, recording each bailed subtree's real `arity()`.
  * `LocalId` / `GlobalId` binding map for `Get`.
* `lower.rs` translates `MirRelationExpr -> Rel`.
  Supported variants map structurally; an unsupported variant becomes an opaque leaf `Rel::Get(LeafId)` carrying its recorded arity; scalars are interned.
* `raise.rs` translates `Rel -> MirRelationExpr`, the inverse: opaque leaves re-expand to their stored subtree, and `ScalarId`s restore their `MirScalarExpr`.
* `lib.rs` ties it together as `optimize`: lower, then if the root itself is unsupported return the input unchanged, else add to the e-graph, saturate, extract the cheapest plan, raise, and assert `arity(input) == arity(output)`.

## Data flow

```
MIR --lower--> Rel   (intern scalars; unsupported subtree => opaque leaf with arity)
                 |
           EGraph::add --> saturate(rules, max_iters) --> extract(root, cost)
                 |
Rel --raise--> MIR   (re-expand leaves, restore scalars)  -- assert arity preserved
```

## Bail strategy

Bail is per subtree, not per plan.
A `Reduce` buried under `Filter`s becomes a single opaque leaf, while the `Filter` / `Join` / `Union` envelope around it still saturates.
An opaque leaf is semantically an unknown base relation carrying a known arity, so every relational rule over it stays valid because no rule inspects a leaf's contents.
Coverage grows by teaching `lower` and `raise` one more variant at a time; nothing else in the engine changes.

* Milestone 1 supported variants: `Get`, `Constant`, `Project`, `Map`, `Filter`, `Join` (with `equivalences`), `Negate`, `Threshold`, `Union`, `Let` / `LocalGet`.
* Milestone 1 bail-to-leaf variants: `FlatMap`, `Reduce`, `TopK`, `ArrangeBy`, `LetRec`.

## Cost model

Reuse the prototype's abstract degree / AGM cost.
Opaque leaves are unknown size `N`; a plan is scored by its worst-case asymptotic degree multiset, node count as tie-breaker.
`StatisticsOracle` is plumbed through `EqSatOptions` but ignored when blank, which is the current reality.
A later milestone can let a non-blank oracle refine leaf sizes.

## Scalars

Scalars stay opaque payloads plus the cached `support()` and literal metadata from `intern.rs`.
Relational side conditions (`uses_only_input`, `all_columns`, `any_false`) compute from that metadata, so no scalar optimizer is required for milestone 1.
Scalar-internal rewriting is left to the existing production transforms (`FoldConstants`, `CoalesceCase`, `RelationCSE`, `LiteralLifting`) that run around our pass.
A scalar e-graph layer that unlocks the currently-`sorry` Lean rules is a deferred, optional later step, not a prerequisite for integration.

## Active rules

The relational rules that need no scalar optimizer are active: filter fusion, projection fusion, map fusion, filter-through-map (support-based), filter-over-union distribution, union flattening, double-negation, threshold idempotence, filter-through-negate, threshold and negate distribution over union, empty-on-false-filter, and map-of-column-refs to projection.

Disabled in milestone 1:

* `join_to_wcoj`, because `MirRelationExpr` has no `WcoJoin` variant.
  Materialize chooses delta versus differential join later in `JoinImplementation`, so a `WcoJoin` node would have no MIR target.
  The `Rel::WcoJoin` constructor is not emitted; the rule is off.
  A later milestone revisits this by mapping the worst-case-optimal choice to a forced delta-join *implementation* rather than a new relational node.
* The scalar-IR rules `fold_constants` and `projection_extraction`, which require a scalar optimizer.

## Testing

* Unit tests in `src/transform-egraph`: `lower` then `raise` is identity with rules off, for each supported variant and each bail variant; `arity` is preserved; a plan made only of unsupported variants round-trips byte-identical.
* Datadriven tests use the existing `.spec` format and harness under `src/transform/tests/test_transforms`.
  Add an `EqSat` arm to `get_transform` in `src/transform/tests/test_runner.rs` returning `Box::new(EqSatTransform::new(...))`, and add `eqsat.spec` exercised through `apply=EqSat` with `explain` snapshots.
  Include plans containing `Reduce` and `TopK` to assert both partial rewrite around an opaque leaf and identity when the whole plan is unsupported.

## Out of scope (milestone 1) and later steps

1. Registering `EqSatTransform` in the live `Optimizer` pipeline behind a feature flag.
2. A scalar optimizer that unlocks scalar-aware rules and the currently-`sorry` Lean theorems.
3. Mapping the worst-case-optimal join choice to a forced delta-join implementation, and demonstrating a plan the greedy pipeline misses.
4. `LetRec` cross-binding e-matching through the recursive back-edge.
