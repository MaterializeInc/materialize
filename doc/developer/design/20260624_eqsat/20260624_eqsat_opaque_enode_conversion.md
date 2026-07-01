# Converting the remaining `Opaque` enodes: feasibility

## Purpose

The eqsat lowering models every `MirRelationExpr` variant as a first-class `Rel` except two, which it bails to `Rel::Opaque`: a real `Constant` (with rows) and a global `Get` (`src/transform/src/eqsat/lower.rs:240-248`).
This note evaluates converting each into a proper enode, and the larger alternative of parameterizing `MirRelationExpr` over its child type so the e-graph reuses it directly.
The two bailed cases are not arbitrary gaps; both carry a verbatim payload that no relational rewrite rule reads, so the question is whether a first-class variant buys anything beyond removing the `Opaque` wrapper.
The recommendation is to skip the `Constant` conversion, treat the global-`Get` conversion as an optional semantics-preserving cleanup gated on the golden suite, and defer the functor refactor as a multi-week effort needing explicit buy-in.

## Constant

`Rel::Constant` carries `card`, `arity`, and `col_types` but no rows, and raise always emits an empty relation `MirRelationExpr::constant(vec![], typ)` (`src/transform/src/eqsat/raise.rs:158-174`).
The variant exists only for the synthesized `Empty(r)` nodes that saturation rules (`empty_false_filter`, `union_cancel`) encode as `Constant { card: 0, .. }`; a constant with actual rows is bailed to `Opaque` so raise can re-emit its rows verbatim.
Making `Constant` first-class would require adding the MIR row payload, which is `Result<Vec<(Row, Diff)>, EvalError>`, to the enode.
That payload would join the hash-cons/intern key, so every `Constant` enode would carry and hash a row vector for zero rewrite benefit, since no rule inspects constant rows.

Recommendation: skip.
The synthesized-empty `Rel::Constant` already models the only fact rules care about (emptiness), and a literal collection is exactly the kind of verbatim subtree `Opaque` exists to carry.
This is a case where the existing split is correct, not a gap.

## Global Get

`Rel::Get { name, arity }` is vestigial: lowering never emits it and raise hits `unreachable!` for it (`src/transform/src/eqsat/raise.rs:115-116`).
A global `Get` is instead carried as `Opaque(MirRelationExpr::Get { Id::Global(gid), .. })`, and a fixed set of sites peek inside that `Opaque` for the `GlobalId`.

The free-if-indexed cost oracle and its callers depend on this form:

* `global_id_from_leaf` matches `Opaque(Get{Global})` (`src/transform/src/eqsat/cost.rs:716`), consumed by `input_already_arranged` (`cost.rs:561`) and `arrange_by_oracle_covered` (`cost.rs:603`).
* The extractor's arrangement-free check rebuilds a temporary `Rel::Opaque` to reuse the same oracle (`src/transform/src/eqsat/extract.rs:584-592`).
* `cond_reads_indexed_global` walks to an `Opaque` global `Get` and checks the availability map (`src/transform/src/eqsat/egraph.rs:589`, `egraph.rs:1211`).
* The cost-model and cse helpers synthesize `Opaque(Get)` for transient ids (`cost.rs:1711`, `cse.rs:281`).

A conversion would extend `Rel::Get` to carry `GlobalId` and the relation type (it currently holds only `name` and `arity`, too little for faithful raise), make lowering emit it, make raise reconstruct `MirRelationExpr::Get`, and switch each site above from matching `Opaque` to matching `Rel::Get`.
The blast radius is medium (roughly eight production match sites plus lowering and raise) and the change is semantics-preserving: the oracle returns the same `GlobalId`, so no plan changes.

Recommendation: optional cleanup, gated on the golden suite.
The benefit is a cleaner e-graph that no longer peeks inside `Opaque` for ids, at the cost of touching the load-bearing oracle, availability, and seeding paths that make index reuse and the projection pull-up work.
Because the payoff is purely structural and the risk lands on plan-shaping code, it should be its own reviewed change verified against the full eqsat golden suite, not folded into unrelated work.
This needs a go/no-go from the user before spending the churn.

## The `MirRelationExpr<Inner>` functor

Parameterizing the child type with a default, `MirRelationExpr<Inner = Box<Self>>`, so the e-graph enode is `MirRelationExpr<Id>` and the normal tree keeps the defaulted `MirRelationExpr<Box<Self>>`, would collapse the `Rel`/`ENode` duplication into one algebra and end the drift between lowering and raising.
The default is the key: `MirRelationExpr` is referenced across 118 files in `src/`, but every one of those references binds the default `Inner`, so they stay source-compatible and compile unchanged.
The blast radius is therefore the type definition and its inherent methods, not the 118 call sites.
The real work is making the e-graph instantiate `MirRelationExpr<Id>` and handling the non-child payloads: `typ`, the join `implementation`, and access strategies are not children and stay attached regardless of `Inner`, and the e-graph wants flat interned child ids where the default wants `Box`, so a few methods that recurse through children need to be generic over `Inner` rather than assuming `Box`.

Recommendation: viable, more so than a from-scratch second algebra.
The defaulted type parameter is the standard way to generalize a widely-used type without touching its callers, and it removes the standing `Rel`/`ENode` drift risk that the current two-type design carries.
The cost is concentrated in the type's own methods and the e-graph bridge, not spread across the tree, so this is a bounded refactor worth scoping as its own change rather than a multi-week rewrite.
It still wants explicit buy-in before starting, because it reshapes the core relational type, but the earlier "118-file blast radius" framing was wrong: the default absorbs the callers.

## Summary

* Constant: skip, the `Opaque`/synthesized-empty split is already correct.
* Global Get: viable semantics-preserving cleanup, no plan change, but touches load-bearing oracle code; do it as its own golden-verified change only on a user go-ahead.
* `MirRelationExpr<Inner = Box<Self>>`: viable. A defaulted child-type parameter keeps all 118 callers source-compatible; the work is concentrated in the type's own methods and the e-graph bridge. Worth scoping as its own change, with buy-in, and likely the better long-term answer than maintaining two algebras.
