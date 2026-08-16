# MIR stack-safety hardening

- Associated: (none yet)

<!--
This spec is deliberately narrow and representation-independent.
It closes the stack-overflow class on the existing tree AST.
It does not depend on, and is not blocked by, the separate shared-representation
evaluation in 20260710_mir_shared_representation.md.
-->

## The Problem

`MirScalarExpr` (`src/expr/src/scalar.rs:69`) and `MirRelationExpr` (`src/expr/src/relation.rs:100`) are recursive tree ASTs.
Expression depth is user-controllable, for example a deeply nested boolean chain or `((1+1)+1)+1...`, and transforms can deepen expressions further.
Several operations recurse to the depth of the tree and can overflow the thread stack.

The hazard is not hypothetical.
The general `Visit` machinery was already rewritten to be iterative (`src/expr/src/visit.rs:206`).
`MirRelationExpr` carries a hand-written `PartialEq` whose only purpose is to avoid a stack overflow in the derived one (`src/expr/src/relation.rs:93`).
A residual set of unguarded recursive operations remains.

The residual set is closed and greppable:

* Hand-written recursive methods on `MirScalarExpr`: `typ` (`scalar.rs:1083`), `eval` (`scalar.rs:1172`), `could_error` (`scalar.rs:1208`), `non_null_requirements` (`scalar.rs:1049`), and the `Debug` (`scalar.rs:112`) and `Display` (`src/expr/src/explain/text.rs:1061`) formatters.
* The derived `Ord`, `Hash`, and `PartialEq` on `MirScalarExpr` (`scalar.rs:58`), which recurse structurally and are exercised whenever a scalar is used as a `BTreeMap`/`BTreeSet` key.
* On `MirRelationExpr`, the derived `Ord` and `Hash` (`relation.rs:99`), and a latent recursion inside the supposedly stack-safe manual `PartialEq`: its debug assert calls `self.cmp(other)`, the derived recursive `Ord` (`relation.rs:321`).

## Success Criteria

* A deeply nested expression, at any depth the system can construct or a transform can produce, does not overflow the stack in any of the operations above.
* The guarantee is a property of the code, not a convention that new code is expected to follow.
* The fix is representation-independent.
  It holds for the tree AST as it exists today and does not presuppose any move to a graph representation.

## Out of Scope

* Subterm sharing, common-subexpression elimination, and memory deduplication.
  Those are evaluated separately in `20260710_mir_shared_representation.md`.
* Any change to the `MirScalarExpr` or `MirRelationExpr` representation.

## Solution Proposal

Two mechanisms, applied together.

### Grow the stack at the hand-written recursive sites

Wrap the recursive body of each hand-written method with `mz_ore::stack::maybe_grow` (`src/ore/src/stack.rs:103`), which grows the stack on demand rather than merely bounding depth.
This covers `typ`, `eval`, `could_error`, `non_null_requirements`, and the two formatters.
There is precedent already: `consequence_for_input` guards scalar recursion with `maybe_grow` (`src/expr/src/relation/join_input_mapper.rs:407`).

### Close the derived-impl gap

`maybe_grow` cannot wrap a derived `Ord`/`Hash`/`PartialEq`, because the recursion lives in macro-generated code with no call site to instrument.
Two options close this, and the spec should pick one during implementation.

* Preferred: establish and preserve a MIR depth invariant.
  The planner already applies `CheckedRecursion` limits, but frame size times depth can still exceed a small thread stack, and transforms deepen expressions after planning.
  Bound depth at ingestion and re-check or cap it where transforms can increase it, so that the derived recursive impls are always called on trees shallow enough to be safe.
  This is the cheaper option and it also protects any future recursive method by construction.
* Fallback: hand-write iterative `Ord`/`Hash`/`PartialEq` for `MirScalarExpr`, mirroring the existing manual `PartialEq` on `MirRelationExpr`, and fix that manual impl's debug assert so it does not recurse through `cmp` (`relation.rs:321`).

The preferred option is a guarantee that scales to all recursive operations at once.
The fallback is local but must be repeated for every recursive derive and hand-written comparison.

## Minimal Viable Prototype

Reproduce a stack overflow with a deeply nested scalar, for example a long `AND` chain or repeated unary application, run through `typ` or a `BTreeMap` insertion.
Then apply the chosen mechanism and show the same input completes.
`src/expr/benches/static_eval.rs` already has a 1024-deep expression benchmark (`deep_add`) that can seed the reproduction.

## Alternatives

* Do nothing and rely on the existing partial guards.
  Rejected because the residual set above is unguarded and the manual `PartialEq` on `MirRelationExpr` still recurses in debug builds.
* Solve stack-safety by moving to a graph representation.
  Rejected as the mechanism for this problem.
  A graph invites iterative traversal but does not prevent recursion, and as long as the tree remains the ingestion and debug form, deep expressions still hit the tree-side recursive operations before any lowering.
  Stack-safety must be closed on the tree regardless, which is what this spec does.

## Open questions

* Which mechanism closes the derived-impl gap: the depth invariant or hand-written iterative impls?
  The depth invariant is preferred if transforms can be made to preserve it at acceptable cost.
* What is the safe depth bound, given the largest per-node frame among the recursive methods and the smallest thread stack the system runs with?
