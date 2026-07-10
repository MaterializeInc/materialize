# Shared MIR representation: measurement-gated evaluation

- Associated: (none yet)

<!--
Stack-safety is NOT a motivation for this work. It is closed independently and
more cheaply on the tree in 20260710_mir_stack_safety.md. This spec evaluates
whether a shared representation pays for itself on memory, and if so, in what
shape.
-->

## The Problem

`MirScalarExpr` (`src/expr/src/scalar.rs:69`) and `MirRelationExpr` (`src/expr/src/relation.rs:100`) are tree ASTs.
A tree stores every occurrence of an identical subterm separately.
This costs memory, and it forecloses any optimization that depends on knowing two subterms are the same.

The question this spec answers is narrow and empirical.
Do real Materialize plans contain enough duplicate subterms that a shared representation saves meaningful memory?
Every other claimed benefit of sharing is either available more cheaply elsewhere or is not delivered by the representation alone.

* Stack-safety is representation-independent and is closed on the tree in `20260710_mir_stack_safety.md`.
  A graph invites iterative traversal but does not prevent recursion, and the tree remains the ingestion and debug form regardless.
* Common-subexpression elimination as a rewrite win is cost-model-bound, not representation-bound.
  Prior equality-saturation work found no free CSE win: extraction re-expands shared subterms inline unless the cost model is sharing-aware.
  Structural sharing stays invisible to costing, so it does not by itself improve plans.

The only benefit a shared representation uniquely and genuinely delivers is memory saved by deduplicating subterms, and that number is currently unmeasured.

## Success Criteria

* A measured baseline exists for the duplicate-subterm fraction of real plan memory, on representative workloads.
* The decision to build a shared representation is gated on that number being large enough to matter.
* If built, the representation deduplicates identical subterms and provides constant-time structural equality.
* If built, it does not regress correctness.
  In particular, it is sound under the rewrite style the optimizer actually uses.

## Out of Scope

* Stack-safety.
  See `20260710_mir_stack_safety.md`.
* A durable, cross-version serialization format.
  MIR is never persisted across binary versions, so serde byte-compatibility is not a constraint.
* Dependence on the `eqsat` optimizer.
  `eqsat` is unmerged and its capability findings were negative.
  This design must pay for itself as a mainline optimizer IR on its own terms.
  `eqsat`'s scalar node is cited only as a reusable node shape, not as a destination.

## Minimal Viable Prototype

The prototype is a measurement, not a representation change.

Instrument plan construction to count, for representative plans, the number of distinct subterms versus the total number of subterm occurrences, and estimate the memory that hash-consing would reclaim.
Run it on the introspection catalog plans and on TPC-H.
Report the duplicate-subterm fraction and the estimated memory saving.

This number gates everything below.
If the fraction is small, the right move is to stop after the stack-safety spec.

## Solution Proposal

This section applies only if the measured duplicate-subterm fraction justifies the work.

### Enabling encoding

Make each AST enum generic over the type of its recursive children, with a default that reproduces the tree exactly.

```rust
enum MirScalarExpr<Inner = Box<MirScalarExpr>> { /* single children `: Inner`, variadic `: Vec<Inner>` */ }
enum MirRelationExpr<R = Box<MirRelationExpr>, S = MirScalarExpr> { /* R for sub-relations, S for scalars */ }
```

The tree is the default instantiation.
A shared representation instantiates the same enum with an index child type.
One enum, no parallel type.
The eight standard and serde derives expand to `impl<Inner: Trait>`, so the index instantiation gets shallow, index-based, constant-time `Ord`/`Hash`/`Eq` for free.

The recursive child slots must be enumerated exhaustively, or a conversion silently drops subterms.
For relations that is `input`, `inputs`, `value`, `body`, `base`, `LetRec.values` (sub-relations), and `Map.scalars`, `Filter.predicates`, `FlatMap.exprs`, `Join.equivalences`, `Reduce.group_key`, `AggregateExpr.expr`, `TopK.limit`, `ArrangeBy.keys` (scalars).

### Destination: add-only memo, not a mutable shared AST

The honest destination, if the memory number justifies building anything, is a Cascades-style add-only memo: nodes stored once, children referenced by id, deduplicated on insert.
It is not a mutable shared AST.

The reason is the rewrite style.
Materialize transforms mutate expressions in place via `visit_mut`.
On a hash-consed arena that is unsound: mutating a shared node mutates it under every parent, wanted or not.
This is why every graph-world optimizer that survived, Cascades and egg among them, is add-only, and why in-place systems stay tree-plus-let.
Cranelift and Calcite's HepPlanner both accumulated this exact class of DAG-versus-tree rewrite bug.

The dominant cost of this work is therefore not making traversals iterative.
It is converting every ported transform from in-place mutation to build-new-nodes style.
Porting a pass means rewriting its rewrite discipline, and the plan must budget for that, not for a mechanical traversal reshape.

### Reuse

`eqsat` already defines `enum SNode` (`src/transform/src/eqsat/scalar/node.rs:31`), a hand-written mirror of `MirScalarExpr` with ids as children and leaf payloads.
Reuse its node shape.
Do not lift its `EGraph` storage, which is fused to union-find, congruence rebuild, colors, and analysis hooks a plain memo does not need.
Shape the generic index instantiation so `SNode` could later collapse into it.

### Phase-0 scope guards

* `MzReflect`: its derive macro emits a non-generic impl header (`src/lowertest-derive/src/lib.rs:96`), so it will not compile against `MirScalarExpr<Inner>`.
  Patch the macro to emit `impl<Inner: MzReflect> MzReflect for MirScalarExpr<Inner>` with the right bound.
  Do not let a `MzReflect` retirement ride along.
  Retirement is an unrelated ecosystem migration with no replacement in hand.
* The two-parameter relation default nests defaults, `MirRelationExpr<Box<MirRelationExpr>, MirScalarExpr<Box<MirScalarExpr>>>`.
  Spike this specific defaulting interaction before committing to the two-parameter shape.
  About one hundred files reference each type.

### Migration and kill criteria

If passes are ported incrementally, a raise/lower shim runs per unported pass inside the fixpoint loop, so its cost is paid repeatedly.
A half-migrated state is strictly worse than either endpoint, two representations plus shims carried indefinitely.
Set commit criteria before starting.
For example: if after the relation phase the round-trip exceeds a set fraction of optimize time, or fewer than a set number of passes have ported, revert to tree plus the stack-safety guards.

## Alternatives

### Syntactic sharing, the tree-native option Materialize half-has

Materialize already expresses sharing syntactically: `Let` and `LetRec` plus `RelationCSE` for relations, and hoisting into `Map` columns for scalars.
Its advantage over structural sharing is decisive for optimization: passes and the cost model can see the sharing.
Structural sharing in a hash-consed arena is invisible to costing unless the extractor is made sharing-aware, which is the same wall the prior CSE work hit.
Extending syntactic sharing is the lower-risk path to the CSE benefit and should be weighed against building a memo.

### What other IRs do

| System | Substrate | Sharing mechanism |
|---|---|---|
| LLVM | CFG + SSA | SSA use-def edges share values. Types, constants, attributes hash-consed in the context |
| GHC Core | Tree AST | Syntactic sharing via `let`. CSE pass introduces lets |
| MLIR | Op graph | Types and attributes uniqued, ops not hash-consed |
| HotSpot C2, Graal, old V8 | Sea of nodes | Full graph with built-in GVN. V8 retreated to CFG-based Turboshaft in 2023 over debuggability and scheduling cost |
| Cranelift | CFG + acyclic e-graph mid-end | Hash-consing plus equivalence classes for pure ops, elaborated back out |
| Z3, BDD packages | Fully hash-consed DAG | Perfect structural sharing, mandatory because formulas share exponentially |
| Lean, Coq kernels | Pointer-shared terms + hash caches | Hash-consing |
| SQL Server, Orca, CockroachDB, Calcite-Volcano | Cascades memo | Groups plus expressions whose children are group ids, a hash-consed term graph plus e-classes |
| DuckDB, Calcite-Hep, Postgres, Materialize today | Tree + directional rewrites | `Let`/`LetRec` plus CSE passes, syntactic sharing |

Two lessons bound the design space.
Every serious cost-based optimizer converged on the Cascades memo, so the add-only-memo destination has ancestry.
But the midpoint this work must avoid, a hash-consed DAG driven by directional in-place rewrites, is thinly populated for a reason: tree-world systems stay tree-plus-let, graph-world systems go add-only.
Nobody gets stack-safety from the representation.

## Open questions

* What is the duplicate-subterm fraction of real plan memory?
  This is the gate, and the prototype answers it.
* If the number justifies building something, is a full Cascades-style memo warranted, or does extending syntactic sharing capture most of the benefit at lower risk?
* Can the cost model and extractor be made sharing-aware?
  Without this, structural sharing yields memory savings but no plan-quality improvement.
* What is the measured per-pass round-trip cost of the shim, and where is the kill threshold set?
