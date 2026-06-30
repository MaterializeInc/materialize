# P3: IndexedFilter absorption (e-graph-native)

## Decision (supersedes the old commit-at-raise sketch)

The old plan assumed commit-at-raise would mirror WcoJoin.
It does not: WcoJoin works because delta planning is oracle-free and structure-preserving, and the WcoJoin node carries the *same* children as Join.
IndexedFilter is different: its key ordering must match a specific index (oracle), its literal rows need imperative MFP analysis, and `detect_literal_constraints` is tightly coupled to `TransformCtx` (oracle + notices).
None of that fits a declarative rewrite rule.

So P3 makes IndexedFilter a genuine **e-graph node**, seeded imperatively from the production detector, so cost-based extraction owns the decision (the faithful "absorption"), rather than a post-pass that duplicates the downstream transform.

## Core insight

An IndexedFilter is semantically `Filter[predicates](Get g)`.
Model it as exactly that, plus a cached physical realization:

`IndexedFilter { input: <Get class>, predicates: Vec<EScalar>, committed: Box<MirRelationExpr> }`

* `input` + `predicates` make it byte-for-byte equivalent to the `Filter` node it joins in the e-class, so **every analysis arm reuses Filter's logic** (`ENode::Filter { .. } | ENode::IndexedFilter { .. }`). Sound and exact.
* `committed` is the production `LiteralConstraints` output for that subtree (the semi-join `Join { implementation: IndexedFilter(..) }` wrapped in its MFP). It is an opaque payload (like `Opaque(MirRelationExpr)`), never unifies.
* Cost is a cheap point-lookup (does not scan `input`), so extraction prefers it when seeded.
* Physical raise (`commit_wcoj = true`) emits `committed` verbatim. Logical raise emits `Filter[predicates](raise(input))`. Seeding happens only in the physical pass, so the logical phase never contains the node; the logical raise arm exists only for totality/safety.

## Why this is sound regardless of bugs

The live boundary (`adopt_if_type_preserving`) rejects any output that changes arity or scalar types. An IndexedFilter semi-join preserves both. Any seeding/cost mistake degrades to "node not picked" or "boundary rejects → no-op". Downstream `LiteralConstraints` still runs and is a no-op on already-committed subtrees.

## Scope bound

Seed only the **direct** case: a Filter e-class whose input class holds a `Get { Id::Global }` and where the production transform, run on the reconstructed `Filter[p](Get g)`, returns an IndexedFilter join. Map/Project envelopes and residual predicates beyond the literal constraint are out of scope for seeding (downstream `LiteralConstraints` still handles them). `committed` captures the exact production output, so residual handling is correct *when* we seed.

## Tasks

### Task 1: IR + e-node variant (no behavior yet)
* `ir.rs`: add `Rel::IndexedFilter { input: Box<Rel>, predicates: Vec<EScalar>, committed: Box<MirRelationExpr> }`. Wire `arity` (= input.arity()), `children` (= [input]), `with_children`, `pretty`.
* `egraph.rs`: add `ENode::IndexedFilter { input: Id, predicates: Vec<EScalar>, committed: Box<MirRelationExpr> }`, `Sym::IndexedFilter`, `sym()`, `children()`, `map_children()`, `add_rel`, `arity_guarded`. Add to `rewrite_escalars` NO-REWRITE arm (predicates ride verbatim).
* Verify: `cargo check -p mz-transform`.

### Task 2: analyses share Filter's arms
* `analysis.rs` + `egraph.rs::node_column_types`: extend every `ENode::Filter { input, predicates }` arm to `ENode::Filter { input, predicates } | ENode::IndexedFilter { input, predicates, .. }`. Covers Equivalences, ConstantColumns, Keys, NonNeg, Monotonic, col_types.
* Verify: `cargo check`; existing analysis unit tests still green.

### Task 3: extraction + raise
* `egraph.rs` extraction: `ENode::IndexedFilter { .. } => Rel::IndexedFilter { .. }` (sign-preserving demand propagation identical to Filter).
* `raise.rs`: physical → `committed.clone()`; logical → `raise(input).filter(resolve(predicates))`.
* Test: `roundtrip` — a hand-built `Rel::IndexedFilter` raises to `committed` under commit=true and to `Filter(input)` under commit=false.

### Task 4: cost (cheap lookup)
* `cost.rs`: `IndexedFilter` charges a small constant work (proportional to result rows, not input scan) and no new arrangement memory (the index exists). It must cost strictly less than the sibling `Filter(Get)`. Add `Rel::IndexedFilter` to `size_degree`, `collect_work`, `collect_memory`.
* Test: cost(IndexedFilter) < cost(Filter(Get)) for the same get.

### Task 5: seeding
* `eqsat.rs` / `engine.rs`: thread a seed list `Vec<IndexedFilterSeed>` into `optimize_with_availability` → engine. After the e-graph is built from the lowered rel, for each seed find the class containing `Filter { input: G, predicates }` with `G` holding `Get { global_id }` and predicates matching, then `add` the `IndexedFilter` node and `union` it into that class.
* `transform.rs::PhysicalEqSatTransform`: walk `relation` for direct `Filter[p](Get g)` subtrees; for each, clone, run `LiteralConstraints.action(clone, ctx)`; if it became an IndexedFilter join, push a seed `{ global_id: g, predicates: lower(p), committed: clone }`.
* Test (transform-level): a plan `Filter[#0 = 5](Get g)` with an index on `(#0)` of `g` comes out of `PhysicalEqSatTransform` as an IndexedFilter join.

### Task 6: cse + lean totality
* `cse.rs`: treat `IndexedFilter` as a shareable compound (it has a child).
* `lean.rs`: `collect_binders` passthrough; `translate_pat`/translate handles the variant (no proof obligation beyond Filter's, since it is a Filter).
* Verify full `cargo test -p mz-transform eqsat`.

### Task 7: integration test + goldens
* Add an SLT or transform test proving end-to-end an indexed literal filter becomes an IndexedFilter through the eqsat physical pass.
* Rewrite any churned goldens (one SLT at a time; shared CockroachDB).

## After plan: agent reviews
Per user: dispatch agents to (a) review the implementation, (b) improve performance, (c) improve documentation.
