# MIR term graph: from tree AST to shared term graph

- Associated: (none yet)

## The Problem

`MirScalarExpr` (`src/expr/src/scalar.rs:69`) and `MirRelationExpr` (`src/expr/src/relation.rs:100`) are tree ASTs.
Their recursive children are hardcoded to owning pointers: `Box<MirScalarExpr>` and `Vec<MirScalarExpr>` for scalars, `Box<MirRelationExpr>` and `Vec<MirRelationExpr>` for relations.

The tree encoding has two structural deficits.

First, it invites algorithms that recurse to the depth of the tree.
Expression depth is user-controllable, for example a deeply nested boolean chain or `((1+1)+1)+1...`.
Deep trees overflow the stack.
The codebase already carries scar tissue for this.
The general `Visit` machinery was rewritten to be iterative with an explicit worklist (`src/expr/src/visit.rs:206`).
`MirRelationExpr` carries a hand-written `Eq` whose only purpose is to avoid a stack overflow in the derived one (`src/expr/src/relation.rs:93`).
A residual set of hand-written recursive methods on `MirScalarExpr` remains unguarded: `typ`, `eval`, `could_error`, `non_null_requirements`, and the `Debug`/`Display` formatters.
The derived `Ord`/`Hash`/`PartialEq` on both types also recurse to depth.

Second, a tree cannot share subterms.
Identical subexpressions are stored once per occurrence.
This costs memory and forecloses common-subexpression elimination and any rewrite engine that relies on structural sharing.

We want an incremental path from the tree AST to a term graph, where children are indices into an arena and identical subterms can be deduplicated by hash-consing.

## Success Criteria

* A single enum definition describes both a tree (owning-pointer children) and a term graph (index children), with the same variants and no parallel type.
* Existing code that uses the tree keeps compiling with minimal churn.
* Deeply nested expressions no longer overflow the stack in the graph representation.
* The graph representation supports hash-consing so identical subterms share one node.
* The change lands incrementally.
  Each step is independently reviewable and leaves the tree in a working state.

## Out of Scope

* Changing a durable, cross-version serialization format.
  MIR is never persisted across binary versions.
  The serde encoding only accelerates restart and plan transport within a single binary version, so serde byte-compatibility is not a constraint on this work.
* Retiring the tree representation.
  The tree remains the ingestion and debug form (see the Solution Proposal).
* Reducing the size of the `MirScalarExpr` enum itself, for example by boxing the `Literal` payload.
  This is a separate memory optimization.
* Depending on the `eqsat` optimizer.
  `eqsat` is informative prior art (see Alternatives) but lives in a separate, unmerged worktree.

## Solution Proposal

### Summary

Make each AST enum generic over the type of its recursive children, with a default that reproduces the tree exactly.
Introduce a term-graph arena that instantiates the same enum with an index child type.
Convert the hazardous recursive algorithms to iterative passes over the arena.
Add hash-consing on top of the arena.
Apply this pattern to scalars first, then relations, then optionally a combined arena.

The generic parameter is, on its own, inert.
`MirScalarExpr<Box<Self>>`, the default, keeps every current hazard byte-for-byte.
Stack-safety and sharing come only from code that runs on the arena representation.
The parameter is the bridge that lets the tree and the graph be one type, so passes port incrementally without maintaining a parallel enum.

### Generic encoding

Scalars take one parameter, because a scalar never embeds a relation.

```rust
enum MirScalarExpr<Inner = Box<MirScalarExpr>> {
    Column(usize, TreatAsEqual<Option<Arc<str>>>),
    Literal(Result<Row, EvalError>, ReprColumnType),
    CallUnmaterializable(UnmaterializableFunc),
    CallUnary { func: UnaryFunc, expr: Inner },
    CallBinary { func: BinaryFunc, expr1: Inner, expr2: Inner },
    CallVariadic { func: VariadicFunc, exprs: Vec<Inner> },
    If { cond: Inner, then: Inner, els: Inner },
}
```

`Inner` is the child-reference type.
The tree is `MirScalarExpr<Box<MirScalarExpr>>` (the default).
The graph is `MirScalarExpr<ScalarId>`.

Relations take two parameters, because a relation node has two child kinds: sub-relations and embedded scalars.

```rust
enum MirRelationExpr<R = Box<MirRelationExpr>, S = MirScalarExpr> {
    // R for sub-relations: `input: R`, `inputs: Vec<R>`, `value: R`, `body: R`,
    //   `base: R`, and `LetRec.values: Vec<R>`.
    // S for embedded scalars: `Map.scalars: Vec<S>`, `Filter.predicates: Vec<S>`,
    //   `FlatMap.exprs: Vec<S>`, `Join.equivalences: Vec<Vec<S>>`,
    //   `Reduce.group_key: Vec<S>`, `AggregateExpr.expr: S` (nested via `Reduce.aggregates`),
    //   `TopK.limit: Option<S>`, `ArrangeBy.keys: Vec<Vec<S>>`.
    // The conversion must cover every recursive slot above, or subterms are dropped.
}
```

The tree is `MirRelationExpr<Box<MirRelationExpr>, MirScalarExpr>` (the default).
The graph is `MirRelationExpr<RelId, ScalarId>`.

Several properties follow from the encoding.

The eight standard and serde derives (`Clone`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`, `Serialize`, `Deserialize`) expand to `impl<Inner: Trait> Trait for MirScalarExpr<Inner>`.
So `MirScalarExpr<ScalarId>` gets shallow, index-based, non-recursive `Ord`/`Hash`/`Eq` for free.
That is exactly what the hash-cons dedup map needs, and it lets the manual `Eq` hack die on the graph.
The tree instantiation keeps deep, structural semantics.

`MzReflect` is the one exception, and it is a hard prerequisite rather than a free ride.
Its derive macro emits a non-generic impl header (`impl MzReflect for MirScalarExpr`, `src/lowertest-derive/src/lib.rs:96`), which does not compile against a generic `MirScalarExpr<Inner>`.
Genericization must therefore either fix the macro to emit `impl<Inner: MzReflect> MzReflect for MirScalarExpr<Inner>` with the right bound, or retire `MzReflect` on these types.
See the Open questions.

The default is the literal `Box<Self>`, not a newtype wrapper.
Existing `Box::new(...)` construction and deref keep compiling.

`CallVariadic` becomes `Vec<Inner>`, which for the tree is `Vec<Box<MirScalarExpr>>` rather than today's `Vec<MirScalarExpr>`.
Serde treats `Box` transparently, so the serialized form is unchanged.
Type references resolve through the defaults and need no edit, so the churn is not proportional to the roughly one thousand `MirScalarExpr::` sites.
It concentrates in two places: variadic element access, which now yields `&Box<_>` and needs an explicit deref in match arms, and, for the relation sub-project, every `Map.scalars` / `Filter.predicates` style pattern that must survive the `S` default.
The relation two-parameter change is the wider of the two and is scoped to its own sub-project.

### Term-graph substrate

The arena stores nodes in a `Vec` indexed by a newtype id.

```rust
struct ScalarId(u32);

struct ScalarGraph {
    nodes: Vec<MirScalarExpr<ScalarId>>, // topologically ordered: child ids < parent id
    root: ScalarId,
}
```

The arena is topologically ordered so that every child id is smaller than its parent id.
Hash-consing adds a dedup map consulted on insert.

```rust
// std HashMap is banned repo-wide for determinism, so use BTreeMap or the mz_ore wrapper.
memo: BTreeMap<MirScalarExpr<ScalarId>, ScalarId>,
```

With the memo off, the arena is a plain structural graph that still allows duplicates.
With the memo on, identical subterms collapse to one id.

We reuse the shape of `eqsat`'s scalar node, not its storage.
`eqsat` already defines `enum SNode` (`src/transform/src/eqsat/scalar/node.rs:31`), a hand-written mirror of `MirScalarExpr` with ids as children, leaf payloads, and `TreatAsEqual` name handling.
Its interning, however, is fused to union-find, congruence rebuild, colors, and analysis hooks that a plain shared AST does not need.
We therefore copy the node design and write our own arena plus memo.
We shape `MirScalarExpr<ScalarId>` so that `SNode` can later collapse into it, deleting the parallel enum.

### Stack-safety

The passes split into two groups by whether every child is always visited.

The unconditional passes are `typ`, `could_error`, `non_null_requirements`, and the `Debug`/`Display` formatters.
Each combines results from all of a node's children, so it becomes a single linear bottom-up scan.
The pass allocates a `Vec<Result>` indexed by id and fills it in id order, and each node reads its children's already-computed results.
Because the arena is topologically ordered, no recursion is needed.

`eval` is different, because `If` short-circuits.
The enum contract requires that `then` and `els` are evaluated only according to `cond`, and this is the only construct through which a user can guard evaluation (`src/expr/src/scalar.rs:97`).
A uniform bottom-up fill would evaluate both branches, so `IF false THEN 1/0 ELSE 1` would wrongly error.
`eval` therefore must be a demand-driven walk: an explicit-stack top-down evaluation over the arena that honors `If` short-circuiting, not an eager fill.
Sharing helps `eval` only for subterms that are always reached and side-effect-free.
A memo may cache those, but it must never force a guarded branch, so the earlier framing of "memoizing shared subterms for free" does not apply to guarded positions.
This keeps `eval` iterative and stack-safe while preserving guard and error semantics.

The derived recursive `Ord`/`Hash`/`Eq` hazard vanishes on the graph because the derives are shallow there.
Tree-to-graph and graph-to-tree conversion are themselves iterative topological scans, so conversion cannot overflow either.

### Migration boundary

The graph is canonical where sharing and traversal matter, and the tree is canonical where construction and inspection matter.
The split is by role, not by persistence.

* Tree is the ingestion and debug form.
  SQL planning builds trees, which is natural with owning pointers.
  `Debug`, `Display`, and tests read trees.
* Graph is the optimizer's working IR and the same-version plan-transport form.
  We lower tree to graph once at the SQL-to-MIR boundary.
  Every transform pass runs on the graph.
  We serialize the graph for same-version transport.
  We lower graph to LIR or the dataflow plan at the end.

This answers the round-tripping concern directly.
Conversions live at the optimizer's two edges, not per pass.
CSE and any rewrite substrate live inside optimization, where they matter.
The end-state graph converges with `eqsat`'s combined arena, used as the general optimizer IR rather than only for saturation.

Until a given pass is ported, we wrap just that pass: raise to tree, run the tree pass, lower back to graph.
This shim is temporary and self-liquidating.
It shrinks to nothing as passes port, so round-tripping is a transient cost of the migration, not the steady state.

### Sub-project sequencing

The dependency order is forced: relations embed scalars, so the scalar representation must settle first.

1. Scalars.
   `MirScalarExpr<Inner>`, the scalar arena, iterative passes, and hash-consing.
   Self-contained, and delivers scalar stack-safety on its own.
   Proves the pattern.
2. Relations.
   `MirRelationExpr<R, S>`, the relation arena with scalar children as `ScalarId`.
   Reuses the scalar pattern and deletes the manual `Eq` hack on the graph.
3. Combined arena.
   One id space in the style of `eqsat`'s `CNode`, only if measurement shows that cross-kind sharing or a single transport form justifies it.
   Otherwise two paired arenas suffice.

Each sub-project runs the same internal phases: genericize the enum, add the arena and conversions, convert the hazardous passes to iterative form, then add hash-consing.

## Minimal Viable Prototype

The prototype is the first phase of the scalar sub-project: add `<Inner = Box<MirScalarExpr>>` to `MirScalarExpr` and make the tree compile again with no behavior change.
This validates the central mechanical claim, that the generic default reproduces the tree with narrow churn.
It must also resolve the confirmed `MzReflect` blocker (see Open questions), since nothing else in phase 0 compiles until the derive emits a generic impl or is retired on these types.

## Alternatives

* Keep the tree and guard every recursive method with `mz_ore::stack::maybe_grow`.
  This treats the symptom, not the cause.
  It adds no sharing, no CSE, and no rewrite substrate, and it leaves a growing set of call sites to audit.
* A separate, non-generic term-graph type, for example a dedicated `ScalarGraph` node enum.
  This is what `eqsat`'s `SNode` is.
  It forces a parallel enum and lower/raise bridges to be maintained forever, and it duplicates the variant set.
  The generic parameter avoids the parallel type.
* Adopt `eqsat`'s `EGraph` wholesale as the optimizer IR.
  Its interning is fused to union-find, congruence rebuild, colors, and analysis hooks that a plain shared AST does not need.
  We reuse its node design but not its storage.
* Make the graph canonical everywhere, including at ingestion.
  Rejected as the starting point because SQL planning constructs expressions naturally as trees, and forcing arena construction at that boundary adds friction without benefit.
  The tree stays as the ingestion form.

## Open questions

* How do we get `MzReflect` past the generic enum?
  The derive macro emits a non-generic impl header (`src/lowertest-derive/src/lib.rs:96`), so it does not compile against `MirScalarExpr<Inner>`.
  This is a confirmed phase-0 blocker, not a mere risk, and it is the first thing the prototype must resolve.
  Two paths: fix the macro to emit `impl<Inner: MzReflect> MzReflect for MirScalarExpr<Inner>` with the right bound, or retire `MzReflect` on these types.
  `MzReflect` is already slated for retirement but has no replacement, so genericization may force that decision.
  The prototype should record which path is cheaper.
* How wide is the two-parameter relation churn in practice?
  The `S` default must keep the roughly eighty referencing files compiling.
* What is the measured round-trip overhead of the raise/lower shim?
  This tells us when enough passes have ported to form a graph-only stretch.
* Do we need the combined arena, or do two paired arenas suffice?
  This is a measurement question deferred to after the relation sub-project.
* Should hash-consing be scoped per optimizer run, or should the arena persist and intern across a longer lifetime?
