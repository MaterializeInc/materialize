# Equality saturation: a generic language-parameterized e-graph core (SP1)

- Associated: CLU-137; `doc/developer/design/20260625_eqsat_scalar_expressions.md` (the scalar engine this builds on)

> File and line references were grounded on 2026-06-27 against branch
> `claude/mir-equality-optimizer-sodbej`. Re-verify before relying on a specific
> line; they drift.

> This is the design for **Sub-Project 1 (SP1)** of a larger effort to unify the
> relational and scalar equality-saturation engines. SP1 is a self-contained,
> behavior-neutral refactor. The overarching vision and the remaining
> sub-projects are summarized under "Where this fits" so the SP1 abstraction is
> shaped by its eventual consumers; SP2–SP4 get their own design + plan cycles.

## Where this fits (the overarching effort)

`src/transform/src/eqsat/` today holds **two** equality-saturation engines with
near-duplicate cores:

- the mature **relational** engine (top-level files: `egraph.rs`, `engine.rs`,
  `analysis.rs`, `cost.rs`, `extract.rs`, `matcher.rs`, `dsl.rs`, `rules.rs`,
  Lean emission, ILP) over an `ENode` language, and
- the lean **scalar** engine (`scalar/`) over an `SNode` language, built to fix
  the CLU-137 bug class and currently wired through `canonicalize_predicates`.

The agreed end state is **runtime integration**: scalars become first-class
participants in the relational e-graph via a **colored e-graph** (Singher–Itzhaky
style), so that context-dependent scalar equalities (`col0 = col1` valid only
under a particular `Filter`/`Join` scope) are represented as *colored congruence*
and the relational `Equivalences` analysis + Phase-2a payload rewriting are
**subsumed**, not preserved. Context-free scalar rewrites (the 21 rules:
const-fold, boolean algebra, De Morgan, null/error propagation, `isnull_fold`)
live in the base e-graph and are inherited by all colors.

Decomposition (each its own design + plan + implementation cycle):

- **SP1 — Generic language-parameterized core (this document).** Make the
  e-graph data structure, congruence closure, and analysis framework generic
  over a `Language` trait. Relational stays the sole instance. Behavior-neutral.
- **SP2 — Scalar as a second instance.** Define the scalar `Language`; port
  `SNode` and the 21 rules to the (extended) rewrite DSL with `sorry`-stubbed
  Lean theorems; express the scalar analyses (`could_error`, `literal`) as
  `Analysis` impls; provide a tree-size cost. Retire the standalone `scalar/`
  engine; `canonicalize_predicates` runs through the unified engine.
- **SP3 — Colored e-graph mechanism.** Base + colored union-finds, hierarchical
  colors, colored congruence, color-aware extraction; built generically and
  tested in isolation. Opens with a color-explosion measurement spike.
- **SP4 — Multi-sort runtime integration.** Multi-sorted nodes (relational nodes
  reference scalar e-class `Id`s), scalars as first-class e-classes, color
  derivation from relational structure (subsuming `Equivalences`), color-aware
  joint cost/extraction.

SP1 deliberately does **not** genericize the saturation loop, the rule
machinery, or cost/extraction. The saturation loop will be redesigned by colors
(SP3) regardless, and the rule machinery is best genericized once SP2 provides a
second concrete consumer to constrain the abstraction.

## The Problem

The two engines duplicate the genuinely shared e-graph substrate — `Id`,
union-find, the class map, the hashcons, the congruence-closure `rebuild`, the
matcher index, and a per-class analysis fixpoint — in two places
(`src/transform/src/eqsat/egraph.rs` and `src/transform/src/eqsat/scalar/egraph.rs`).
That duplication is a tax on every subsequent step of the unification effort:
colored e-graphs (SP3) and multi-sort integration (SP4) cannot be built once and
shared until there is one core to build them on. There is no type parameter for
"the node language" anywhere today; both `EGraph` structs are hardcoded to their
respective node enums.

## Success Criteria

- A single generic e-graph core — `EGraph<L: Language>`, its congruence
  operations, and an `Analysis<L>` framework — exists in a new `eqsat/core`
  module and is the substrate the relational engine runs on.
- The relational engine is the sole instantiation (`EGraph<RelLang>`) and is
  **behavior-neutral**: the existing eqsat unit tests, datadriven tests, and the
  full sqllogictest corpus pass with **zero golden diffs**, and the `build.rs`
  rule-codegen path still builds.
- The abstraction is demonstrably reusable for a second language: a small toy
  `Language` (arithmetic expressions) exercises `add`/`union`/`rebuild`/`find`/
  `index` generically in unit tests, proving the core is not secretly
  relational before SP2 commits to it.
- The `Language` trait and `Analysis<L>` signatures are shaped so that the
  scalar `SNode` language (SP2), colors (SP3), and multi-sort references (SP4)
  can be added without re-opening the core's public surface beyond planned
  extension points.

## Out of Scope

- The saturation loop (`EGraph::saturate`): Phase-2a equivalence
  canonicalization, `AnalysisNeeds` gating, eq-caching, and per-rule backoff
  stay relational-typed and unchanged. (Redesigned by SP3.)
- The rule machinery: the rewrite DSL (`dsl.rs`), the WCOJ matcher
  (`matcher.rs`), build-time codegen (`build.rs`, `rules.rs`), `CompiledRule`,
  `EBindings`, `Payload`, and Lean emission (`lean.rs`). (Genericized in SP2.)
- Cost and extraction: `CostModel`, `Cost`, `Objective`, `Extractor`,
  `GreedyExtractor`, `IlpExtractor`. (Genericized later.)
- The scalar engine itself (`scalar/`): untouched by SP1; ported in SP2.
- Colors, multi-sort nodes, and any change to how scalars are stored in
  relational e-nodes (`EScalar` payloads). (SP3/SP4.)
- Any behavior or plan-quality change. SP1 is a pure refactor.

## Solution Proposal

Introduce a new module `src/transform/src/eqsat/core` (or `core.rs`) holding the
language-agnostic substrate, and reduce the relational `EGraph` to an
instantiation of it via a type alias, so existing call sites and generated code
keep compiling.

### The `Language` trait

```rust
pub type Id = usize;

pub trait Language {
    /// The e-node type. Operator nodes carry child `Id`s; leaves carry payload.
    type Node: Clone + Eq + std::hash::Hash + Ord + std::fmt::Debug;
    /// The operator symbol used to bucket e-nodes in the matcher index.
    type Sym: Eq + std::hash::Hash + Clone;
    /// Language-owned auxiliary state stored on the e-graph. Relational uses this
    /// for the index-availability oracle; scalar will use `()` (or col types).
    type GraphData: Default;
    /// Per-run context handed to `Analysis::make`. Relational supplies arity;
    /// scalar will supply column types. A GAT so it can borrow per run.
    type AnalysisCtx<'a>;

    /// Child e-class ids in operand order (empty for leaves).
    fn children(node: &Self::Node) -> Vec<Id>;
    /// Rewrite child ids (used to canonicalize a node against the union-find).
    fn map_children(node: &Self::Node, f: impl FnMut(Id) -> Id) -> Self::Node;
    /// The operator symbol for index bucketing.
    fn symbol(node: &Self::Node) -> Self::Sym;
}
```

`children`/`map_children` already exist on both `ENode` (`egraph.rs:257`,
`egraph.rs:280`) and `SNode` (`scalar/node.rs:65`, `scalar/node.rs:90`) with
these exact shapes; SP1 routes them through the trait. `symbol` formalizes the
existing relational `Sym` projection used to build the matcher `Index`.

### `EGraph<L>` and congruence

```rust
pub struct EGraph<L: Language> {
    uf: Vec<Id>,
    pub(crate) classes: HashMap<Id, HashSet<L::Node>>,
    memo: HashMap<L::Node, Id>,
    data: L::GraphData,
}
```

The generic methods are the current relational logic with `ENode` replaced by
`L::Node`: `find`, `new_class`, `canon` (via `L::map_children`), `add` (hashcons
+ new class), `union` (fold `rb` into `ra`), `rebuild` (the three-phase
congruence closure), and `index()` (group canonical nodes by `L::symbol` into
`HashMap<L::Sym, Vec<(Id, L::Node)>>`). These are lifted verbatim, so
behavior-neutrality is structural.

The relational `available` oracle (`egraph.rs:380`) moves into
`RelGraphData { available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> }` and is
read through an `available()` accessor; `set_available` writes `self.data`. This
accessor is the only site generated rule code must be updated to call.

### `Analysis<L>` framework

```rust
pub trait Analysis<L: Language> {
    type Domain: Clone + Eq;
    fn bottom(&self) -> Self::Domain;
    fn make(
        &self,
        node: &L::Node,
        get: &dyn Fn(Id) -> Self::Domain,
        ctx: &L::AnalysisCtx<'_>,
    ) -> Self::Domain;
    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain;
}
```

This is today's `Analysis` trait (`analysis.rs:36`) with two changes: it gains
the `L` parameter, and the relational-specific `arity: &dyn Fn(Id) -> usize`
third argument is generalized to `ctx: &L::AnalysisCtx<'_>`. For `RelLang`,
`AnalysisCtx<'a>` carries the arity provider (relational column count); for the
scalar language (SP2) it will carry `&'a [ReprColumnType]`. The generic
`run_analysis`/`run_analysis_bounded` drivers (`egraph.rs:1554`, `:1554+`) take
the ctx and are otherwise the existing monotone fixpoint. The five relational
analyses (`NonNeg`, `Monotonic`, `Keys`, `Equivalences`, `ConstantColumns`) each
change from `impl Analysis` to `impl Analysis<RelLang>` and read arity from the
ctx instead of the closure parameter — mechanical, one site each.

### Minimal-churn instantiation strategy

```rust
// in the relational module
pub type EGraph = core::EGraph<RelLang>;

impl core::EGraph<RelLang> {
    pub fn add_rel(&mut self, rel: &Rel) -> Id { /* unchanged */ }
    pub fn arity(&self, id: Id) -> usize { /* unchanged */ }
    pub fn saturate(&mut self, /* … */) -> usize { /* unchanged */ }
    // …seed_indexed_filters, column_types, set_available, extract_with, …
}
```

Because `EGraph` remains a type named `EGraph` (now an alias), every existing
reference — including the generated `fn(&EGraph, &Index, &Analyses, usize)`
fn-pointer types in `CompiledRule` (`rules.rs:60`) and the `apply` pointers
(`rules.rs:62`) — keeps compiling unchanged. Inherent impls on the concrete
`core::EGraph<RelLang>` are permitted because the type is local to the crate, so
the relational-specific methods stay exactly where call sites expect them. The
net churn is: new `core` module; `ENode → L::Node` inside the lifted methods;
`impl Analysis<RelLang>` on five analyses; `available` moved behind an accessor.

### File organization

- **New** `eqsat/core` (module or `core.rs`): `Id`, `Language`, `EGraph<L>`,
  congruence methods, `index`, `Analysis<L>`, `run_analysis*`, and the toy-
  language tests.
- `eqsat/egraph.rs`: keeps `ENode`, `Sym`, `Rel` lowering (`add_rel`), the
  `RelLang: Language` impl, `RelGraphData`, the relational inherent-impl block,
  and the `pub type EGraph` alias.
- `eqsat/analysis.rs`: the five analyses gain `<RelLang>`; the `Analysis` trait
  definition moves to `core`.
- Everything else unchanged.

## Minimal Viable Prototype

The toy-`Language` unit test is the MVP and the de-risking artifact: a tiny
arithmetic-expression language (`Num(i64)`, `Add(Id, Id)`, `Mul(Id, Id)`)
implementing `Language`, with tests that add nodes, union classes, rebuild, and
assert congruence (e.g. after `union(a, b)`, `Add(a, x)` and `Add(b, x)` collapse
to one class). If the core can host this second language with no relational
leakage, the abstraction is sound. This lands in the same PR as the refactor.

## Alternatives

- **Wider SP1 (genericize the saturation loop too).** Rejected: colors (SP3)
  rewrite the loop regardless, so genericizing it now is wasted motion, and it
  enlarges the behavior-neutral surface to verify.
- **Wider SP1 (genericize rules/cost/extraction too).** Rejected: front-loads
  the hardest genericization (build-time-generated fn-pointer types,
  `EBindings`/`Payload`, `CostModel`) with no second consumer to constrain it;
  SP2 is exactly that consumer.
- **Scalar-first / new parallel core / core-first standalone.** Rejected in
  favor of relational-first generalize-in-place: the mature engine carries the
  real machinery, so generalizing it in place avoids building and then
  reconciling a parallel core.
- **Newtype wrapper instead of a type alias for the relational `EGraph`.**
  Rejected: a newtype forces delegating every method and updating generated code;
  the type alias keeps call sites and codegen untouched.

## Open questions

- **GAT ergonomics for `AnalysisCtx<'a>`.** Confirm the GAT form compiles
  cleanly through the `run_analysis` drivers and trait objects used by the
  saturation loop; if it fights the borrow checker, fall back to passing the
  arity provider as an explicit driver argument rather than an associated type.
  (To be resolved during implementation; does not change the design's shape.)
- **Module vs file for `core`.** `core.rs` vs `core/` with submodules
  (`egraph.rs`, `analysis.rs`). Decide by size during implementation.
- **`children()` allocation.** Both engines already return `Vec<Id>`; SP1
  preserves that for behavior-neutrality. A `smallvec`/iterator optimization is
  possible later but is out of scope here.
