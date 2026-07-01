# SP4d — eqsat colored saturation

Status: design (2026-06-29)
Branch: `claude/mir-equality-optimizer-sodbej`
Predecessors: SP4a (multi-sort fusion), SP4b (colored contextual equalities),
SP4c (colored runtime hardening).
Consumes: SP3b's still-unused colored machinery (`close`/`close_all`/
`add_colored`/`extract_colored`/`remove`) and the SP4b colored runtime
(`derive_facts`, the `ColoredEGraph` overlay).

## 1. Goal

Re-run rewrite rules *inside* contextual colors so that contextual equalities
compose with the **full rewrite system**, not only with extraction-time
cheapest-spelling substitution (SP4b's reach). This is the foundational
capability the colored e-graph was designed for: a contextual equality such as
`#0 = #1` under a filter lets a predicate `#1 = #0` rewrite to `#0 = #0`, which a
rule simplifies to `true`, which lets a filter-removal/pushdown rule fire — a
*chain of rewrites*, not a single substitution.

This cycle is **capability-first**. Success is:
- the colored rule driver works and is the production home for contextual rules;
- SP3b's dead machinery (`close`/`close_all`/`add_colored`/`extract_colored`/
  `remove`) is exercised in production;
- the contextual rewrite is visibly applied at the **eqsat-output level** (the
  SP4b filter-placement case is recovered there, accepting that the full
  pipeline normalizes it downstream — see §9);
- **zero end-to-end regressions**.

The immediate behavioral payoff (saturation-time filter pushdown) is normalized
away by the full optimizer pipeline, so it is invisible end-to-end. We build the
capability anyway because it is the substrate future contextual rules require;
the success criterion is therefore the capability + eqsat-level visibility +
zero regressions, **not** an end-to-end diff.

## 2. Scope

**In:**
- (A) A `GraphView`/`GraphSink` trait abstraction over the graph operations the
  rule codegen emits, so compiled rules run over either the base (black) layer or
  a colored view. Black layer routed through the traits is **byte-identical**.
- (B) A colored rule driver: per color, run a **tagged targeted subset** of
  rules over the colored view, applying conclusions via `add_colored` and closing
  congruence via `close`.
- (C) **Hierarchical** color derivation: a color forest ordered by
  equality-set inclusion, with `remove`-in-`close` pruning redundant child-delta
  edges.
- (D) **Incremental dirty-tracking congruence** in `close` (perf).
- (E) Color-threaded **relational** extraction via `extract_colored`, extending
  SP4b's scalar-only color threading.

**Out (deferred):**
- Color-aware analyses (NonNeg/Keys/Monotonic) and the analysis-gated rules that
  read them. The colored subset is deliberately restricted to color-exact side
  conditions so these analyses need not become color-aware (§5). Making them
  color-aware (and thereby admitting analysis-gated rules into colored
  saturation) is future work.
- Color-aware key/arrangement cost reasoning (Join/Reduce key payloads). The
  targeted subset is predicate/filter-structural; it does not rewrite
  Join/Reduce key payloads, so `cost::CostModel` arrangement accounting stays as
  in SP4b. (This is the same boundary SP4b's refit chose.)
- The full rule set running colored (only the tagged subset participates).

## 3. Background: what exists today

Mapped against the current branch (`src/transform/src/eqsat/`):

- **The colored mechanism is a read-only overlay.** `ColoredEGraph<'b, L> {
  base: &'b EGraph<L>, colors, next_colored_id }` (`colored.rs`) immutably
  borrows the frozen base and owns colored deltas. `find`/`union`/`canon`/
  `colored_class_members`/`visible_nodes` are layered through the ancestor chain.
- **`close`/`close_all`** (`colored/congruence.rs`), **`add_colored`**
  (`colored/conclusions.rs`), **`extract_colored`** + `CostModel` trait
  (`colored/extract.rs`), and **`remove`** (`colored/union_find.rs`) are built,
  unit-tested on `ToyLang`, and **uncalled in production** (`#![allow(dead_code)]`
  at `colored.rs:6`).
- **SP4b uses the overlay only at extraction time.** `colored_derive::derive`
  (called from `engine.rs:106`) interns reduced scalar spellings into the base
  *before* freeze; `build_colored_layer` then builds a flat overlay (one color per
  distinct equality-set, each a child of black); `resolve_scalar_colored`
  resolves *scalar payloads* to their cheapest colored spelling during
  extraction. No new relational nodes are created contextually; no rule re-runs.
- **The rule engine is bound to the concrete base.** Compiled `find`/`apply`
  have signatures `fn(&EGraph, &Index, &Analyses, usize) -> (Vec<EBindings>,
  bool)` and `fn(&mut EGraph, &EBindings) -> Result<Id, String>`
  (`rules.rs`). The codegen (`build/codegen.rs`) emits, for `find`:
  `eg.rel_class_ids()` (pure-relvar root range), `index.get(&Sym::X)` (sym root
  range), `eg.rel_class_nodes(class)` (child enumeration), and side conditions
  `eg.arity`/`eg.data()`/`eg.cond_*`/`eg.column_types`; for `apply`:
  `eg.add(CNode::Rel(...))`, `eg.binding_arities`, and (via `matcher.rs`)
  `eg.intern_scalar`. Matching is **global over the base**, with **no** color
  awareness.
- **`CombinedLang`** (`egraph/combined.rs`): `CNode = Rel(ENode) | Scalar(SNode)`
  on one `Id` space; `ENode` scalar payloads are scalar-class `Id`s.

The crux SP4d resolves: to reuse the compiled rules contextually, the
graph-operation surface the codegen emits must be abstracted so a colored view
can supply color-canonical reads and colored-delta writes. There is **no**
index-only shortcut, because child traversal (`rel_class_nodes`) and side
conditions read the concrete graph directly.

## 4. Architecture & invariants

The base stays **frozen** after context-free saturation (as SP4b). All
contextual rewriting happens in the `ColoredEGraph` overlay, which immutably
borrows the frozen base and owns colored deltas. The base is never mutated by
colored saturation, so the black-layer result is unchanged unless a color
supplies a cheaper, valid conclusion the extractor prefers.

Two invariants govern the whole design:

1. **Black-layer byte-identity.** The trait abstraction (A) is first landed as a
   pure refactor: `&EGraph` routed through `GraphView`/`GraphSink` produces
   identical saturation output and identical goldens. Behavior changes only when
   the colored driver (B) is switched on.
2. **Colored soundness by confinement + exact conditions.** A colored conclusion
   lives only in its color's delta (`add_colored`), never escaping to the base or
   sibling colors. It is sound because (a) the seeding equalities come from the
   proven `Equivalences` fact (the same source SP4b uses), and (b) the colored
   rule subset is restricted to rules whose side conditions are **color-exact**
   (payload/arity/structure only). Analysis-gated conditions are excluded, so we
   never need color-aware NonNeg/Keys/Monotonic analyses to remain sound.

## 5. Components

### A. `GraphView` / `GraphSink` trait abstraction

Introduce two traits abstracting exactly the operations `build/codegen.rs`
emits:

- **Read (`GraphView`)**: range roots (`rel_class_ids`, and a sym-indexed lookup
  replacing `index.get(&Sym::X)`), enumerate a class's relational e-nodes
  (`rel_class_nodes`, returning **color-canonical** nodes for the colored impl),
  `arity`, escalar/`data` resolution for payload reads, and the **color-exact**
  side conditions used by the tagged subset.
- **Write (`GraphSink`)**: `add` (instantiate an RHS node), `intern_scalar`
  (intern a permuted scalar), `union` (equate the new node with the match root).

`build/codegen.rs` changes to emit trait-method calls against a generic graph
parameter `g` instead of the concrete `eg.`. Two implementations:

- **Black** = `(&EGraph, &Index)` (and `&mut EGraph` for the sink), delegating to
  today's inherent methods — *identical* behavior. This is the byte-identity
  guarantee.
- **Colored** = `(&mut ColoredEGraph, ColorId, &ColoredIndex)`: reads
  canonicalize through `find(c,·)`/`canon(c,·)`; writes go to `add_colored(c,·)`
  and the colored `union(c,·)`. A `ColoredIndex` is built per color from
  `visible_nodes(c)` grouped by `Sym` under `canon`.

**Trait-surface minimization.** Only the **tagged** subset's `find`/`apply` are
emitted generic over the graph parameter; untagged rules keep the concrete base
path unchanged. This bounds the colored impl to the operations the subset
actually uses. Side conditions that read analyses (`cond_non_negative`,
`cond_monotonic`, `cond_is_unique_key`, `cond_produces_key`) are **not** part of
the tagged subset, so the colored impl need not make them color-aware; if the
trait must carry them for uniformity, the colored impl supplies a conservative
(never-more-permissive) result, which is unreachable for the tagged subset.

**Rule tagging.** The rule DSL/grammar gains a marker (e.g. a `colored`
attribute) identifying rules eligible for colored saturation. The build-time
codegen records the tagged set (analogous to `AnalysisNeeds`) so the driver
knows which compiled rules to run per color. The initial tagged set is the
predicate-simplification rules plus the filter structural rules (removal /
merge / pushdown). Tagging a future rule is then sufficient to give it a
contextual home — no driver edit.

### B. Colored rule driver

A new driver (a colored analogue of `saturate`) that, per color, runs the tagged
subset over the colored view to a bounded per-color fixpoint:

1. build the `ColoredIndex` for color `c`;
2. **Phase 1 (read):** for each tagged rule, enumerate matches over the colored
   view;
3. **Phase 2 (apply):** instantiate each RHS via `add_colored(c,·)` and
   `union(c, new_id, root)`;
4. `close(c, ·)` to restore colored congruence;
5. iterate until no change or a bound is hit.

The same guard structure as `saturate` applies: a per-color e-node budget
(bounding colored-delta growth) and an iteration cap, both mirroring `saturate`'s
`MAX_ENODES` / `max_iters` so a contextual explosion is bounded and sound
(stopping early yields fewer colored conclusions, never wrong ones).

### C. Hierarchical color derivation

Reuse SP4b's `derive_facts` for per-class output-equivalence facts. Then:

- **Colors = the distinct equality-sets** across all class facts.
- **Forest by inclusion:** `parent(C)` is a maximal proper subset of `C` among
  the other colors; colors with no proper-subset color are children of black
  (∅). Each child color stores only its **delta** equalities (those not already
  provided by its parent chain); the rest are inherited via the layered `find`.
- **Tie-break.** A color may have several incomparable maximal proper subsets;
  pick the parent deterministically (most equalities, ties broken by a canonical
  ordering of the equality-set). Inheritance correctness is **independent** of
  which proper-subset parent is chosen — any proper subset gives sound
  inheritance, and `remove`-in-`close` prunes whatever the chosen parent chain
  redundantly covers. The tie-break only fixes a deterministic, stable forest.

This is well-defined on the e-graph DAG because each class maps to exactly one
output-fact, and the forest is built over the *set lattice of facts*, not the
structural graph — sharing/multiple structural parents do not create ambiguity.
Every color has exactly one parent, so the result is a forest (rooted at black).

`close_all` processes colors parent-first (ascending the forest), so a child
inherits its ancestors' induced merges. **`remove`-in-`close`:** when closing a
child color, an edge congruence derives that the parent (or any ancestor) already
provides is dropped from the child's delta via `union_find::remove`, keeping each
color's delta minimal (the SP3b `remove` use, which only has meaning under this
hierarchy).

### D. Incremental dirty-tracking congruence

`close` today re-scans all `visible_nodes(c)` each call; the driver (B) calls it
repeatedly. Add a dirty-set: after a batch of `add_colored`/`union`, only nodes
whose canonical children changed are re-examined, propagating to a fixpoint.
This is **behavior-identical** to full-pass `close`; the full-pass path is
retained under a debug/test assertion that the incremental and full-pass
fixpoints agree.

### E. Color-threaded relational extraction

SP4b's `extract_with` already threads colors for *scalar* payloads
(`resolve_scalar_colored`). SP4d extends extraction to *relational* conclusions:
`extract_colored` yields the lowest-cost node per colored class (base e-nodes +
colored delta nodes), and relational extraction walks from the root choosing, at
each class, the best node under `color(class)`. It reuses the existing
`cost::CostModel` (relational greedy/ILP) unchanged, so cost comparisons remain
consistent between base and colored conclusions. A color whose equalities are
unsatisfiable routes to the empty relation, reusing SP4b's empty-class handling.

## 6. Task decomposition (SDD, internal phases)

One spec, one plan; the plan's tasks follow these seams:

1. **P1 — trait refactor, byte-identical.** Introduce `GraphView`/`GraphSink`;
   re-emit the codegen against a generic graph parameter; implement both traits
   for the base layer. **Gate: zero behavior change** — full `mz-transform` +
   all EXPLAIN slt unchanged, clippy clean, before any colored path exists.
2. **P2 — flat-color colored driver.** Rule tagging in the DSL/codegen; the
   colored view impl; the colored rule driver (B) over flat colors (one per
   distinct fact, child of black); `add_colored`/`close`/`extract_colored` go
   live; relational color-threaded extraction (E). Capability demonstrable;
   differential captured alongside the existing path.
3. **P3 — hierarchical colors.** The inclusion forest (C); `close_all`
   parent-first; `remove`-in-`close`.
4. **P4 — incremental congruence (D).**
5. **P5 — extraction integration & goldens.** Switch the production extraction
   to the colored-saturation result; capture and justify the corpus differential;
   regenerate goldens per-file-standalone.

## 7. Testing

- **P1 byte-identity:** full `bin/cargo-test -p mz-transform` (unit +
  datadriven `*.spec`) and all EXPLAIN slt unchanged; `cargo clippy -p
  mz-transform --all-targets -- -D warnings`.
- **Capability:** integration tests on `CombinedLang` driving
  `close`/`close_all`/`add_colored`/`extract_colored`/`remove` through the
  production driver (today they are `ToyLang`-only). A targeted test asserting the
  **filter-placement case is recovered at the eqsat-output level**.
- **Soundness:** extend the SP3a/b all-pairs differential-oracle reasoning to the
  colored conclusions on `CombinedLang`; property tests that each colored
  conclusion is valid in its context (the conclusion holds under the color's
  equalities).
- **Incremental congruence:** the debug/test assertion that incremental and
  full-pass `close` reach the same fixpoint, plus seeded property tests.
- **Zero end-to-end regression:** build colored saturation **alongside** the
  current colored layer first, capture the corpus differential, justify each
  delta, then switch over and regenerate goldens **per-file-standalone**.

## 8. Behavior gate

- P1 lands with **zero** golden movement (pure refactor).
- After P5, any moved golden must be a justified consequence of a contextual
  rewrite now firing at saturation time; each is reviewed per-file-standalone via
  `bin/sqllogictest --optimized -- <file>`.
- Full `bin/cargo-test -p mz-transform` green; `cargo clippy -p mz-transform
  --all-targets -- -D warnings` clean at every task.

## 9. Risks & deferrals

- **Risk — codegen generalization (biggest).** Abstracting the matcher surface
  over a trait touches `build/codegen.rs` and the rule machinery. Mitigation:
  land it as a pure byte-identical refactor (P1) before any colored impl, so the
  riskiest change is verified in isolation.
- **Risk — color-tree correctness on shared subexpressions.** Mitigated by the
  fact-lattice framing (§5.C): the forest is over distinct facts, not the
  structural DAG.
- **Risk — perf.** Colored saturation runs the tagged subset across colors.
  Bounded by: small tagged subset, per-color fixpoint + e-node caps, and
  incremental congruence (D).
- **Risk — extraction integration.** Threading colors through relational
  extraction extends SP4b's scalar-only path; mitigated by reusing the existing
  `cost::CostModel` and landing extraction as its own task (P5) with the
  differential gate.
- **Deferral — color-aware analyses & analysis-gated colored rules.** The tagged
  subset is restricted to color-exact conditions; admitting analysis-gated rules
  requires color-aware NonNeg/Keys/Monotonic, deferred to future work.
- **Deferral — color-aware key/arrangement cost.** Out of scope; the targeted
  subset does not rewrite Join/Reduce key payloads, so arrangement accounting
  stays as in SP4b.
- **Known semantic note.** The headline payoff (saturation-time filter pushdown)
  is normalized downstream and so is invisible end-to-end; this cycle is
  justified as foundational capability, verified at the eqsat-output level (§1).
