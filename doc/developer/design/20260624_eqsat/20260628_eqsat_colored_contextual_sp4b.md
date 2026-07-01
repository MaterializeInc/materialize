# SP4b — Colored Contextual Equalities

> Part of the eqsat relational+scalar unification effort. Predecessors:
> SP1 (generic core), SP2a (scalar instance), SP3a (colored spike), SP3b (full
> colored mechanism), SP4a (multi-sort fusion). This sub-project consumes the
> SP4a combined graph and the SP3b colored mechanism (specifically its colored
> union-find / color-tree layer).
>
> Status: design (revised after an adversarial viability review — see §10). One
> monolithic cycle (spec → plan → SDD). Full replacement, no feature flag.

## 1. Goal & scope

Replace the eqsat **Phase-2a payload rewriting** (the imperative
equivalence-substitution loop inside `EGraph::saturate`) and re-home the eqsat
`Equivalences` analysis onto the SP3b **colored** mechanism, applied to the SP4a
combined graph (`EGraph<CombinedLang>`, `CNode = Rel(ENode) | Scalar(SNode)`).

**Mechanism (the refit).** Contextual scalar equalities — `#0 = #1` guaranteed
under a Filter, join-key equalities, Reduce key equalities, Map output-column
definitions — are recorded as **per-scope colored unions** over scalar `Id`s. The
*rewritten spelling* of a payload is produced exactly as Phase-2a produces it
today (`reduce_escalar` with its `max_col` guard), but instead of being unioned
into the base as a new relational alternative, the rewritten scalar term is
interned as an ordinary base term and the equality `original ≅ reduced` is
recorded in that scope's **color**. Color-threaded extraction then resolves each
Filter/Map payload under its scope's color, reading the `escalar` of the cheapest
member of the payload's colored class.

**Why colored (vs Phase-2a's relational-alternative trick).** Phase-2a is sound
but coarse: it duplicates whole relational e-nodes (Filter/Map with substituted
payloads) into the base. The colored mechanism confines the *equality* to the
scope where it holds and reasons at the scalar level — no relational-node
duplication, and it is the substrate on which deeper colored congruence (SP4c)
builds.

### Key enabling fact

SP4a hash-conses column references globally, so `Column(0)` is a *single* scalar
e-class shared across all relations. Recording `Column(0) ≅ Column(1)` is globally
unsound but locally correct for the scope that guarantees it — which is exactly
what a color confines.

### In scope (deleted / replaced)

- The Phase-2a block inside `EGraph::saturate` (`rewrite_escalars`,
  `reduce_escalar` as a *saturation* step, `filter_input_reducer`, the
  `Analyses.eq` field) — egraph.rs ~1674–1735, ~2114–2265.
- The eqsat `Equivalences` *analysis wrapper* (`eqsat/analysis/equivalences.rs`)
  as an `Analysis` impl. **Its per-operator derivation logic is preserved** —
  moved into `colored_derive` (§3). `reduce_escalar`'s rewrite logic is also
  **preserved** and reused by derivation (§3).
- `collapse_unsatisfiable` rule + `cond_unsatisfiable` + the `Cond::Unsatisfiable`
  machinery (dsl/grammar/codegen), re-expressed as derivation-time
  unsatisfiability + extraction-time empty-folding (§4, §5).

### Out of scope (untouched)

- The standalone `src/transform/src/equivalence_propagation.rs` transform and
  `src/transform/src/analysis/equivalences.rs` — a separate classic-pipeline
  subsystem. **`EquivalenceClasses` and `ExpressionReducer` (defined there) stay**
  and are reused by `colored_derive`.
- `empty_false_filter` — it folds a literal-`false` predicate (`any_false`) and is
  **independent of the equivalence analysis**; it stays.
- The relational join-order cost model (`cost::CostModel`) and the greedy/ILP
  relational extractors — reused as-is, made color-aware only at the Filter/Map
  scalar-payload resolution seam (§5).
- **Application is restricted to Filter predicates and Map scalars** — Phase-2a's
  actual reach. The derivation lattice still propagates Join/Reduce/etc.
  equivalences *upward* so they are available at Filters/Maps above; we simply do
  not rewrite Join/Reduce/ArrangeBy key payloads (Phase-2a never did, and doing so
  would force `cost::CostModel`'s arrangement accounting and `arrangements_of` to
  become color-aware — an SP4c concern).
- SP3b's colored **congruence closure** (`close`/`close_all`), **conclusions**
  (`add_colored`), and **`extract_colored`** are *not* used by SP4b (the reducer
  pre-computes substituted spellings, so per-payload congruence is unnecessary).
  SP4b uses SP3b's colored **union-find + color tree + layered `find`** plus a new
  member-enumeration accessor (§7). The unused pieces are the SP4c evolution path.

### Gate

Not byte-identical (full replacement changes plans). The gate is:

1. **Differential capture** (temporary coexistence): the colored path is built and
   wired *alongside* the still-present Phase-2a path, so a corpus pass can compare
   colored-driven output vs Phase-2a-driven output and enumerate each plan delta.
   The Phase-2a deletions (§6) land **after** the differential is captured and the
   deltas justified. The plan must sequence build → differential → delete.
2. **Review**: regenerate affected slt goldens; review each diff as an improvement
   or a sound-neutral change. Per-file-standalone evaluation (the SP4a list-mode
   caveat).
3. **Green**: `bin/cargo-test -p mz-transform` and `cargo clippy -p mz-transform
   --all-targets -- -D warnings`.

## 2. Architecture / phase ordering

The SP3b colored layer borrows the base immutably (`ColoredEGraph<'b, L>` holds
`&'b EGraph<L>`). That forbids interning into a frozen base, so **all interning of
rewritten spellings happens before the freeze**, on the mutable graph:

1. **Context-free saturation** of the base, exactly as today **minus Phase-2a**.
   Only globally-sound rules run.
2. **Index-filter seeding** — `seed_indexed_filters` mutates the graph *after*
   saturate (engine.rs ~210/265); derivation must observe the seeded graph.
3. **Derivation (mutable graph)** (§3): compute per-relational-class
   output-equivalence facts via the existing lattice (merge across all nodes in a
   class, bounded fixpoint — the logic from the deleted analysis); for each
   Filter predicate / Map scalar, compute its reduced spelling via the reused
   `reduce_escalar` (preserving its `max_col` guard), **intern** the reduced term
   into the base, and record the contextual equality `original_id ≅ reduced_id`
   plus a `color_of[rel class]`. Detect unsatisfiable scopes via the existing
   `EquivalenceClasses::unsatisfiable()` on the merged fact.
4. **Freeze** the base (`rebuild()`). Interned reduced terms are now hash-consed
   and stable.
5. **Build the colored layer** (§4): `ColoredEGraph::new(&base)`; create one color
   per distinct equality-set; apply the recorded unions (re-canonicalizing ids
   through `base.find` after rebuild). No congruence closure needed.
6. **Color-threaded extraction** (§5): the existing relational greedy/ILP extractor
   + `cost::CostModel` is reused unchanged for relational structure/join-order/
   arrangement cost; the only change is that Filter/Map scalar payloads are
   resolved under `color(input-scope)`, reading the `escalar` of the cheapest
   colored-class member; a class whose derived fact is empty (`None`) folds to an
   empty `Constant`.
7. **Raise** — unchanged.

### Known semantic change (flagged)

Contextual equalities now act at *extraction* time, not inside the saturation rule
loop. Phase-2a re-interned and unioned contextually-rewritten *relational* nodes
mid-saturation, so context-free rules could then match on them; that no longer
happens. This is the principal source of plan deltas and is covered by the
regenerate-and-review gate. Re-running rules over colored conclusions (colored
saturation) is an explicit SP4c deferral.

## 3. Derivation — `eqsat/colored_derive.rs` (new), runs on the mutable graph

Replaces the eqsat `Equivalences` `Analysis` impl; reuses its derivation logic and
`reduce_escalar`.

**Per-class output-equivalence fact.** Compute, per relational e-class, an
`EquivalenceClasses` describing that class's **output** column-equivalences. This
is *not* "one node's fact": it folds **every** e-node in the class through the
per-operator `make` and combines with the lattice `merge` (`extend` +
`minimize_bounded(None, 100)`), to a bounded fixpoint — exactly what
`run_analysis_bounded` does today (core.rs) with `MAX_EQUIVALENCES_ANALYSIS_ITERS`.
`None` (empty relation) is the absorbing top and propagates upward through the
lattice (Filter/Join/Union arms), which §5 relies on for empty-folding. The
per-operator arms are unchanged from the deleted analysis:

| Node | Output equivalences |
|---|---|
| `Constant`, `Get`, `Opaque`, `LocalGet` | empty (`Some(default)`); `LocalGet` reads recursion locals |
| `Project { outputs }` | input, `project(outputs)` |
| `Map { scalars }` | input + `[col(input_arity+pos), scalar_pos]` per new column |
| `Filter`/`IndexedFilter { predicates }` | input + `[pred_0, …, true]` |
| `FlatMap` | input (opaque new columns) |
| `Join`/`WcoJoin` | per-input equivalences offset-shifted + the join's own classes; `None` if any input empty |
| `Reduce { group_key, aggregates }` | key-column equivalences, `minimize`, project to keys, plus pass-through aggregates |
| `Negate`, `Threshold`, `TopK`, `ArrangeBy`, `ArrangeByMany` | input unchanged |
| `Union { inputs }` | intersection (`union_many`) of non-empty branches; all-empty → `None` |

**Reduced-spelling interning (the term-creation step).** For each Filter
predicate and Map scalar, apply the reused `reduce_escalar` with the appropriate
reducer and `max_col` guard:
- Filter predicate: the **input's** reducer (`color(filter.input)` equivalences),
  `max_col = input_arity` — reproducing today's `filter_input_reducer`.
- Map scalar at position `pos`: the Map class's reducer, `max_col = input_arity +
  pos` — reproducing today's forward-reference guard.

If `reduce_escalar` produces a changed expression, intern it (`intern_scalar` →
`lower_into`, a base mutation) to obtain `reduced_id`, and record
`(color, original_id, reduced_id)`. Interning a *term* is globally sound; only the
*equality* is colored.

**Colors.** Lower each class's relevant equalities to scalar-`Id` pairs; the set of
recorded unions for a scope defines its color. Distinct equality-sets dedup into a
`HashMap<EquivSetKey, ColorId>`; `color_of: HashMap<Id /*rel class*/, ColorId>`
maps each relational class with non-trivial equivalences to its color (black/none
otherwise). All colors are children of black for SP4b (flat; hierarchy is SP4c).

**Unsatisfiability.** A class whose merged fact is `Some(ec)` with
`ec.unsatisfiable()` (the existing literal-contradiction check, after `minimize`)
— or whose fact is `None` — is recorded in `empty_classes: HashSet<Id>`. Because
`None` propagates upward through the lattice, parents of a contradictory class are
also marked, reproducing the saturation-time Empty *cascade* at derivation time.

**Output.** `colored_derive` returns the recorded unions per color, `color_of`,
and `empty_classes` — all in terms of pre-freeze ids (re-canonicalized after
freeze via `base.find`).

## 4. Colored layer construction

After freeze: `ColoredEGraph::new(&base)`; for each `ColorId`, `new_color(None)`
and apply its recorded unions via `union(color, a, b)` (re-canonicalizing `a`,`b`
through `base.find`). **No `close`/congruence** — the reducer already produced the
substituted spellings, so the payload's colored class directly contains
`{original, reduced}` and union-find `find` gives transitive closure of the
recorded equalities (e.g. `#0≅5` and `#0≅6` ⇒ `5≅6`, already detected as
unsatisfiable in §3).

A new **member-enumeration accessor** is added to the colored layer: given a
`ColorId` and an `Id`, return the base-class members of that colored class
(implemented by grouping base scalar classes by layered `find`, or a reverse
index). §5 uses it to pick the cheapest spelling.

## 5. Color-threaded extraction

The relational extractor (greedy + ILP) and `cost::CostModel` are reused
**unchanged** for relational structure, join-order and arrangement cost. Two
seams:

**(a) Filter/Map payload resolution.** Where extraction today resolves a Filter
predicate or Map scalar via `EGraph::resolve_scalars` (a plain `escalar` lookup,
egraph.rs:1233), it now resolves under the node's scope color:
- Determine `color = color_of[input class]` (Filter predicates and Map scalars are
  both evaluated over the node's input; the input `Id` is in hand at build time, so
  no top-down color threading is needed). Black/none ⇒ identical to today.
- Among the payload's colored-class members (§4 accessor), pick the cheapest by a
  small **scalar tree-size cost** over each member's `escalar(member).expr` (reuse
  the scalar-cost notion from `scalar/raise.rs` `node_cost`; no `CostModel<L>`
  trait needed). Read that member's `escalar` to emit the `EScalar`.
- Every candidate is a **base** class with an `escalar` entry (the reduced term was
  interned pre-freeze), so reconstruction is a cache read — **no color-aware
  scalar raise, no `add_colored`.** Memoize per `(ColorId, payload Id)`.

Only Filter predicates and Map scalars use this path; Join/Reduce/ArrangeBy key
payloads resolve via the unchanged `escalar` cache, so ILP arrangement accounting
(`arrangements_of`, `is_oracle_covered`) is unaffected.

**(b) Empty-folding.** If the class being extracted is in `empty_classes`, emit an
empty `Constant` of the class's arity, replacing the deleted `collapse_unsatisfiable`
rule. The §3 upward `None`-propagation means parents fold too.

**Threading.** The colored layer (`ColoredEGraph` borrowing the frozen base),
`color_of`, the member accessor, and `empty_classes` are built once (steps 3–5)
and passed into extraction. This changes the `Extractor` trait signature
(extract.rs:27) and **both** impls (`GreedyExtractor`, `IlpExtractor`), and the
**five** extraction call sites in engine.rs (~217, 225 [the direct non-trait
`extract_with` for `TimeFirst`], 271, 414, 466) — all must share one derived
colored layer. The greedy path stays internally consistent (it costs the
already-color-resolved `Rel`).

## 6. Deletions & replacements (precise)

- Delete the Phase-2a block in `EGraph::saturate` and its helpers used only there
  (`rewrite_escalars`, the saturation-time `reduce_escalar` call sites,
  `filter_input_reducer`, `Analyses.eq`). Keep `reduce_escalar`'s rewrite routine
  (reused by §3).
- Delete `eqsat/analysis/equivalences.rs` as an `Analysis` impl and its
  registration in `analysis.rs` (mod + `pub use Equivalences`). Move its
  derivation arms into `colored_derive`.
- Delete `collapse_unsatisfiable` (the rule wired to unsatisfiability —
  `relational.rewrite` ~287–291) and `cond_unsatisfiable` (egraph.rs ~1397), plus
  the now-unused `Cond::Unsatisfiable` machinery: `dsl.rs` ~280–285,
  `build/grammar.rs` ~548, `build/codegen.rs` ~330 and ~1055. **Keep
  `empty_false_filter`** (independent).
- Keep `EquivalenceClasses` / `ExpressionReducer` (shared types).
- `AnalysisNeeds` (rules.rs ~38–45) has no `eq` field — no `needed_analyses`
  change.

## 7. Components / files (isolation boundaries)

- **`eqsat/colored_derive.rs`** (new) — pre-freeze derivation: per-class merged
  equivalence facts (reused arms), reduced-spelling interning (reused
  `reduce_escalar`), `color_of`, recorded unions, `empty_classes`. Unit-testable
  against a built graph.
- **`eqsat/colored.rs` + `colored/`** (SP3b) — remove `#![allow(dead_code)]`; add
  the member-enumeration accessor (§4). The congruence/conclusions/extract modules
  stay present but unused by SP4b (SP4c consumers).
- **`eqsat/extract.rs`** + the resolution seam in `egraph.rs` — color-aware
  Filter/Map resolution (the cheapest-member pick + scalar tree-size cost);
  empty-folding; `Extractor` trait + both impls threaded with the colored layer.
- **`eqsat.rs` `optimize_inner`** — insert seed → derive → freeze → build-colors →
  color-aware-extract between saturate and raise.
- **`eqsat/engine.rs`** — `Optimizer::optimize` builds the colored layer after
  seeding/freeze and threads it into all extraction call sites (incl. the
  `TimeFirst` direct call).

### File decomposition (egraph.rs)

`egraph.rs` is ~3000 lines / 134KB and SP4b modifies it heavily. Following the
SP4a precedent (`analysis.rs` → `analysis/`), an **early pure-movement task**
splits it by responsibility into an `egraph/` module folder, public surface
preserved and call sites untouched (the original becomes a re-exporting root).
Proposed boundaries (final split decided in the plan):

- `egraph/node.rs` — `ENode`, `Sym`, `impl ENode` (relational node language,
  `relational_children`/`scalar_children`/`map_children`).
- `egraph/combined.rs` — `CNode`, `CSym`, `RelGraphData`, `CombinedData`
  (`escalar` cache + accessors), `CombinedLang`.
- `egraph/build.rs` — `add_rel`, `intern_scalar`/`intern_scalars`,
  `resolve_scalars`.
- `egraph/saturate.rs` — the saturation loop, `Analyses`, rule conditions.
- `egraph.rs` (root) — re-exports + remaining glue.

Do the split **before** the behavioral changes. If it entangles behavior unsafely,
fall back to splitting only the modules SP4b touches.

## 8. Testing & gate

- **Unit — derivation:** per-operator arm (Filter/Join/Reduce/Map/Project/Union/
  FlatMap/pass-through) and the per-class *merge across multiple nodes* fixpoint —
  re-pointed from the deleted analysis's tests. Reduced-spelling interning
  reproduces `reduce_escalar`'s output incl. the `max_col` guard (Filter and Map
  forward-reference cases).
- **Unit — colored layer:** recorded unions give the right `find` partitions;
  member enumeration; transitive literal contradiction ⇒ `empty_classes`.
- **Unit — color-aware resolution:** a Filter/Map payload with a cheaper congruent
  spelling under its scope is selected; black-scope resolution is identical to the
  `escalar`-cache path; empty-fold for an unsatisfiable/`None` class.
- **Differential capture:** colored path wired alongside Phase-2a; corpus pass
  enumerating every plan delta, captured **before** the §6 deletions.
- **Behavior gate:** full `bin/cargo-test -p mz-transform`; per-file-standalone
  slt; regenerate affected goldens and review every diff; clippy `-D warnings`.

## 9. Risks & SP4c deferrals

- **Risk — extraction-time-only contextual reasoning** (§2): contextual equalities
  no longer feed the saturation rule loop → plan deltas. Mitigated by
  regenerate+review. *Deferral:* colored saturation.
- **Risk — base-term interning pressure:** reduced spellings are interned into the
  base; bounded by plan size and the existing `MAX_ENODES = 600` ceiling, but
  validate on the corpus that derivation does not push the graph past the cap.
- **Risk — lost saturation-time restructuring for emptiness:** §3/§5 reproduce the
  empty *cascade* (via `None` propagation) but not any further rule restructuring
  the saturation-time `Empty` node enabled. Plan-equivalent (empty subrelations);
  flag as a reviewed delta.
- **Deferral (the "pure colored" evolution):** colored congruence closure
  (`close`/`close_all`), `add_colored` conclusion materialization, `extract_colored`,
  hierarchical color sharing, incremental congruence, UF-edge remove-in-close, real
  joint cost, and color-aware key/arrangement reasoning.
- **Carry-over:** SP4a M1.1 (`arity()` panics on a scalar class), M1.2 (inert
  scalar-class iteration in extract/analysis).

## 10. Viability review (2026-06-28)

Two adversarial opus reviewers checked the original spec against the code and
found two showstoppers — (1) colored congruence cannot *create* rewritten terms
while Phase-2a's core job is creating them, and (2) a freeze/intern ordering
contradiction — plus several gaps (per-class fact must merge across all nodes;
scope over-reach into Join/Reduce keys breaking ILP arrangement accounting; the
deletion list named `empty_false_filter` instead of `collapse_unsatisfiable` and
missed the `Cond::Unsatisfiable` dsl/grammar/codegen sites and the full
`Extractor`-trait threading; `EScalar::lit` being bool-only; freeze needing to
follow `seed_indexed_filters`; per-`(ColorId,Id)` extraction blow-up). This
revision adopts **pre-freeze interning of `reduce_escalar` spellings + colored
equalities** (term creation by the proven reducer; colors confine only the
equality), restricts application to **Filter predicates + Map scalars** (Phase-2a's
real reach), and folds in every specific fix above. The refit also *removes* the
need for color-aware scalar raise, `add_colored`, and colored congruence closure
from SP4b (they become the SP4c evolution path).
