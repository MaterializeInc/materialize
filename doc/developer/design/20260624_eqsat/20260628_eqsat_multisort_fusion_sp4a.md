# SP4a — Multi-sort e-graph fusion (context-free)

> Part of the relational+scalar eqsat unification effort. Predecessors:
> SP1 (generic core), SP2a (scalar instance), SP3a/SP3b (colored mechanism).
> SP4a is the first of three SP4 sub-projects:
> **SP4a — multi-sort fusion (context-free)** → SP4b (colored contextual
> equalities, subsuming `Equivalences`) → SP4c (production hardening / SP3b
> deferrals).

## 1. Scope & goal

Make scalar expressions **first-class e-classes inside the relational
e-graph**, replacing the opaque `EScalar`/`MirScalarExpr` payloads that
`ENode` carries today with scalar-class `Id` references. After SP4a there is
**one** `core::EGraph` holding both sorts, relational nodes reference scalar
classes as children, and the core canonicalizes those references — the
structural prerequisite for SP4b's colored contextual equalities.

SP4a is **behavior-neutral structural fusion**:

- Scalars become e-classes, but scalar e-classes are **inert**: no scalar
  rewrite rules, no scalar unions, no colors. Each scalar class therefore
  holds exactly one interned expr.
- Lower interns the *same* `reduce`-canonical exprs used today; raise
  reconstructs identical MIR. `raise(lower(p)) == p`.
- The only observable internal change is scalar **CSE** (identical scalars
  share a class). This does not alter **plan structure, arity, join order, or
  scalar expressions**.

> **Correction (2026-06-28, discovered in Task 3/4):** an earlier draft of this
> section claimed scalar CSE "cannot alter output plans" at all. That is
> inaccurate for one **cosmetic** dimension: `MirScalarExpr::Column` carries a
> `TreatAsEqual` display *name*, which fusion's global hash-consing of column
> references collapses to one (deterministic but arbitrary) name per shared
> scalar class. This changes only the humanized `{name}` annotations in EXPLAIN
> (verified name-only: stripping `{…}` makes pre/post output equal). The cheap
> remedy (strip names → rely on the `column_names` analysis) was empirically
> refuted — the carried per-use names are authoritative and richer than the
> analysis can reproduce (join-implementation keys, physical-LIR keys, CTE/
> derived columns). The byte-identical remedy (per-occurrence name preservation)
> would re-touch the flip; the human chose instead to **accept the cosmetic
> drift and regenerate the affected EXPLAIN goldens** (see §8).

The acceptance gate (see §8): with `enable_eqsat_optimizer` at its default
(on), the optimized/physical **plans are semantically identical** to pre-SP4a —
byte-identical except the regenerated cosmetic EXPLAIN column-name annotations
on fused scalar classes.

### Out of scope (later sub-projects)

- Colors / contextual scalar equalities / subsuming the `Equivalences`
  analysis + Phase-2a payload rewriting — **SP4b**.
- Running the scalar rewrite rules inside the relational graph (an actual
  optimization, not behavior-neutral) — SP2b / later.
- SP3b deferrals (incremental dirty-tracking congruence, UF-edge
  remove-in-close, rule-driven colored conclusions, real joint cost tuning)
  — **SP4c**.

### Cycle structure

One SP4a cycle. A separate substrate spike is **not** warranted: the
combined-`Language` substrate is thin (no `core` changes), so the risk lives
entirely in the *port* (analyses, matcher, cost, extraction), which a
substrate spike would not de-risk. The cycle is internally staged into four
testable milestones (see §8):

1. `CombinedLang` + substrate + lower/raise/intern → identity round-trip.
2. Cached scalar-fact map + analyses ported (+ `analysis.rs` split).
3. Relational rules/matcher ported to the combined graph.
4. Joint cost/extraction → full goldens/slt-unchanged gate.

## 2. Architecture

One e-graph, one combined `Language` on the existing generic core:

```rust
enum CNode { Rel(ENode), Scalar(SNode) }   // Language::Node
enum CSym  { Rel(Sym),  Scalar(ScalarSym) } // Language::Sym
struct CombinedLang;                         // Language::GraphData = CombinedData
// the combined e-graph is core::EGraph<CombinedLang>
```

No changes to `core::EGraph` / `Language` / `Analysis`. The combined graph
shares a single `Id` space across both sorts; a relational e-class and a
scalar e-class are both just `Id`s, and an `ENode` referencing a scalar class
holds that scalar's `Id`.

The relational engine's runtime entry points (`EqSatTransform`,
`PhysicalEqSatTransform`, the saturation driver) operate on
`EGraph<CombinedLang>` instead of `EGraph<RelLang>`. `RelLang` /
`EGraph<RelLang>` are removed (or retained only as a transitional alias if
that reduces churn — see §7).

## 3. Data structures

### 3.1 `ENode`: scalar payloads become `Id`s

Every scalar payload field in `ENode` changes type from `EScalar` to a scalar
`Id` reference:

| Field | Before | After |
|---|---|---|
| `Map.scalars` | `Vec<EScalar>` | `Vec<Id>` |
| `Filter.predicates` | `Vec<EScalar>` | `Vec<Id>` |
| `FlatMap.exprs` | `Vec<EScalar>` | `Vec<Id>` |
| `Reduce.group_key` | `Vec<EScalar>` | `Vec<Id>` |
| `ArrangeBy.key` | `Vec<EScalar>` | `Vec<Id>` |
| `IndexedFilter.predicates` | `Vec<EScalar>` | `Vec<Id>` |
| `Join.equivalences` / `WcoJoin.equivalences` | `Vec<Vec<EScalar>>` | `Vec<Vec<Id>>` |
| `ArrangeByMany.keys` | `Vec<Vec<EScalar>>` | `Vec<Vec<Id>>` |

`Aggregate` exprs in `Reduce.aggregates` and other non-`EScalar`
`MirScalarExpr`-bearing payloads (`TopKShape.limit`, `Opaque`, `LocalGet.get`,
`IndexedFilter.committed`) remain opaque payloads in SP4a — they are not part
of the `EScalar` fact surface the rules read, so converting them buys nothing
and is deferred. (`AggregateExpr.expr` may be revisited in SP4b if colors need
it.)

`ENode` gains `scalar_children() -> Vec<Id>` flattening all scalar-ref fields
in a fixed, documented order. The existing `children()` (relational inputs)
is renamed `relational_children()`.

### 3.2 `CombinedLang` trait impl

- `children(Rel(e))` = `e.relational_children()` ++ `e.scalar_children()`;
  `children(Scalar(s))` = `s.children()`.
- `map_children` remaps relational inputs **and** scalar `Id`s on the `Rel`
  arm, and child `Id`s on the `Scalar` arm. This is what makes scalar refs
  genuine e-class references that the core canonicalizes (and that SP4b's
  colored `find` will recanonicalize when scalar classes merge under a color).
- `symbol(Rel(e))` = `CSym::Rel(e.sym())`; `symbol(Scalar(s))` =
  `CSym::Scalar(<scalar sym>)`.
- `on_add` / `on_union` dispatch by sort (see §3.3).

Including scalar `Id`s in the generic `children`/`map_children` does **not**
perturb the matcher's positional *child* model: the matcher's structural
operand binding still uses `ENode`'s typed fields, not the generic
`children()`. The matcher's *payload* layer does change, however — its
`Binding` scalar variants hold `Id`s and its helpers resolve/intern through the
cache (see §5.1). (Earlier drafts said the matcher layer was wholly unaffected;
that was inaccurate — only the operand-position model is unaffected.)

### 3.3 `CombinedData` (the `GraphData`)

```rust
struct CombinedData {
    rel: RelGraphData,             // availability oracle, unchanged
    scalar: ScalarGraphData,       // col_types + per-class scalar analysis, unchanged
    escalar: HashMap<Id, EScalar>, // NEW: cached EScalar fact per scalar class
}
```

`on_add` dispatches by the added node's sort: a `Scalar` add populates the
scalar analysis (as today) **and** writes `escalar[id]` from the interned
expr; a `Rel` add updates `rel` as today. `on_union` likewise. In SP4a scalar
classes never merge, so `escalar` is write-once-per-class and static.

Relational code reads scalar facts through a `data.escalar(id) -> &EScalar`
accessor. Code that constructs a new scalar (a rule applier, `permute_cols`)
interns the expr (obtaining an `Id`) and registers its `EScalar`.

## 4. Lower / raise / intern

### 4.1 Lower (relational)

At each point relational lower would store an `EScalar`, it instead:

1. interns the **same reduced** `MirScalarExpr` into the scalar sub-graph,
   reusing the existing scalar `lower` (§4.3), obtaining a scalar `Id`;
2. registers the `EScalar` (`{ expr, lit }`, computed exactly as today) in the
   `escalar` cache for that `Id`;
3. stores the `Id` in the `ENode` field.

Scalar content (`reduce` form, `lit`) is unchanged; only its addressing
changes.

### 4.2 Raise (relational)

Reconstruct `Rel` as today; for each scalar `Id` field, **read the cached
`EScalar`** (`escalar[id].expr`) — the authoritative verbatim expr raise
returns today. Each scalar class holds exactly one expr, so the cache value is
the extracted scalar; `raise(lower(p)) == p` holds by construction.

*SP4b-forward:* SP4b replaces this cache read with color-aware scalar
extraction from the class; the relational raise call sites are unchanged.

### 4.3 Scalar lower/raise reuse

The existing `scalar/lower.rs` decomposes `MirScalarExpr` into `SNode`s. Its
intern target is generalized so it writes `CNode::Scalar(snode)` into the
combined `EGraph<CombinedLang>` (smallest change: parameterize the interning
step; the decomposition logic is untouched). `scalar/node.rs`,
`scalar/raise.rs` are reused as-is (raise reads a class's single `SNode`).

## 5. Rules / matcher / analysis port

### 5.1 `matcher.rs` (the largest single-file change)

`Binding`'s scalar variants (`Predicates`, `Scalars`, `Equivalences`,
`GroupKey`, `FlatMapExprs`) hold `Vec<Id>` / `Vec<Vec<Id>>` instead of
`Vec<EScalar>`. Its helpers go through the cache:

- `cols(id)`, `is_col(id)` → `data.escalar(id).cols()` / `.is_col()`.
- `permute_cols` → resolve `Id`→`EScalar`, permute as today, **intern the
  permuted expr** → new `Id` + registered fact. Column-shifting pushdown rules
  (through `Project`/`Map`/`Join`) thus mint new scalar classes.
- column-bound filters (`cols < b`, `cols >= b`) and the `into_*` constructors
  resolve via the cache.

The **DSL rules in `rules/relational.rewrite` are unchanged** — they sit on
these binding primitives. Binding operations that produce scalars take the
graph (or an intern closure) so they can register new classes.

**Codegen `let`-hoisting (required).** The build-time codegen
(`build*/codegen.rs`) currently emits payload helpers as *nested* expressions
(e.g. `remap_payload(eg, e, &(… shift_payload(eg, …) …))`). Once those helpers
take `&mut EGraph` to intern, the nested form fails the borrow checker (and
two-phase borrows do not rescue the nested free-fn form). Codegen's
`pexpr_expr` (and `cond_expr` where it threads scalars) is therefore
restructured to **hoist every sub-payload into sequential `let` bindings**, so
the emitted code is straight-line `let a = shift_payload(eg, …); let b =
remap_payload(eg, e, &a); …`. The scalar-reading `cond_*` helpers
(`cond_all_columns`, `any_false`, `no_false`, `no_error`, `all_true`,
`produces_key`, `has_inner_equiv`, `uses_only_input`, `cols_in_range`, etc.)
are re-typed to take `&mut EGraph` / `&CombinedData` and resolve via the cache.
This keeps every rule's column-manipulation *logic* byte-identical — only the
access/intern path and the generated statement structure change.

*Decision note (2026-06-28):* an Option-B alternative kept `Binding` on
`EScalar` and changed only the two codegen boundaries (resolve at match,
intern at construction), deferring the matcher `Id`-domain port to SP4b. The
human chose this Option A (matcher in the `Id` domain now) so SP4b inherits a
matcher already operating on scalar classes.

### 5.2 Analyses

Each `impl Analysis<RelLang>` becomes `impl Analysis<CombinedLang>`:

- `make` matches `CNode::Rel(e) => <today's logic>`,
  `CNode::Scalar(_) => self.bottom()` (relational facts — arity, keys,
  non-negativity, monotonicity, equivalences — are meaningless on scalar
  classes).
- Analyses that read scalar columns (`Keys` via `group_key`, `Equivalences`
  via join equivalences) resolve `Id`→facts through a **scalar-fact accessor
  passed in `Ctx<'a>`** (the `Analysis::Ctx` GAT exists for exactly this —
  "scalar later: column types").
- `Equivalences` stays semantically as-is, just reading via the cache; SP4b
  subsumes it.

### 5.3 `analysis.rs` split

Split the 64KB `analysis.rs` into per-analysis modules under `analysis/`
(`nonneg.rs`, `monotonic.rs`, `keys.rs`, `equivalences.rs`, and the remaining
analyses), with a thin `analysis.rs` root re-exporting the public surface.
Pure code-movement, performed in the same stage that re-types the analyses so
each analysis is one coherent reviewable change. (Per repo convention there
are no `mod.rs` files — the module root is `analysis.rs` beside the
`analysis/` dir, mirroring `colored.rs`/`colored/`.)

### 5.4 Engine / transform

`engine.rs`, `transform.rs`, and the matcher driver re-type to
`EGraph<CombinedLang>`. Matching uses a **relational view** — iterate
Rel-sort classes, unwrap `CNode::Rel(e)`, feed the existing `Sym`-bucketed
matcher. No scalar rules are registered, so scalar classes are inert during
saturation.

## 6. Cost & extraction

### 6.1 Selection (SP4a)

Extraction picks the best relational node per class and recurses through
**relational** children exactly as today. Scalar children are resolved via the
`escalar` cache — each scalar class holds one expr, so scalar "extraction" is
a cache lookup, no recursion or scalar objective.

*SP4b-forward:* genuine color-aware scalar extraction and a joint
relational+scalar cost objective land in SP4b; the extraction entry points are
unchanged.

### 6.2 Cost

Relational cost is computed over the extracted relational tree as today;
wherever it reads scalar content it reads through the cache → identical
numbers. Scalar e-classes are **not** counted as relational nodes
(`node_count` / cost iterate the relational extraction tree, not raw combined
classes), so there is no double-counting.

### 6.3 Critical behavior-neutrality safeguard — keying

`ENode`'s derived `Ord` now compares scalar `Id`s (intern order) rather than
`EScalar` content. Extraction relies on that `Ord` for deterministic
tie-breaks among cost-equal plans, and `cse.rs` keys shared subtrees
structurally. If those keys silently switch from expr-content to raw `Id`,
extraction could pick a different (cost-equal) plan and CSE could group
differently → goldens change.

**Hard requirement:** every extraction / cost / CSE comparison or hash that
today keys on `EScalar` content must resolve `Id`→`EScalar` through the cache,
so the keys are byte-identical to today's. Tie-break order and CSE grouping
are then provably unchanged. This is the single most important correctness
invariant for the gate; §8 targets it with a dedicated test.

## 7. Conventions & migration notes

- Repo lints: no `mod.rs` (use `analysis.rs` + `analysis/`); `clippy
  --all-targets -D warnings` is CI. Tests use `#[mz_ore::test]`.
- `RelLang` removal vs. transitional alias: prefer removing `RelLang` and
  retyping call sites to `CombinedLang`. If the churn is large enough to
  threaten reviewability, a transitional `pub type RelLang = CombinedLang` is
  acceptable *within* SP4a but must be gone by the end of the cycle (no
  permanent dual naming).
- No new feature flag: SP4a is an internal refactor under the existing
  `enable_eqsat_optimizer`; the combined graph *is* the relational graph when
  eqsat runs.

## 8. Testing & acceptance gate

### Per-stage milestones

1. **Substrate + bridge:** `raise(lower(p)) == p` over a corpus covering every
   `ENode` kind that carries scalars (Map, Filter, FlatMap, Reduce, Join,
   WcoJoin, ArrangeBy, ArrangeByMany, IndexedFilter), plus existing engine unit
   tests — no rules running.
2. **Cache + analyses:** `escalar(id)` returns the interned `EScalar`;
   analyses produce identical facts to a pre-fusion baseline on a fixture
   corpus; the analysis split compiles with all analyses re-typed.
3. **Rules/matcher:** the existing relational rule unit tests pass against the
   combined graph (rules fire identically; `permute_cols` mints correct scalar
   classes).
4. **Keying safeguard:** a plan with cost-equal alternatives whose selection
   depends on scalar ordering extracts the **same** plan as pre-fusion.

### Behavior-neutral acceptance gate (the deliverable)

With `enable_eqsat_optimizer` at its default (on):

- `mz-transform` unit + datadriven (`.spec`) tests unchanged, **byte-identical**
  (no `REWRITE` / no `insta accept` — unchanged *is* the assertion). Confirmed in
  Task 3: 329 pass byte-identical.
- `bin/sqllogictest --optimized` over `test/sqllogictest/`: optimized/physical
  **plans semantically identical** to pre-SP4a. **Amendment (2026-06-28):** the
  EXPLAIN humanized column-**name** annotations (`{name}`) on fused scalar
  classes drift (cosmetic, `TreatAsEqual`; verified name-only — stripping `{…}`
  makes pre/post equal). Per the §1 correction, these affected EXPLAIN goldens
  are **regenerated** (Task 4) after per-file verification that each diff is
  name-only; any non-name diff is a real regression and is NOT rewritten. Eval
  slt **per-file standalone** — the multi-file sweep has pre-existing list-mode
  cross-file variance (`case_literal`, `uniqueness_propagation_filter`, etc. pass
  standalone), which is orthogonal and must not be conflated with the name drift.
- `clippy -p mz-transform --all-targets -D warnings` clean.

### Optional measurement (non-gating)

A `#[ignore]`d harness reporting scalar-class sharing (distinct scalar classes
vs total scalar references) to confirm CSE is happening and quantify the
structure SP4b will exploit — in the SP3a/SP3b measurement tradition.

## 9. Risks

- **Keying regression (§6.3):** the highest-likelihood source of golden
  drift. Mitigated by the hard keying requirement + the dedicated stage-4
  test.
- **Port surface size:** `matcher.rs`, the analyses, cost, extraction, engine,
  and transform all touch the `EScalar`→`Id` boundary. Mitigated by staging
  and by the cache keeping rule/analysis *logic* unchanged (only the access
  path moves).
- **`permute_cols` interning churn:** column-shifting rules now allocate
  scalar classes; a rule that permutes in a hot loop could allocate many
  near-duplicate classes. Behavior-neutral (correctness unaffected); if it
  shows up in the measurement, dedup is automatic via hash-cons.
- **Analysis `Ctx` plumbing:** threading the scalar-fact accessor through
  every analysis's `Ctx` is mechanical but broad; covered by the stage-2
  parity test.
