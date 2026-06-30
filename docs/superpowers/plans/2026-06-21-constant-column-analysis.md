# Constant-column e-class analysis + more-powerful condition language

**Goal:** Let eqsat collapse the outer-join null-extension for equality-filtered
self-joins (fixes `column_knowledge.slt:480` fast-path regression) by adding a
constant-column e-class analysis and a condition that consumes it, designed
forward-looking toward general scalar-expression reasoning.

## Why (root cause, established by clean diagnostics)
eqsat emits, for `t4 a1 LEFT JOIN t4 a2 USING(f1,f2) WHERE a1.f1=123 AND a1.f2=234`:
```
cte l0 = Filter(#0=123 AND #1=234)(Get u1)         -- used 3x (CSE-shared)
Union[ Map(123,234)(Negate(Project()(Get l0))), Get l0, Get l0 ]
```
`= -l0+l0+l0 = l0` (fast-path), but the Negate branch is structurally
`Map(123,234)(Negate(Project()(Get l0)))`, equal to `Negate(Get l0)` only if you
know l0's columns are the constants 123,234. That knowledge is trapped behind the
multi-use `Get l0` (CSE); no MIR post-pass can cross the Let boundary
(`EquivalencePropagation` post-pass tested, did not fix; `NormalizeLets` won't
inline a 3-use binding). The e-graph is the right home: an e-class analysis fact
lives on the congruence-shared class, available at all uses, so the rule fires
during saturation before extraction/CSE.

## Architecture (confirmed in code)
* Condition language = `Cond` enum, `src/transform/src/eqsat/dsl.rs:171`.
  Existing variants incl. analysis-backed ones: `NonNegative`(NonNeg),
  `IsUniqueKey`(Keys), `Monotonic`(Monotonic), `Unsatisfiable`(Equivalences).
* Conditions evaluated in `EGraph::check_conds(conds, b: &EBindings, an: &Analyses)`,
  `src/transform/src/eqsat/egraph.rs:1144`. `an` carries per-round per-class
  analysis results: `an.nn`, `an.keys`, `an.mono`, `an.eq`. Lookup pattern:
  `b.rels.get(rel)` -> Id -> `an.<x>.get(&self.find(id))`.
* Analyses defined in `src/transform/src/eqsat/analysis.rs` via `Analysis` trait
  (bottom/make/merge). Existing: `NonNeg`, `Monotonic`, `Keys`(+KeySet), `Equivalences`.
  `Equivalences` already derives col==literal (Filter arm pushes `[pred.., true]`)
  but is EXPENSIVE (minimize_bounded per merge; run bounded/cached, not every round).
* Analyses run via `run_analysis` / `run_analysis_bounded` (egraph.rs:1238); the
  cheap finite ones (NonNeg/Monotonic/Keys) run every saturation round
  (`Analyses` struct built ~egraph.rs:974-1000).
* Rules + condition syntax: `src/transform/src/eqsat/rules/relational.rewrite`;
  parser `parser.rs`; matcher `matcher.rs`. (Explorer afe0cd576 confirming exact
  condition syntax + whether a scalar-term notion already exists.)

## Design

### 1. `ConstantColumns` analysis (new, dedicated + cheap)
Do NOT reuse the expensive `Equivalences` for this; add a finite-height analysis
like `NonNeg`/`Keys`, run every round.
* `Domain = BTreeMap<usize, EScalar>` (column index -> its constant value as a
  scalar PAYLOAD, not a bare Datum) — FORWARD-LOOKING: same Domain generalizes
  from "column = literal" to "column = scalar expression" later.
* `bottom()` = empty map.
* `make`:
  * `Get`/`Constant`/`Opaque`: empty (opaque leaves).
  * `Filter(preds)`: for each pred of form `#i = <literal>` (or `<literal> = #i`),
    insert `i -> literal`. (Mirror non_null/equiv literal recognition.)
  * `Map(scalars)`: output col `input_arity+pos` -> scalar if it's a literal;
    also propagate `#j` if j already constant.
  * `Project(outputs)`: remap surviving constants to new positions.
  * `Join`: permute each input's constants into join layout; add constants implied
    by join equivalences linking a col to a literal.
  * `Union`: INTERSECT branches (a col is constant only if all branches agree on
    same value). Negate/Threshold: pass through input.
* `merge` = intersection on (col,value): keep only cols where both agree.
  Finite-height: monotonically shrinking map -> terminates.

### 2. Condition language: a small `ScalarTerm` sublanguage (the "more powerful language")
Rather than a one-off `ColIsConst`, add a general condition that grows:
* `ScalarTerm` enum: `Col(IxExpr)`, `Lit(Datum/EScalar)`, `Payload(name, idx)`,
  and (stub, forward-looking) `Apply(func, args: Vec<ScalarTerm>)`.
* `Cond::ScalarEquiv { rel: String, lhs: ScalarTerm, rhs: ScalarTerm }` — true iff
  `lhs` and `rhs` are equivalent under `rel`'s facts. Phase-1 impl: consult
  `ConstantColumns` (Col(i) ≡ Lit(c) iff col i is constant c); later escalate to
  the `Equivalences` `ExpressionReducer` for general equivalence.
* Parser: add `scalar_equiv(rel, <term>, <term>)` syntax in the .rewrite grammar,
  matching the existing condition-call style (confirm exact style from parser.rs).
* DECISION (user, 2026-06-21): incremental extensions OK, BUT they must not need a
  complete rewrite in the next step. So: build `ScalarTerm` + `Cond::ScalarEquiv`
  now, and ensure both are additively extensible — `ScalarTerm` carries an
  `Apply(func, args)` arm from day one (even if unconstructed) so adding function
  application later is a new match arm, not a type change; the Domain holds full
  `EScalar` (not bare Datum) so "col = expr" needs no Domain change; predicate
  algebra (and/or/not) is deferred but `Cond::ScalarEquiv` stays a leaf that a
  future `Cond::And(Vec<Cond>)` can wrap without touching ScalarEquiv.

### 3. The rule (relational.rewrite)
`Map(c)(Project([])(X)) -> X` guarded by: arity(X) == len(c) AND for each i,
`ScalarEquiv(X, Col(i), <c[i]>)`. This rewrites the null-extension Negate branch
`Map(123,234)(Negate(Project()(Get l0)))` to `Negate(Get l0)`, after which the
existing Z-set union-cancellation collapses `Union[Negate(l0),l0,l0]` -> l0 ->
fast-path peek.
(If the matcher can't express the per-i ScalarEquiv over a variadic payload,
fall back to a dedicated `Cond::MapProjectIsConstId { map_payload, rel }` that
checks the whole reshape against ConstantColumns in Rust — less elegant but
unblocks; revisit the term language after.)

## Verify
* Unit: datadriven analysis test for ConstantColumns (analysis.spec or a new spec).
* `column_knowledge.slt:480` returns to fast-path peek (clean rebuild, single-threaded).
* Full mz-transform `run_tests` green; SLT differential on a sample shows no new
  regressions; corpus rewrite only where eqsat genuinely improves.

## Confirmed architecture (explorer afe0cd576) + exact recipe
* Rule type: `Rule { name, doc: Option<String>, lhs: Pat, rhs: Tmpl, conds: Vec<Cond> }`
  (dsl.rs:241). Multiple `where` clauses are ANDed.
* Rule syntax in `.rewrite`: `Op[payload](children...) => template where cond(args)`.
  Uppercase=operator, lowercase=metavar, `[ident]`=payload binding, `ident...`=rest.
  PExpr helpers exist: `concat`, `shift`, `arity(rel)`, `cols_of(gk)`, IxExpr arithmetic.
* **Adding a `Cond` needs exactly 3 edits:** (1) enum arm in `dsl.rs` (Cond, ~:171),
  (2) `parse_cond` arm in `parser.rs:244-322`, (3) match arm in
  `egraph.rs::check_conds` (:1144). `an` (Analyses) + bindings `b` are in scope there.
* Scalars: `EScalar { expr: MirScalarExpr, lit: Option<bool> }` (ir.rs:59). Conditions
  today only see `lit` (true/false) and `is_col()` — NO general scalar-structure
  inspection. `lit` is set once at lower time (column types gone afterward).
* `Equivalences` is ALREADY in `an.eq: HashMap<Id, Option<EquivalenceClasses>>` at
  check_conds time. Query "col i ≡ literal": find the class in `ec.classes`
  containing `MirScalarExpr::column(i)`, scan it for `MirScalarExpr::Literal(Ok(row),_)`
  (mirror `EquivalenceClasses::unsatisfiable()` at analysis/equivalences.rs:1122).
  `ec.reducer()` gives `&BTreeMap<expr,canonical>`; `reducer.get(&column(i))` then
  `as_literal()`.

### Decision refinement (post-explorer)
Two viable backings for the constant-column fact:
* (A) DEDICATED `ConstantColumns` analysis — cheap, finite, every round. User asked
  for this explicitly; build it as primary.
* (B) Query existing `an.eq` (Equivalences) directly in the condition — zero new
  analysis, but Equivalences is expensive/bounded/cached (may miss facts).
RECOMMENDED layered/forward-looking design: `Cond::ScalarEquiv { rel, lhs, rhs }`
backed FIRST by the cheap `ConstantColumns` (Col(i) ≡ Lit case), ESCALATING to the
`Equivalences` reducer for general scalar equivalence later. This gives the user's
"more powerful condition language" with a cheap fast path + a general fallback.

### Build order (subagent-driven)
1. `ConstantColumns` analysis in analysis.rs (+ wire into `Analyses` struct egraph.rs:917
   and the per-round build ~egraph.rs:992; add `an.cc` field). Datadriven analysis test.
2. `ScalarTerm` enum + `Cond::ScalarEquiv` (3 edits above) backed by ConstantColumns;
   parser syntax `scalar_equiv(rel, <term>, <term>)`. Unit test on a synthetic e-graph.
3. The rule in relational.rewrite: `Map[c](Project[[]] x) => x where ...ScalarEquiv per i...`
   OR the `MapProjectIsConstId` fallback Cond if the matcher can't do per-i over variadic c.
4. Rebuild clean, confirm column_knowledge:480 -> fast-path peek; mz-transform run_tests
   green; SLT sample no new regressions.

## Constraints / hygiene (carried from session)
* it's all about eqsat; NO HIR changes. Use subagents for implementation.
* One sqllogictest at a time (shared CockroachDB); `killall sqllogictest` exact name,
  never `pkill -f`. Clean rebuild before trusting SLT (contamination history).
* See project memory `project_egraph_mir_optimizer.md` for full root-cause detail.
