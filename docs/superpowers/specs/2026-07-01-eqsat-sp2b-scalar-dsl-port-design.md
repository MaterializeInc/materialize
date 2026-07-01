# Eqsat SP2b: scalar rules to the declarative DSL, on one e-graph (Design)

> **Sub-Project 2b (SP2b)** of unifying the relational and scalar
> equality-saturation engines. SP2a made scalar a second `Language` instance on
> the generic core. SP2b ports the 21 hand-written scalar rules to the
> declarative rewrite DSL and runs them through the single `CombinedLang`
> machinery, deleting the standalone scalar e-graph. Behavior-neutral.
>
> Predecessor: `doc/developer/design/20260624_eqsat/20260627_eqsat_scalar_instance.md`.
> Substrate rationale: `doc/developer/design/20260624_eqsat/20260625_eqsat_scalar_expressions.md`.

## North star (beyond SP2b)

The end-state is a **unified saturation pass**. Relational and scalar rewrite
rules saturate together in one `EGraph<CombinedLang>`, enabling cross-sort
rewrites: a relational rule matching or building scalar structure, a scalar
simplification informing relational extraction. SP2b builds toward this but does
**not** turn on the mixed pass. It delivers the rule-layer unification and the
DSL/codegen machinery that a later sub-project needs to activate the mixed pass.

## Goal

1. The 21 hand-written scalar rules (`src/transform/src/eqsat/scalar/rules.rs`)
   re-expressed as **declarative grammar entries**, compiled by the existing
   build-time codegen into `CompiledRule`s targeting `CNode::Scalar`.
2. Each ported rule gets an **auto-generated, `sorry`-stubbed Lean theorem**
   (1:1, structural, same mechanism as relational), backed by a scalar
   denotation in `Semantics.lean` bounded to the operators the 21 rules touch.
3. Scalar canonicalization runs through the **single `CombinedLang` machinery**
   (core, matcher, and `saturate`). The standalone `EGraph<ScalarLang>`
   instantiation (`ScalarEGraph`), its `saturate` driver, the hand-written
   matchers, and the standalone scalar extractor are **deleted**.
4. `crate::eqsat::scalar::canonicalize()` re-expressed to spin up a
   `CombinedLang` graph seeded with the scalar expr and run the scalar rule
   subset.

## Global constraints

- **Behavior-neutral, strict gate.** Identical rewrites, identical goldens. No
  `--rewrite`, no `cargo insta accept`. A behavior-neutral refactor that needs a
  golden rewrite has, by definition, changed behavior. One narrow escape (see
  Testing): a golden provably cost-neutral tie churn may be regenerated
  per-golden with written justification. Default is align.
- **`ScalarLang` the type stays.** `combined.rs` delegates to
  `ScalarLang::symbol/on_add/on_union` for `CNode::Scalar`. SP2b deletes the
  standalone `EGraph<ScalarLang>` *instantiation* and its driver, rules, and
  extractor. It does not delete the `ScalarLang` type, nor
  `SNode`/`ScalarGraphData`/`analysis.rs`.
- **Relational grammar byte-unchanged.** `relational.rewrite` is not edited. All
  codegen changes are additive scalar arms. Relational goldens are a regression
  check in every slice.
- **Delete last.** The old `EGraph<ScalarLang>` engine is kept as an A/B oracle
  and deleted only in slice 7, after every slice's parity gate is green.

## Non-goals (deferred)

- Turning on the mixed relational+scalar saturation pass.
- Rewriting relational rules to embed scalar sub-patterns. The grammar will
  *support* it (Approach A). SP2b does not *use* it.
- Changing scalar cost (stays tree-size) or the analysis lattice (stays
  `could_error` plus `literal`).
- Any colored or multi-sort runtime work.

## The two e-graphs today

- **`EGraph<CombinedLang>`** (`CNode = Rel(ENode) | Scalar(SNode)`,
  `egraph/combined.rs`) is the relational engine's graph. Scalar classes already
  live here, but only as inert payload, read via the `escalar` cache. No scalar
  *rules* fire here.
- **`EGraph<ScalarLang>`** (`ScalarEGraph`, `scalar/egraph.rs`) is SP2a's
  standalone canonicalizer. `canonicalize()` spins up a fresh one, lowers one
  `MirScalarExpr`, runs the 21 hand-written Rust rules via its own `saturate`,
  and extracts via `scalar/raise.rs`. Gated `enable_eqsat_scalar_canonicalize`,
  offline predicate-canonicalization only.

"One e-graph at the end" means retiring the `EGraph<ScalarLang>` instantiation.

## The production seam (slice-7 blast radius)

`crate::eqsat::scalar::canonicalize_predicates` (and the `canonicalize()` it
wraps) is a **production directional-pipeline utility**, not
eqsat-relational-local. Three callers, all gated
`enable_eqsat_scalar_canonicalize`:

- `src/transform/src/predicate_pushdown.rs`
- `src/transform/src/literal_constraints.rs`
- `src/transform/src/fusion/filter.rs`

The eqsat *relational* pass never calls it (it is scalar-opaque). Slice 7 swaps
the internal engine (`ScalarEGraph` becomes `CombinedLang`), keeping these three
callers byte-unchanged. Blast radius is the output of PredicatePushdown,
LiteralConstraints, and Fusion across the whole slt corpus. This is why the
strict gate and corpus adequacy are load-bearing.

> Naming. The reroute target is fully-qualified
> `crate::eqsat::scalar::canonicalize_predicates`. It is distinct from
> `LiteralConstraints::canonicalize_predicates`, which is a method, one of the
> callers, not the thing being rerouted.

## Architecture: one grammar over `CNode` (Approach A)

A single rule can carry both sorts. Relational rules already reference scalars as
predicates, keys, and aggregates, and the north-star mixed pass needs cross-sort
rules. So the vocabularies cannot be separated. The grammar spans both, matching
and constructing `CNode` uniformly. Scalars stop being opaque payload and gain
structural `Pat`/`Tmpl` variants. Pure-scalar rules are the scalar-only subset.

### 2.1 Grammar and AST: scalar structural vocabulary

Add scalar structural variants to `Pat` and `Tmpl` (`dsl.rs`,
`build/grammar.rs`), mirroring `SNode`: `SColumn(usize)`, `SLiteral(...)`,
`SUnary(UnaryFunc, pat)`, `SBinary(BinaryFunc, pat, pat)`,
`SVariadic(VariadicFunc, listpat)`, `SIf(pat, pat, pat)`. Scalar metavars bind a
scalar e-class and reuse the existing metavar mechanism (a metavar is an `Id`,
sort-agnostic). Variadic scalar operands reuse the existing `ListPat`/`ListTmpl`
(`items` plus `rest`) machinery, the same `rest...` splice relational Join/Union
use today (for example `relational.rewrite:169`,
`Filter[p] (Join[e](a, rest...)) => Join[e](Filter[p] a, rest...)`). A rule is
"scalar" iff its LHS root is a scalar variant.

### 2.2 Codegen: node-vocabulary dispatch

`build/codegen.rs` hard-codes `ENode` in three spots: `sym_name` (Pat to Sym),
`Matcher::node` (match-arm emission), `tmpl_stmts` (Tmpl to constructor). Extend
each with scalar arms emitting against `CNode::Scalar(SNode::…)` and
`CSym::Scalar(ScalarSym::…)`. Additive: relational arms unchanged, scalar arms
new. Output stays the same `CompiledRule { find, apply }` shape over
`EGraph<CombinedLang>`.

### 2.3 Match/apply surface: scalar methods on the view traits

`MatchGraph`/`ApplyGraph` (`egraph/view.rs`) are relational-facing. Add
scalar-facing methods the generated scalar code calls.

- Match: `scalar_class_nodes(id) -> Vec<SNode>`,
  `nodes_by_scalar_sym(ScalarSym)`, and analysis reads
  `scalar_lit(id) -> Option<(Result<Row, EvalError>, ReprColumnType)>`,
  `scalar_could_error(id) -> bool`.
- Apply: `add_scalar(SNode) -> Id`.

`BaseView`/`EGraph` implement them by reading the already-present
`CombinedData.scalar`, with no new state.

### 2.4 Conditions: scalar analysis gates

`Cond` gains scalar variants read by the gated rules: `scalar_lit_bool`,
`scalar_lit_bool_or_null`, `scalar_could_error(var)`, `scalar_is_non_nullable`,
`scalar_lit_err`. These are scalar analogs of relational
`is_unique_key`/`no_error`, gated on `CombinedData.scalar` analysis (maintained
incrementally via `on_add`/`on_union`). Codegen emits them as guards in `find`.
Lean maps them to theorem hypotheses, the same as relational conds.

### 2.5 Builtin appliers: const-eval and type-context escape hatch

Some rules cannot be pure LHS-to-RHS patterns because their RHS is computed by an
`mz_expr` function (evaluation or type inference). Re-implementing the evaluator
or type-checker in the grammar is wrong on both YAGNI and correctness grounds.
Instead a **builtin-applier** template term. The RHS names a Rust function
(`=> const_eval(self)`, `=> typed_null(func, args)`), which codegen calls with
the bound `Id`s and graph. This mirrors the relational DSL's payload-algebra
builtins (`concat`/`shift`/`remap`). The LHS stays declarative and
Lean-checkable. Only the RHS construction is a named builtin, whose theorem is
permanently `sorry`.

**The builtin-applier set is exactly these 6 rules** (RHS is a Rust function):

1. `const_fold`, reconstruct plus `eval`.
2. `if_err_cond`, retype the error to the branch-union type via
   `call_scalar_type`.
3. `null_prop_binary`, `typed_null`.
4. `err_prop_binary`, `typed_err`.
5. `null_prop_variadic`, `typed_null` (variadic).
6. `err_prop_variadic`, `typed_err` (variadic).

**Not builtins**, because their RHS is declarative (type-dependence lives in a
condition or is a constant literal), so they are provable-later, not
permanent-`sorry`: `and_or_empty` (RHS is the constant `true`/`false` literal)
and `isnull_fold` (RHS is the constant `false`, nullability is a `Cond`, not the
RHS).

The builtins MUST reuse the same `mz_expr` functions the old rules call
(`eval`, `call_scalar_type`), not reimplement them.

**Re-home dependency.** Today's `call_scalar_type` (`rules.rs:875`) computes a
call's result type by calling `raise::raise` on each child
(`raise::raise(eg, c).typ(eg.col_types())`, see `rules.rs:879`, `951-952`,
`1148`). Both `rules.rs` and `scalar/raise.rs` are deleted at slice 7. So the
type-context builtins (slices 5 and 6) must obtain the raise-equivalent and the
type helper from the new `eqsat/scalar_extract.rs`, not the old `raise.rs`.
Concretely, `scalar_extract.rs` exposes `pub fn raise(eg: &EGraph<CombinedLang>,
id: Id) -> MirScalarExpr` and a `call_scalar_type` re-homed to read
`CNode::Scalar`. Slice 1 pins this public API (see the slice table), so the
slice-5/6 builtin subagents have a stable target.

### 2.6 Scalar extractor on CombinedLang, with determinism parity

Port `scalar/raise.rs`'s bottom-up tree-size cost plus fixpoint reconstruction to
read `CNode::Scalar` nodes from `EGraph<CombinedLang>`. Same algorithm
(`compute_costs` relaxation plus `build`), new node accessor. This makes a
scalar-rooted CombinedLang graph extractable. `canonicalize()` becomes: lower the
scalar expr into a CombinedLang graph, `saturate` with the scalar rule subset,
scalar-extract from the root. No synthetic relational root is needed.

**Determinism-parity sub-task (decides whether the gate is met).** The ported
extractor must reproduce, verbatim, the old extractor's tie resolution. Same
result, not just same cost function:

1. Tie-break comparator, copied literally from `raise.rs:124`:
   `ca.cmp(&cb).then_with(|| a.cmp(b))`, cost then `SNode::Ord`. `SNode` is the
   shared node type, so `SNode::Ord` is identical on CombinedLang. Parity by
   construction, not hypothesis.
2. The And/Or operand `sort()` in `reconstruct`, which matches
   `MirScalarExpr::reduce`'s canonical operand order.
3. The saturate bounds matched on the CombinedLang scalar run. A cap makes
   iteration order observable, so mismatched caps leak order into the result.
   Define them as module-level constants in the new scalar canonicalize path,
   mirroring the values in `scalar/egraph.rs` verbatim: `MAX_ENODES = 600`,
   `MATCH_LIMIT = 1_000`, `MAX_ITERS = 100`. They are copied, not shared at
   runtime, because the old constants are private to the `EGraph<ScalarLang>`
   module that slice 7 deletes.

### 2.7 Lean: scalar semantics and emission

`Semantics.lean` gains a `ScalarExpr` denotation **bounded to the operators the
21 rules reference** (And/Or/Not, If, the binary-negate pairs, IsNull, plus the
literal/error/null constructors), not all of `MirScalarExpr`. `lean.rs`'s
`emit_rule` gains scalar arms in `translate_pat`/`translate_tmpl`/`choose_proof`.
Trivially-provable rules (boolean laws, `not_not`) get tactics, the rest emit
`sorry` with a comment. One theorem per rule (structural 1:1), into
`Generated.lean` alongside relational theorems (one grammar, one file).

The **6 builtin-applier rules of 2.5 are the permanent-`sorry` set**. Their RHS
is a Rust function, never Lean-provable. The ledger convention: each such theorem
carries the exact comment line `-- PERMANENT SORRY: RHS is a Rust builtin`
immediately above its `sorry`, distinct from provable-later `sorry`s (which carry
a rule-specific `-- TODO` comment). A CI check greps `Generated.lean` and asserts
the `PERMANENT SORRY` count is exactly 6, so a regression that turns a provable
rule into a builtin (or vice versa) fails the build rather than silently
mislabeling the ledger.

### Component boundaries

| Component | File(s) | Change |
|---|---|---|
| Grammar/AST | `dsl.rs`, `build/grammar.rs` | add scalar `Pat`/`Tmpl` variants |
| Codegen | `build/codegen.rs` | add scalar arms in `sym_name`/`Matcher::node`/`tmpl_stmts` |
| View traits | `egraph/view.rs` | add scalar match/apply methods (read existing `CombinedData.scalar`) |
| Conditions | `dsl.rs`, `build/codegen.rs`, `lean.rs` | add scalar gate `Cond` variants |
| Builtin appliers | `build/codegen.rs`, new `eqsat/scalar_builtins.rs` | const-eval / typed-literal RHS builtins (6 rules), import raise/`call_scalar_type` from `scalar_extract.rs` |
| Scalar extractor | new `eqsat/scalar_extract.rs` (from `scalar/raise.rs`) | reads `CNode::Scalar` on CombinedLang, determinism-parity, exposes `pub fn raise(eg, id)` and re-homed `call_scalar_type` (pinned by slice 1) |
| Lean | `lean/MirRewrite/Semantics.lean`, `lean.rs` | scalar denotation (bounded) plus emission arms |
| Rules source | `*.rewrite` | 21 scalar rules declarative |
| Deletions (slice 7) | `scalar/{egraph,rules,raise,lower}.rs` | remove standalone `EGraph<ScalarLang>` path |

## Slice decomposition

Seven slices, each an independently testable sub-plan. **A/B oracle
throughout.** Production `canonicalize_predicates` stays on the old
`EGraph<ScalarLang>` engine for slices 1 through 6. The gate is a **differential
test**. Both drivers are parametrized by a rule-name subset, asserting
`new_combined(e, ct) == old_scalar(e, ct)` over the adversarial corpus (below),
restricted to the rules ported so far. The old engine is the diff target and is
deleted only in slice 7. Every slice also runs the scalar unit tests green.

| Slice | Machinery landed | Rules ported (of 21) | Gate |
|---|---|---|---|
| **1, Seam (go/no-go)** | scalar `Pat`/`Tmpl` AST (2.1), codegen scalar arms (2.2), scalar view methods (2.3), scalar extractor on CombinedLang with determinism parity (2.6) exposing the pinned `pub fn raise(eg, id)` + re-homed `call_scalar_type`, CombinedLang `canonicalize` path (lower, saturate, scalar-extract), differential harness **and the corpus fixture** (one work unit, not split), Lean scalar denotation skeleton plus emit dispatch (sorry) | `and_or_dedup`, `and_or_single`, `not_not`, `not_binary_negate` (4, plain structural) | slice-1 gate below |
| **2, Variadic** | wire scalar variadic `Pat`/`Tmpl` plus `rest...` splice onto existing `ListPat`/`ListTmpl` | `flatten_assoc`, `not_demorgan` (2, unconditional variadic) | differential parity |
| **3, Analysis gates** | scalar `Cond` variants plus `scalar_lit`/`scalar_could_error` view methods (2.4) | `and_or_short_circuit`, `and_or_drop_unit`, `if_true`, `if_false_or_null`, `if_same_branches` (5) | differential parity |
| **4, Const-eval builtin** | builtin-applier term (2.5), `const_eval` builtin, constant-literal RHS template | `const_fold` (builtin), `and_or_empty` (declarative literal) (2) | differential parity |
| **5, Type-context builtin (binary)** | `typed_null`/`typed_err` builtins plus `scalar_is_non_nullable` cond, reading `col_types` | `if_err_cond`, `null_prop_binary`, `err_prop_binary` (builtins), `isnull_fold` (declarative plus cond) (4) | differential parity |
| **6, Variadic-set** | `inner_sets`-style variadic-set reasoning in DSL/builtins | `null_prop_variadic`, `err_prop_variadic` (builtins), `factor_and_or`, `absorb_and_or` (4) | differential parity |
| **7, Flip plus delete** | route production `crate::eqsat::scalar::canonicalize_predicates` to CombinedLang, delete `scalar/{egraph,rules,raise,lower}.rs` standalone path (keep `ScalarLang`/`SNode`/`ScalarGraphData`/`analysis.rs`) | none | **full `enable_eqsat_scalar_canonicalize` slt goldens, no `--rewrite`**, scalar unit tests green |

**Rule tally.** 4 plus 2 plus 5 plus 2 plus 4 plus 4 equals 21.

**Ordering.** Linear dependency, since each slice's machinery is used by later
ones, but slices 2 through 6 are independent in their *rule* content. Slice 1 is
the critical seam and the go/no-go. It proves determinism-parity and
fixpoint-equivalence on 4 trivial rules under matched bounds. Pass, and slices 2
through 7 grind a proven path. Fail, and you reassess the one-grammar-over-CNode
approach having ported only 4 rules. Slice 7 is the irreversible production flip
and must be last. Capability-ascending order means a gate failure localizes to
the one capability that slice added. The heaviest rules (type, error,
variadic-set) come last, when the machinery beneath them is parity-proven.

**Lean is per-slice, not a trailing slice.** Each slice extends the scalar
denotation for its operators and emits a theorem per ported rule (tactic where
trivial, else `sorry`), preserving the 1:1 invariant from slice 1. The 6
builtin-applier rules are the permanent-`sorry` set.

## Testing and gates

- **Per-slice (1 through 6): differential parity.** `new_combined(e, ct) ==
  old_scalar(e, ct)` over the adversarial corpus, both drivers restricted to the
  rules ported so far. Continuous no-changes check against a live oracle, not
  deferred to goldens.
- **Adversarial corpus (construction procedure).** The corpus is a committed
  static fixture, built once in slice 1 and part of that slice's work unit (do
  not split corpus-building from harness-building, they share the file path and
  format). Construction:
  1. **Collect.** Instrument the three production callers
     (`predicate_pushdown.rs`, `literal_constraints.rs`, `fusion/filter.rs`)
     under `enable_eqsat_scalar_canonicalize` to dump each `(expr, col_types)`
     pair they pass to `canonicalize_predicates` during a full transform-suite
     slt run (`bin/sqllogictest --optimized -- test/sqllogictest/transform/`).
     Deduplicate structurally.
  2. **Commit** the deduplicated pairs as a static fixture at
     `src/transform/tests/testdata/eqsat_scalar_corpus` (datadriven format),
     checked in so the differential test is hermetic and does not re-run slt to
     regenerate inputs.
  3. **Acceptance criterion for "dominates".** The harness runs a coverage
     assertion over the fixture and fails if any of the three divergence axes is
     unrepresented: at least one input yielding an equal-cost extraction tie, at
     least one input whose subtree sets `could_error`, and at least one input
     exercising a non-nullable `isnull_fold` or a type-context null/error
     propagation. These three assertions are the operational meaning of
     "dominates the goldens' feature space". Parity on the corpus implies parity
     on the goldens only if this coverage holds.
- **Slice 1 gate (operational).** Two checks, both on the 4-rule corpus subset:
  1. **Extraction identity.** For every corpus input, the CombinedLang scalar
     `canonicalize` produces the same `MirScalarExpr` as the old
     `EGraph<ScalarLang>` `canonicalize`. This is the pass/fail criterion. No
     iteration-count or e-node-count matching is required, extraction identity
     suffices because the extractor is determinism-parity by construction (2.6),
     and the bounds are copied constants (2.6.3).
  2. **Termination.** The CombinedLang scalar `saturate` terminates within the
     copied bounds on every corpus input (does not hit an unexpected growth
     blow-up that the old engine did not).
  Failure of check 1 is the go/no-go trigger: reassess one-grammar-over-CNode
  having ported only 4 rules.
- **Every slice.** Existing scalar unit tests (`eqsat::scalar`, `analysis`)
  green, and relational goldens unchanged, run
  `bin/sqllogictest --optimized -- test/sqllogictest/transform/` and confirm no
  diff without `--rewrite` (regression check on the additive codegen).
- **Slice 7 (the real gate).** Full `enable_eqsat_scalar_canonicalize` slt
  goldens: `bin/sqllogictest --optimized -- test/sqllogictest/transform/` with
  the flag in its test-on state, **no `--rewrite`, no `insta accept`**. One
  narrow escape: a golden provably cost-neutral tie churn may be regenerated
  per-golden with written justification. Default is align.
- **Lean.** `cargo run -p mz-transform --example gen-lean` produces
  `Generated.lean` with one theorem per scalar rule. The 6 builtin appliers are
  marked permanent-`sorry`.
- **Cheap checkers before each slice's commit.** `bin/fmt`, `cargo check`.

## Risks

- **Driver-swap fixpoint drift (the real hazard).** 1:1 rules do not guarantee
  1:1 fixpoint. The old driver genuinely saturates (loops to "no new unions",
  full congruence closure over all 21 rules, not a directional reduce), so the
  risk is equal-cost ties, not systematic canonical-form drift. Mitigated by the
  determinism-parity port (2.6), the differential oracle, and delete-last. A
  genuine, un-closable divergence is a **reportable finding**, not a patch.
- **Corpus under-coverage.** Slice 7 surprises if the corpus misses a predicate
  shape the three passes produce. Mitigated by the adversarial-corpus
  requirement. Residual risk owned at slice 7.
- **Approach-A blast radius on relational codegen.** Additive scalar arms only,
  relational grammar byte-unchanged, relational goldens regression-checked every
  slice.
- **Builtin-applier correctness.** The 6 Rust RHS builtins must reuse the same
  `mz_expr` functions the old rules call, not reimplement them.

## Open questions

None blocking. Activating the mixed relational+scalar saturation pass (the north
star) is deferred to a later sub-project and will get its own design. It depends
on the DSL/codegen machinery this sub-project lands, plus a cross-sort cost model
and termination story not in scope here.
