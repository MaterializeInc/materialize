# SP4c — eqsat colored runtime hardening

Status: design (2026-06-29)
Branch: `claude/mir-equality-optimizer-sodbej`
Predecessors: SP4a (multi-sort fusion), SP4b (colored contextual equalities).
Successor: SP4d (colored saturation — deferred to a future session; see §7).

## 1. Goal

Solidify the SP4b colored runtime with a focused, low-risk hardening pass: make
the cost model scalar-aware, turn one panicking precondition into a safe API, and
clear the deferred micro-cleanups. **No new capability.** Colored saturation
(re-running rules over colored conclusions) is explicitly out of scope and split
out to SP4d.

## 2. Scope

**In:**
- (A) Scalar-aware joint cost — the one behavior-affecting item.
- (B) M1.1 — `arity()` robustness via a total `try_arity`.
- (C) M2 + M8 + M1.2 — iterate relational classes, not all classes.
- (D) M6 — `fresh_reducer` fallback bound/doc.
- (E) M7 — document the empty-but-unsat routing as an intentional divergence.

**Out (deferred):**
- Colored saturation, `close`/`close_all`/`add_colored`/`extract_colored` wiring,
  a colored rule driver, incremental dirty-tracking congruence, UF-edge
  remove-in-close, color-aware key/arrangement reasoning → **SP4d** (§7).
- Making `Rel::node_count()` itself scalar-aware (and thereby the CSE ordering
  key) → recorded as separate future work (memory
  `eqsat-scalar-aware-cse-node-count`); see §3.A and §7.

## 3. Components

### A. Scalar-aware joint cost (behavior-affecting)

Today `Rel::node_count()` counts only relational nodes — scalar subtree size is
invisible to the cost model. So replacing a Map scalar `#1 + 1` with the equal
column `#0` does not change `Cost.nodes`; the colored substitution is a separate
post-extraction phase (`resolve_scalar_colored`, with its own private
`scalar_cost`), not cost-driven. This component makes the cost model see scalar
size, using one shared scalar-cost function.

Changes:
1. **Relocate the scalar-cost fn to a shared home.** Move
   `colored_derive::scalar_cost` (tree-size node count of a `MirScalarExpr` via
   `visit_pre`) into `ir.rs` as the canonical scalar cost (e.g.
   `pub(crate) fn scalar_expr_cost(expr: &MirScalarExpr) -> usize`, or an
   `EScalar` method). `colored_derive` and `cost` both import it; the private
   duplicate in `colored_derive` is deleted.
2. **Add `Rel::scalar_node_count(&self) -> usize`** in `ir.rs`: sum
   `scalar_expr_cost` over every scalar payload of the node, recursing over
   relational children. The shared fn operates on `&MirScalarExpr`; `EScalar`
   payloads are reached via `.expr`, the one bare-`MirScalarExpr` payload
   (`TopKShape.limit`) directly. Scalar-bearing variants to cover:
   `Map.scalars`, `Filter.predicates`, `IndexedFilter.predicates`,
   `Join.equivalences`, `WcoJoin.equivalences`, `FlatMap.exprs`,
   `ArrangeBy.key`, `ArrangeByMany` (all keys), `Reduce.group_key` +
   `aggregates` (each `AggregateExpr.expr`), and `TopK.shape.limit`
   (`Option<MirScalarExpr>`). `Project.outputs` are `Col` (not scalars) and
   `Constant` carries row literals (not scalar trees) — neither is counted;
   `TopKShape.group_key`/`order_key` are `Col`/`ColumnOrder`, not counted.
3. **Make `Cost.nodes` scalar-aware in `CostModel::cost()`**:
   `nodes = rel.node_count() + rel.scalar_node_count()`.
   **Leave `Rel::node_count()` untouched** — it is also the CSE
   shared-subexpression ordering key (`cse.rs:77`, `cse.rs:361`), so changing it
   would churn CSE ordering/naming. Only `Cost.nodes` becomes scalar-aware.
   (Unifying the two is recorded as separate future work.)
4. **Point `resolve_scalar_colored` at the shared fn** (replacing its local
   `scalar_cost`), so the colored substitution's cheapest-spelling choice and the
   global cost tiebreaker agree on one definition of scalar size.

Effect: the extractor now discriminates plan alternatives by scalar complexity
directly (via the `Cost.nodes` tiebreaker), not only via post-hoc
`resolve_scalar_colored`. Golden churn is confined to cases where the memory and
time axes tie and the node tiebreaker decides — regenerate + review per-file
standalone.

### B. M1.1 — `arity()` robustness

`arity()` (`egraph/build.rs:363`) is defined only for relational classes; it
carries a `debug_assert!` and `expect`s a well-defined arity, so a scalar class
panics (clearly in debug, via the `expect` in release). Every current caller
passes a relational class.

Change: add a total `try_arity(&self, id: Id) -> Option<usize>` as the public
face of the existing `arity_guarded` recursion. `arity()` becomes the asserting
convenience wrapper that calls `try_arity().expect(...)` (keeping today's
behavior and message for the documented relational callers). Audit all `arity()`
callers to confirm each passes a relational class. This turns a documented
precondition into an actual safe API and gives future SP4d colored-saturation
code (which iterates mixed relational/scalar classes) a non-panicking primitive.

### C. M2 + M8 + M1.2 — iterate relational classes, not all classes

One theme: drop iteration over inert scalar classes.
- **M2** (`colored.rs::colored_class_members`): drop the redundant
  `self.base.find(y) == y` filter — `class_ids()` already returns only canonical
  keys, so it is a no-op. Verify that invariant, then remove.
- **M8** (`colored_derive::derive`): skip canonical classes with no `CNode::Rel`
  e-node (today they reach the per-class `fresh_reducer`/minimize work for
  nothing). Skip them up front (no relational node ⇒ no Filter/Map to reduce).
- **M1.2** (`egraph/build.rs::rel_index`, line 244): iterate `self.rel_class_ids()`
  instead of all `class_ids()`, so the matcher index build does not loop over
  scalar classes whose `rel_class_nodes` is empty. (`extract.rs:1166` is
  test-only and already uses `rel_class_nodes`; the generic analysis driver in
  `core.rs` cannot filter relationally without leaking relational concepts into
  the language-agnostic core, and already returns `bottom()` cheaply for scalar
  classes — documented, not changed.)

All three are behavior-neutral (perf/clarity only).

### D. M6 — `fresh_reducer` fallback

`colored_derive::fresh_reducer` clones an `EquivalenceClasses` and calls
`minimize(None)` (unbounded) when the reducer is unexpectedly empty. This can
theoretically over-reduce vs the bounded minimize `derive_facts` uses, in a
non-convergent corner that never fires in practice. Change: bound the fallback to
match the bound `derive_facts` uses (parity), and document the fallback as
defensive/never-firing. Behavior-neutral.

### E. M7 — empty-but-unsat routing (doc only)

`derive` routes an empty-but-unsatisfiable Filter to `empty_classes` and skips
predicate rewriting; Phase-2a would still rewrite the predicates. This is sound
(the class is empty) and arguably better. Deliverable: a code comment marking it
an intentional divergence from Phase-2a. No behavior change.

## 4. Task decomposition (SDD)

1. **Scalar-aware joint cost** (A) — the only golden-moving task. Relocate the
   shared fn, add `scalar_node_count`, wire `Cost.nodes`, repoint
   `resolve_scalar_colored`, regenerate + review goldens.
2. **`try_arity` robustness** (B) — add the total API, rewrap `arity()`, audit
   callers.
3. **Relational-class iteration cleanup** (C: M2/M8/M1.2).
4. **`fresh_reducer` bound + M7 doc** (D/E) — grouped doc/safety nits.

## 5. Testing

- **Joint cost (A):** unit test that `CostModel::cost()` reports more `nodes` for
  a plan with a complex scalar than for the same plan with a bare column (e.g.
  `Map[#1 + 1]` vs `Map[#0]` over the same input); a test that `scalar_node_count`
  sums across all scalar-bearing variants; regenerate every moved golden and
  review per-file standalone.
- **`try_arity` (B):** unit test that `try_arity` returns `None` for a scalar
  class and `Some(arity)` for a relational class; `arity()` still panics on a
  scalar class (documented precondition).
- **Iteration cleanup (C):** behavior-neutral — existing suites pass unchanged;
  a targeted unit asserting `rel_index`/`derive` cover the same relational
  classes as before.
- **D/E:** behavior-neutral; existing tests pass unchanged.

## 6. Behavior gate

- Full `bin/cargo-test -p mz-transform` (unit + datadriven `*.spec` goldens).
- Per-file-standalone `bin/sqllogictest --optimized -- <file>` for every golden
  that moves (expected only from Task 1).
- `cargo clippy -p mz-transform --all-targets -- -D warnings`.
- Only Task 1 (joint cost) is permitted to move goldens; any other task moving a
  golden is a regression to investigate.

## 7. Risks & deferrals

- **Risk — joint-cost golden churn:** the `Cost.nodes` tiebreaker shifts; bounded
  to memory+time ties. Mitigated by regenerate + review.
- **Deferral — SP4d colored saturation:** the high-risk evolution. Consumes
  SP3b's unused `close`/`close_all`/`add_colored`/`extract_colored`; needs a
  colored rule driver (the matcher/applier run over the base `EGraph`, not a
  `ColoredEGraph` view); sub-parts incremental congruence + UF-edge
  remove-in-close. Its only known payoff is recovering one accepted SP4b
  regression already normalized downstream. Full kickoff inputs in memory
  `eqsat-shared-core-extraction` (SP4d bullet).
- **Deferral — scalar-aware `Rel::node_count()` / CSE unification:** making
  `node_count()` itself scalar-aware would unify the CSE ordering key with the
  cost tiebreaker but churns CSE output for no clear correctness gain. Recorded in
  memory `eqsat-scalar-aware-cse-node-count`.
