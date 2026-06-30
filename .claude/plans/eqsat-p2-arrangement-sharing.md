# P2: Arrangement sharing (DD lessons L1/L4/L2/L3)

Builds on P1 (`Rel::ArrangeBy`/`ENode::ArrangeBy { input, key }`, committed f185ea73aa).
Turns the arrangement primitive into real wins: remove redundant arrangements, share them, and cost them once per distinct `(eclass, key)`.

Soundness frame: "already arranged by key k" is a PHYSICAL-property fact, not a linearity claim, so it is sound for the SQL-semantic non-linear operators too. Comment style: no em-dash, no structuring semicolons, doc = contract, no vendor names, never drop existing comments.

## Sub-phase P2a: L4 free CSE sharing (cheapest, validates P1)

`cse::eliminate_common_subexpressions` counts every repeated `Rel` subtree and binds it with `Let`. Since `ArrangeBy` is now a first-class `Rel` node with one child, a shared `ArrangeBy` already hoists for free, no new code.

**Files:**
* Test only. Add a unit test in `src/transform/src/eqsat/cse.rs` tests (or `eqsat.spec`): a plan that references the same `ArrangeBy(x, k)` from two parents extracts with a single `Let`-bound `ArrangeBy` and two `LocalGet`s.

**Verify:** the test shows one `ArrangeBy` bound once; `cargo test -p mz-transform`.

## Sub-phase P2b: L1 arrange-idempotence rule (the core)

Rule: `ArrangeBy[k](x) => x  where produces_key(x, k)`, tagged `phase physical`.
Unions the `ArrangeBy` e-class with `x`; extraction then drops the redundant arrange because the cost model charges the `ArrangeBy` memory term (P1) so the un-arranged `x` is cheaper.

### Prerequisite: key canonicalization (deferred from P1)

For `produces_key` to match, the arrangement key and what `x` produces must be in the same canonical scalar form. P1 left the `ArrangeBy` key un-canonicalized in `rewrite_escalars` (egraph.rs, the no-rewrite arm). Move `ArrangeBy` into a rewriting arm: rewrite the `key` like `Map` scalars (apply the e-class reducer, range `0..input_arity`, `lit: None`). The same circularity concern as `Filter` does NOT apply: an arrangement key adds no equivalences, so the node's own Equivalences == its input's, and there is no self-reference.

### The `produces_key(rel, key)` side condition

**Files:**
* `src/transform/src/eqsat/dsl.rs`: add `Pat::ArrangeBy { key, input }` and `Tmpl::ArrangeBy { key, input }` (mirror `Pat::TopK`/`Pat::Reduce` shape; key is a payload, input is a sub-pattern). Add `Cond::ProducesKey { rel, key }`.
* `src/transform/build/grammar.rs`: parse `ArrangeBy[<payload>](<pat>)` into `Pat::ArrangeBy` (mirror the `Reduce`/`Filter` parsers at lines ~270-322 for Pat and ~368-417 for Tmpl). Parse `produces_key(rel, key)` into `Cond::ProducesKey` (mirror `two_idents("is_unique_key")` at grammar.rs:473).
* `src/transform/build/codegen.rs`: emit the `Pat::ArrangeBy` matcher (find binds key + input) and `Tmpl::ArrangeBy` instantiator; emit the `Cond::ProducesKey` check calling `cond_produces_key`. Wire `Cond::ProducesKey` into `AnalysisNeeds` (it reads Keys, so set `keys: true`) in `emit_compiled`.
* `src/transform/src/eqsat/egraph.rs`: add `cond_produces_key(&self, an: &Analyses, rel: Id, key: &Payload) -> bool`. True when the canonical e-class of `rel` contains an e-node that already produces `key` as an arrangement key:
  * `ENode::ArrangeBy { key: k2, .. }` with `k2 == key`,
  * `ENode::Reduce { group_key, .. }` whose output columns `0..group_key.len()` correspond to `key` (a Reduce arranges by its group key),
  * `ENode::TopK` arranged by its group key,
  * an indexed `Get` whose available index key matches `key` (physical only; uses the cost model availability already threaded for WcoJoin).
  Use the Keys analysis / structural payload comparison. Compare canonicalized key payloads (see the prerequisite above).
* `src/transform/src/eqsat/rules/relational.rewrite`: add the `arrange_idempotent` rule, `phase physical`.

**Verify:** a plan with `ArrangeBy(Reduce(...), group_key)` extracts without the redundant `ArrangeBy`; `eqsat.spec` updated intentionally (this rule DOES change plans, so regenerate with `REWRITE=1`); `compiled_and_ast_agree` test passes (the dual codegen backends must agree).

## Sub-phase P2c: L2/L3 cost over distinct (eclass, key) set

Today `collect_memory` charges one memory term per `ArrangeBy`/`Reduce`/`TopK`/join-input independently, over-charging when the same `(eclass, key)` arrangement feeds multiple consumers (e.g. a delta join reading one input by several keys, or two joins sharing an input arrangement).

**Files:**
* `src/transform/src/eqsat/cost.rs`: change `collect_memory` to accumulate the SET of distinct `(eclass-or-structural-id, key)` arrangements, charging each at most once; the 2nd..Nth consumer costs 0. This requires identifying arrangements by `(input identity, key)` rather than counting node occurrences. Keep the existing `input_already_arranged` availability suppression as a special case (an externally-provided index is a pre-existing arrangement, cost 0).

**Verify:** a plan where one arranged collection feeds two joins charges one arrangement term, not two; `eqsat_bench` (LOW iters: filter_over_union is ~2.4s/op, so use 30-50) shows no regression; the triangle/mixed join plans still pick the same shape.

## Cross-cutting verification

* `cargo test -p mz-transform` (lib + spec), `cargo clippy -p mz-transform`, `cargo fmt`.
* `cargo build -p mz-transform` must re-run the build.rs codegen cleanly after grammar/codegen edits.
* Let CI surface SLT EXPLAIN churn; the idempotence rule should REMOVE redundant ArrangeBy nodes (improvement), not reorder.

## Coordination note

The B1-lite re-eval runs in a parallel worktree (branch `claude/b1-lite-reeval`); it edits `saturate`/`run_analysis_bounded` in egraph.rs. P2 edits `cond_*`/`rewrite_escalars`/grammar, different functions, so integration is low-conflict. If B1-lite lands first, rebase P2 onto it.
