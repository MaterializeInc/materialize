# Task 1d: scalar If-condition resolution rules (type-free cases)

Part of Phase 1 of the scalar equality-saturation canonicalizer. Adds the
type-free If-resolution rules to `rules.rs` using the existing 1b rule
infrastructure. No infrastructure changes. No new analyses. No col_types.

## Goal

Port the constant-condition and identical-branch simplifications of
`reduce_if` into the scalar e-graph as local, non-destructive rules:

* `if(true, a, b) -> a`
* `if(false, a, b) -> b`
* `if(null, a, b) -> b`  (a null condition takes the else branch, per SQL)
* `if(c, x, x) -> x`     (identical branches, regardless of the condition)

## Source of truth

`reduce_if` at `src/expr/src/scalar/reduce/if_then.rs`:
* `cond.as_literal()` is `Ok(Datum::True)` -> `then`.
* `Ok(Datum::False) | Ok(Datum::Null)` -> `els`.
* `then == els` -> `then`.

DEFER (do NOT implement here, they are later tasks):
* The `Err(err)` condition arm. It builds `Literal(Err, then.typ.union(els.typ))`,
  which needs the branch types and therefore col_types the canonicalizer does
  not yet thread. That arm belongs to the col_types task.
* The boolean-then/els rewrites (`if(c, true, els) -> ...AND/OR...`). They build
  nested new nodes and need types. They belong to the nested-node and col_types
  tasks.

Mirror reduce's conditions exactly. Do not add or drop guards.

## Verified API you build on

`ScalarEGraph` (egraph.rs): `find(&self, Id) -> Id`, `nodes(&self, Id) -> Vec<SNode>`
(canonicalized class contents), `analysis(&self, Id) -> &ClassAnalysis`.

`ClassAnalysis` (analysis.rs): `{ could_error: bool, literal: Option<(Result<Row, EvalError>, ReprColumnType)> }`.

`SNode::If { cond: Id, then: Id, els: Id }` (node.rs).

The rule signature (do not change): `fn(eg: &ScalarEGraph, node: &SNode) -> Vec<SNode>`.
A rule unions the source class with an EXISTING class by returning that class's
e-nodes (`eg.nodes(child)`); the apply step re-adds them (hash-cons returns the
existing class) and unions. This is exactly how `and_or_single` and `not_not`
work in rules.rs; follow that pattern.

## Reading the condition's literal

The condition class may carry a boolean OR null literal. The existing `lit_bool`
helper returns `None` for null, so it cannot distinguish false from null. For
these rules you need the three cases true / false / null. Read
`eg.analysis(cond).literal` directly:

* `Some((Ok(row), _))` with `row.unpack_first()`:
  * `Datum::True` -> the true case.
  * `Datum::False` -> the false case.
  * `Datum::Null` -> the null case.
* Anything else (non-literal, error literal, non-boolean) -> the rule does not
  fire. (The error-literal condition is the deferred Err arm; leave it alone.)

Add a small helper near `lit_bool` if it keeps the rules clean, e.g.
`fn lit_bool_or_null(eg, id) -> Option<Option<bool>>` returning `Some(Some(true))`,
`Some(Some(false))`, `Some(None)` for null, and `None` otherwise. Choose whatever
is clearest; document it.

## Rules to implement (add to the `rules()` list in rules.rs)

1. `if_true`: match `SNode::If { cond, then, .. }` where cond's literal is
   `Ok(true)` -> return `eg.nodes(then)` (union the If class with the then class).
2. `if_false_or_null`: match `SNode::If { cond, els, .. }` where cond's literal is
   `Ok(false)` or `Ok(null)` -> return `eg.nodes(els)`.
3. `if_same_branches`: match `SNode::If { then, els, .. }` where
   `eg.find(then) == eg.find(els)` -> return `eg.nodes(then)`.

Rules 1 and 2 may be one function or two; keep them readable. Rule 3 fires
independent of the condition.

## Constraints (binding)

* Separate scalar engine: do not touch the relational engine.
* No `as` conversions (use `mz_ore::cast`).
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract.
* Non-destructive: return nodes to union, never remove the source node.
* No col_types, no new analyses, no nested-node construction, no rule-contract
  change. If a case seems to need any of those, it is a deferred case: skip it.

## Tests (per rule, eval-differential, the soundness gate)

Reuse the existing `assert_eval_equiv` / `bool_cube` / `eval_owned` harness in
the rules test module. The condition must be a LITERAL so the rule (not
const_fold) fires; the branches are COLUMNS so const_fold cannot collapse the
whole If.

* `if_true`: `if(true_lit, c0, c1)` canonicalizes to `c0`; differential over the
  2-column cube. Also assert structural `canonicalize(...) == c0`.
* `if_false`: `if(false_lit, c0, c1)` canonicalizes to `c1`.
* `if_null`: `if(null_bool_lit, c0, c1)` canonicalizes to `c1` (null takes else).
  Build the null condition with `MirScalarExpr::literal_null(ReprScalarType::Bool)`.
* `if_same_branches`: `if(c0, c1, c1)` canonicalizes to `c1`, over the 2-column
  cube. The condition is a column here (the rule is condition-independent), so
  const_fold cannot fire and only `if_same_branches` collapses it.

Each test asserts BOTH the structural result (`canonicalize == expected`) and the
eval-differential, so a non-firing rule cannot pass trivially.

Keep all prior scalar tests green.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* New If tests pass; all prior scalar tests pass.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar If-resolution rules (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1d-report.md` and return
only: status, commit sha, one-line test summary, concerns.
