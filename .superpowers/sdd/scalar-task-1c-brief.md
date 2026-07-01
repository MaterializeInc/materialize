# Task 1c: scalar boolean-algebra and NOT rewrite rules

Part of Phase 1 of the scalar equality-saturation canonicalizer. Adds a cohesive
batch of boolean rewrite rules to `rules.rs` using the infrastructure 1b built.
No infrastructure changes. No new analyses. Each rule is non-destructive and
gated by a property test that differentials against evaluation.

## Goal

Port the boolean simplifications of `MirScalarExpr::reduce` into the scalar
e-graph as local, non-destructive rules:

* AND/OR algebra: idempotent dedup, one-argument elimination, zero-argument unit,
  short-circuit to the absorbing element, drop the identity element.
* NOT normalization: double-negation elimination and NOT-over-negatable-binary.

De Morgan (NOT over AND/OR) is DEFERRED to a separate follow-up task. It is the
only rule that must construct nested NEW nodes (`OR(Not a, Not b)`), which the
read-only 1b rule contract cannot express without an apply-step extension. That
extension deserves its own focused task rather than riding inside this batch. Do
NOT implement De Morgan here and do NOT change the rule contract.

## Source of truth and the soundness contract

`reduce` is production-sound. Mirror its boolean rewrites FAITHFULLY, including
its exact conditions. Do NOT invent extra guards and do NOT drop conditions
reduce has. The e-graph's value is not "more guards than reduce", it is that no
rewrite can disable another (the phase-ordering and bundled-guard bug class,
CLU-137). The Phase 2 absorption rules are where `could_error` guards live; the
1c rules below mirror reduce's unconditional forms.

Reference rewrites:
* `reduce_and_canonicalize_and_or` at `src/expr/src/scalar.rs:790`:
  * `exprs.dedup()` after sort: `x AND x -> x`, `x OR x -> x`.
  * `exprs.len() == 1`: AND/OR of one arg evaluates to that arg.
  * `exprs.len() == 0`: evaluates to `func.unit_of_and_or()` (AND -> true, OR -> false).
  * any arg `== func.zero_of_and_or()`: short-circuit to the zero (AND with false
    -> false, OR with true -> true).
  * else `retain(|e| *e != func.unit_of_and_or())`: drop the unit (AND with true
    drops the true, OR with false drops the false).
* `reduce_pre` in `src/expr/src/scalar/reduce.rs` (the `UnaryFunc::Not` arm):
  * `Not(Not(x)) -> x`.
  * `Not(a bf b) -> a (bf.negate()) b` when `bf.negate()` is `Some`.
  * `Not(CallVariadic{..})` -> `e.demorgans()` (NOT over AND/OR pushes NOT inward
    and flips the connective). Read `MirScalarExpr::demorgans` for the exact
    transformation.

These boolean identities hold under three-valued logic (null). For example
`true AND x == x`, `false AND x == false`, `Not(Not(null)) == null`,
`Not(AND(a,b)) == OR(Not a, Not b)` all hold with nulls. The property tests
below verify this, including the null cases, so you do not need to reason it out
by hand, but you must include null and error inputs in the generators.

## Rules to implement (in rules.rs, added to the `rules()` list)

The rule signature is `fn(eg: &ScalarEGraph, node: &SNode) -> Vec<SNode>`
(read-only; return equivalent SNodes to union). Each rule matches one SNode
shape and reads children's classes via `eg.nodes(child)` (the e-nodes in a child
class) and `eg.analysis(child).literal` (the child's literal value, if any).

Reading a child's literal boolean: `eg.analysis(child).literal` is
`Option<(Result<Row, EvalError>, ReprColumnType)>`. For a boolean literal,
`Ok(row)` with `row.unpack_first()` being `Datum::True` / `Datum::False` /
`Datum::Null`. Write a small helper `fn lit_bool(eg, id) -> Option<bool>` that
returns `Some(true)`/`Some(false)` for non-null boolean literals and `None`
otherwise (a null or non-literal must not match the true/false rules).

1. `and_or_dedup`: match `CallVariadic{func: And|Or, exprs}` with duplicate child
   ids. Produce `CallVariadic{func, deduped}` (dedup by canonical id via
   `eg.find`). Preserve order of first occurrence.
2. `and_or_single`: match `CallVariadic{func: And|Or, exprs}` with exactly one
   element. Return the single child as its own class (union the And/Or class with
   `exprs[0]`'s class). Represent this by returning the child's sole e-node(s):
   since you cannot return an Id directly, union is achieved by returning the
   nodes of `eg.nodes(exprs[0])`. Confirm the cleanest mechanism (see "Unioning
   with an existing class" below) and use it consistently.
3. `and_or_empty`: match `CallVariadic{func: And|Or, exprs}` with zero elements.
   Produce `SNode::Literal` of `func.unit_of_and_or()` (AND -> literal true, OR
   -> literal false). Build via `MirScalarExpr::literal_true()` /
   `literal_false()` and destructure to `SNode::Literal`.
4. `and_or_short_circuit`: match `CallVariadic{And, exprs}` where some child has
   `lit_bool == Some(false)` -> produce literal false. `Or` with some child
   `Some(true)` -> literal true. (Mirror reduce's `zero_of_and_or` arm.)
5. `and_or_drop_unit`: match `CallVariadic{And, exprs}` with some child
   `lit_bool == Some(true)` -> produce `CallVariadic{And, exprs without the true
   ones}`. `Or` with `Some(false)` -> drop the false ones. (Mirror reduce's
   `retain` arm.) If dropping leaves one element, rule 2 handles the collapse on
   a later iteration; if it leaves zero, rule 3 does. Do not also collapse here.
6. `not_not`: match `CallUnary{Not, child}` where `eg.nodes(child)` contains a
   `CallUnary{Not, grandchild}` -> union with `grandchild`'s class.
7. `not_binary_negate`: match `CallUnary{Not, child}` where `eg.nodes(child)`
   contains a `CallBinary{bf, e1, e2}` with `bf.negate() == Some(neg)` -> produce
   `CallBinary{neg, e1, e2}` (the children `e1`, `e2` are existing ids, no new
   nested nodes needed).

## Unioning with an existing class

The 1b rule contract returns `Vec<SNode>` that the apply step `add`s and unions
into the source class. Every rule above produces either a literal, a
`CallVariadic`/`CallBinary` over EXISTING child ids, or a union with an existing
child class. None needs to construct nested new nodes, so the 1b contract is
sufficient. Do not change it.

* Rules 2 and 6 union the source class with an EXISTING child class. Within the
  current contract: return the e-nodes of that child class (`eg.nodes(child)`),
  which the apply step re-adds (hash-cons makes this return the existing class)
  and unions. Verify this achieves the union without duplicating nodes.

If you find a rule that genuinely cannot be expressed without a contract change,
STOP and report NEEDS_CONTEXT rather than reshaping the 1b contract. (De Morgan
is the known such case and is intentionally out of scope here.)

## Constraints (binding)

* Separate scalar engine: do not touch the relational engine.
* No `as` conversions (use `mz_ore::cast`).
* Comments: no em-dashes, no clause-joining semicolons; doc states the contract,
  reasoning inline.
* Non-destructive: rules return nodes to union, never remove the source node.
* Mirror reduce's conditions exactly; do not add or drop guards.
* Keep `const_fold` and the 1b infra unchanged except for the minimal apply-step
  extension rule 8 may require.

## Tests (per rule, property-based, the soundness gate)

For each rule, a property test that builds expressions matching the rule and
asserts the canonicalized result is observationally equal to the input under
evaluation. The generator MUST include: boolean columns bound to `true`, `false`,
AND `null`; and at least one error-producing operand (e.g. `1/0`) in an AND/OR to
confirm short-circuit rules are sound (if Materialize's AND/OR did not
short-circuit errors the way reduce assumes, the differential test catches it).

Concretely, write a helper that, given an expression over up to N boolean
columns, enumerates all assignments of `{true, false, null}` to those columns,
evaluates both `expr` and `canonicalize(&expr)` with each assignment via
`eval(&row, &RowArena)`, and asserts the results (including `Err` and `Null`) are
identical. Use it for:

* dedup: `AND(c0, c0, c1)` and `OR(c0, c0)`.
* single: `AND(c0)` (a one-arg AND, constructed directly) collapses to `c0`.
* empty: `AND()` -> true, `OR()` -> false (construct the empty variadic directly).
* short-circuit: `AND(c0, false_lit)` over all c0 incl null and incl a `1/0`
  operand: `AND(1/0 = expr, false_lit)` must equal the input under eval.
* drop-unit: `AND(c0, true_lit)` equals `c0`; `OR(c0, false_lit)` equals `c0`.
* not_not: `Not(Not(c0))` equals `c0` for c0 in {true,false,null}.
* not_binary_negate: `Not(c0 = c1)` equals `c0 != c1` (pick a binary func with a
  negate; `=`/`!=` is the canonical pair, confirm via `BinaryFunc::negate`).

Also keep all Phase 0 / 1a / 1b tests green.

A test that only checks structural shape is NOT sufficient for these rules. The
eval-differential over the {true,false,null} cube plus an error operand is the
required gate, because that is what proves the 3-valued-logic and error
semantics are preserved.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied.
* All new property tests pass; all prior scalar tests pass.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar boolean-algebra and NOT rules (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1c-report.md` and return
only: status, commit sha, one-line test summary, concerns. If you had to extend
the apply step for nested nodes (rule 8), describe the contract you chose.
