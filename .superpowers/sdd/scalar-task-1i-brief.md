# Task 1i: De Morgan rule (NOT over AND/OR)

Part of Phase 1 of the scalar equality-saturation canonicalizer. This is the LAST
Phase 1 rewrite rule. It adds a single new rule, `not_demorgan`, that pushes a
`Not` through an `And`/`Or`. It depends on the Task 1h rule contract (rules build
nested NEW nodes via `eg.add` and return `Vec<Id>`); confirm 1h has landed before
starting (the `Rule` type is `fn(eg: &mut ScalarEGraph, node: &SNode) -> Vec<Id>`).

## The rewrite

```text
NOT(AND(a, b, ...)) -> OR(NOT a, NOT b, ...)
NOT(OR(a, b, ...))  -> AND(NOT a, NOT b, ...)
```

This is the only boolean rewrite that must construct NEW intermediate nodes (the
per-operand `Not` nodes), which is exactly why it waited for the 1h contract.

Source of truth: `MirScalarExpr::demorgans` (`src/expr/src/scalar.rs:831`), reached
from `reduce_pre`'s `Not` arm over a `CallVariadic` (`reduce.rs:91`,
`e.demorgans()`). Mirror it. The two helpers it uses are reusable:

* `VariadicFunc::switch_and_or()` (`src/expr/src/scalar/func/variadic.rs:1757`)
  flips `And`<->`Or`. It `unreachable!()`s on any other variadic, so the rule MUST
  guard with the existing `is_and_or(func)` before calling it.
* `MirScalarExpr::not(self)` (`src/expr/src/scalar.rs:232`) wraps an expr in `Not`.
  In the e-graph you build the `Not` node directly instead:
  `eg.add(SNode::CallUnary { func: not_func, expr: operand_id })`. Get `not_func`
  by cloning the outer node's `func` (it is the `Not` you matched).

## Soundness: UNCONDITIONALLY sound, NO could_error gate

Unlike the 1e/1f null/error-propagation rules, De Morgan needs no `could_error`
gate. It is a structural equivalence that preserves evaluation exactly, including
three-valued logic AND error/short-circuit behavior, because:

* De Morgan holds in Kleene strong three-valued logic:
  `NOT(AND(xs)) == OR(NOT xs)` for every assignment over {true, false, null}.
* It preserves operand ORDER (map over `exprs` in place) and switches the
  short-circuit value in lockstep with `switch_and_or`: `And` short-circuits on
  `false`, `Or` on `true`, and the wrapping `Not` maps `false`<->`true`. So which
  operand decides the result, and which error (if any) surfaces under
  left-to-right eval, is identical on both sides.

Worked check (the CLU-137-class concern, errors under short-circuit):
`AND(false, err)` evaluates to `false` (And short-circuits on false, ignoring the
later error, as `test_and_or_short_circuit_with_error` shows), so
`NOT(AND(false, err)) == true`. The rewritten `OR(NOT false, NOT err) ==
OR(true, err) == true` (Or short-circuits on true). They agree. This is why
`reduce` applies `demorgans` ungated, and why this rule does too.

State this rationale in the rule's doc comment (concise: structural equivalence,
order- and short-circuit-preserving, hence no gate, mirrors `reduce::demorgans`).

## Do NOT flatten associativity

`reduce::demorgans` calls `flatten_associative()` first. Do NOT port that. The
e-graph has no associativity flattening rule (it is a separate, deferred concern),
and De Morgan is sound on a non-flattened variadic as-is:
`NOT(AND(a, AND(b, c)))` rewrites to `OR(NOT a, NOT(AND(b, c)))`, and saturation
applies De Morgan again to the inner `NOT(AND(b, c))` on a later iteration. Operate
on the matched variadic node directly.

## The rule

Add `not_demorgan` to `rules.rs`, alongside the other `not_*` rules, and register
it in `rules()`. Follow the structure of `not_not` / `not_binary_negate`:

1. Match `SNode::CallUnary { func, expr }`; return `vec![]` unless `func` is `Not`.
2. Read the child class nodes (`eg.nodes(*expr)`, owned `Vec<SNode>`) into a local
   BEFORE any mutation, per the 1h borrow discipline.
3. For each child that is `SNode::CallVariadic { func: vf, exprs }` with
   `is_and_or(&vf)`: build a `Not` node id for each operand via `eg.add`, then
   `eg.add` the `CallVariadic` with `vf.switch_and_or()` over those `Not` ids, and
   push the resulting `Id`.
4. Return the collected ids (possibly several, if the class held more than one
   And/Or node; usually one). An empty vec means the rule did not fire.

Clone `func`/`vf` out of the borrowed nodes as needed so the immutable borrow is
dropped before the `eg.add` calls.

## Tests (the gate)

Add tests in the `rules.rs` test module, in the existing style
(`assert_eval_equiv` over `bool_cube`, plus structural and error checks). All
three are required:

1. **Soundness over the cube.** `assert_eval_equiv(NOT(AND(c0, c1)), 2)` and
   `assert_eval_equiv(NOT(OR(c0, c1)), 2)`. Proves three-valued-logic
   preservation over {true, false, null}.

2. **Firing proof via cost-reducing composition.** Min-cost extraction prefers the
   CHEAPER form. `OR(NOT a, NOT b)` is more expensive than the original
   `NOT(AND(a, b))`, so `canonicalize(NOT(AND(c0, c1)))` extracts the ORIGINAL
   unchanged. Do NOT write a test asserting `canonicalize` returns the flipped
   form directly: it will fail. Instead prove firing through a case where De Morgan
   ENABLES a cheaper result by composing with `not_not`:

   ```text
   canonicalize(NOT(AND(NOT c0, NOT c1))) == OR(c0, c1)
   canonicalize(NOT(OR(NOT c0, NOT c1)))  == AND(c0, c1)
   ```

   De Morgan rewrites `NOT(AND(NOT c0, NOT c1))` to `OR(NOT NOT c0, NOT NOT c1)`,
   then `not_not` collapses each double negation, and `OR(c0, c1)` is cheaper than
   the input, so extraction picks it. This can only happen if `not_demorgan`
   fired. Pair each with `assert_eval_equiv` so the asserted equality is also
   eval-checked. (If extraction does not yield exactly `OR(c0, c1)` because of
   commutative operand ordering, assert membership/equality up to operand order,
   matching how other tests handle variadic results.)

3. **Error preservation when the De Morgan form is extracted.** Build a case where
   the extracted (cheaper) result contains a NON-LITERAL erroring operand, so the
   differential actually exercises the rewritten term over an error row. For
   example use `d = (1 / c0 = 5)` (boolean, non-literal, errors at `c0 == 0`,
   const_fold cannot fire on it) inside a double-negation that De Morgan + not_not
   simplify, e.g. `NOT(AND(NOT d, NOT c1))`, whose canonical form is `OR(d, c1)`.
   Differential `eval(input)` vs `eval(canonicalize(input))` over
   `c0 in {0, 5}` x `c1 in {true, false, null}`, including the `c0 == 0` error
   row. Assert the input actually evals to `Err` at `c0 == 0` so the test proves
   error/short-circuit behavior is preserved by the rewrite, not vacuously passing.
   (Confirm the cost arithmetic actually makes the simplified form win; if it does
   not, pick another small composition that does and document why. The binding
   requirement is: the differential must run over the De-Morgan-derived term with a
   live error row, not over the untouched original.)

No behavior changes to existing rules or tests; the 56 prior tests must still pass
unchanged.

## Constraints (binding)

* Separate scalar engine; touch no relational eqsat code.
* No `as` conversions.
* Comments: no em-dashes, no clause-joining semicolons; the doc comment states the
  contract and the no-gate soundness rationale.
* Reuse `is_and_or`, `switch_and_or`; do not duplicate connective logic.

## Done criteria

* `cargo check -p mz-transform --tests` clean.
* `cargo clippy -p mz-transform --tests` clean.
* `cargo fmt -p mz-transform` applied (`bin/fmt`).
* All prior scalar tests pass; new De Morgan tests (soundness cube, firing,
  error-preservation) pass. Report the new total test count.

Consult the `mz-test` skill for the correct test invocation before running tests.

## Commit

On `claude/mir-equality-optimizer-sodbej`. Subject e.g.
`eqsat: scalar De Morgan rule over AND/OR (Phase 1)`. End with:
```
Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01Lyxbqj9pctLPT2nSTy8DLu
```

## Report

Write your full report to `.superpowers/sdd/scalar-task-1i-report.md` and return
only: status, commit sha, one-line test summary (new total count), concerns.
