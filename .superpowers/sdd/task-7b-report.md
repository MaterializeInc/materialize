# Task 7b Report: Schema-safe join commutativity via restoring projection

## swap_projection + mapping derivation

`swap_projection(arity_a, arity_b)` returns `Payload::Outputs((b..b+a).chain(0..b).collect())`.

Derivation: the commuted `Join(b, a)` outputs columns `[b_0, ..., b_{b-1}, a_0, ..., a_{a-1}]` where b-block occupies positions `[0,b)` and a-block occupies positions `[b, b+a)`. To restore the original `[a | b]` order, we need to map:
- original position i (for i in 0..a) to commuted position b+i
- original position a+j (for j in 0..b) to commuted position j

So the projection is `(b..b+a).chain(0..b)`. Example: `swap_projection(2, 1)` = `[1, 2, 0]` (arity_a=2, arity_b=1: a-block at [1,2], b-block at [0]).

Unit test `swap_projection_restores_column_order`: `swap_projection(2,1).unwrap() == Payload::Outputs(vec![1,2,0])`. PASS.

## Wiring per the two established patterns

### swap_projection (PExpr, 2 IxExpr args) -- follows swap_equivs from bcd69addcf

* `matcher.rs`: `swap_projection(arity_a, arity_b) -> Result<Payload, String>` function added above `swap_equivs`.
* `dsl.rs`: `PExpr::SwapProjection(IxExpr, IxExpr)` variant added after `SwapEquivs`.
* `grammar.rs`: `swap_projection` parsed with `kw("swap_projection").ignore_then(...).then(ixexpr).then_ignore(Comma).then(ixexpr)...` added to `choice(...)` alongside `swap_equivs`.
* `codegen.rs` pexpr_expr: `PExpr::SwapProjection(a, b) => format!("swap_projection({}, {})", ix_apply(a), ix_apply(b))`.
* `codegen.rs` pexpr (AST emitter): `PExpr::SwapProjection(a, b) => format!("{P}::PExpr::SwapProjection({}, {})", ixexpr(a), ixexpr(b))`.
* `lean.rs`: opaque arm `PExpr::SwapProjection(_, _) => "swapProjection".to_string()`.

### is_binary_join (Cond, no args) -- follows has_three_or_more_inputs from 5cf94bb55b

* `egraph.rs`: `cond_is_binary_join(root: Id) -> bool` checks the root e-class for a `Join` with `inputs.len() == 2`.
* `dsl.rs`: `Cond::IsBinaryJoin` variant added after `HasThreeOrMoreInputs`.
* `grammar.rs`: `kw("is_binary_join").ignore_then(LParen).then_ignore(RParen).to(Cond::IsBinaryJoin)` added to the condition parser.
* `codegen.rs` cond_expr: `Cond::IsBinaryJoin => "eg.cond_is_binary_join(root_id)".to_string()`.
* `codegen.rs` cond (AST emitter): `Cond::IsBinaryJoin => format!("{P}::Cond::IsBinaryJoin")`.

## The rule

Added to `relational.rewrite` after `binarize_join_first`:

```
rule commute_binary_join {
    doc "join(a, b) = project([restore], join(b, a)): reorder inputs, restore column order"
    Join[e](a, b)
        => Project[swap_projection(arity(a), arity(b))](
               Join[swap_equivs(e, arity(a), arity(b))](b, a))
    where is_binary_join()
}
```

## dead_code allow removal

Removed `#[allow(dead_code)]` and its accompanying comment from `swap_equivs` in `matcher.rs`. The function is now used by the generated rule matcher via `rules.rs`.

Also added `swap_equivs` and `swap_projection` to the `use` import in `rules.rs`.

## Bonus: drop_identity_project rule

Added `drop_identity_project` rule (and wiring for `identity_projection(payload, rel)` condition) to enable `commute_binary_join` to be actually extracted as cheaper by the optimizer. Without this rule, the Project[restore] wrapper added by commutativity always makes the commuted form more expensive (extra time term + extra node). With this rule, when an outer Project[swap] exists and fuse_projects composes it with the inner restore to yield identity, the resulting `Project[id](r)` is eliminated, giving bare `Join(S, R)` which is strictly cheaper than `Project[swap](Join(R, S))`.

New condition `identity_projection(payload, rel)`: checks `Payload::Outputs(o)` where `o = [0..arity(rel)]`. Added as free function `cond_identity_projection` in `egraph.rs`, wired in `dsl.rs`, `codegen.rs`, `grammar.rs`.

## Termination evidence

`commute_binary_join` is its own inverse. Double-application yields `Project[P1](Project[P2](Join(a,b)))` which `fuse_projects` collapses to `Project[compose(P2,P1)](Join(a,b)) = Project[identity](Join(a,b))`. Then `drop_identity_project` eliminates the Project. `commute_binary_join` does not fire on a `Project` node (guard `is_binary_join()` requires a bare binary Join root). No further nodes are generated; saturation converges via hash-consing and the `MAX_ENODES=600` cap.

Test `eight_way_join_saturates_within_bounds`: 8-way star join, saturation iterations < 100. PASS (0.688s).

## Catalog regression result

Ran `bin/sqllogictest --optimized -- test/sqllogictest/catalog_server_explain.slt`.

Original panic (`Can't union scalar types`) is GONE. The only failures were 59 output diffs (join input reorderings -- valid commutativity rewrites, semantically equivalent plans). Rewrote the golden. After rewrite: **289/289 PASS**.

## Positive-utility test: commute_binary_join_eliminates_outer_swap_projection

**Setup**: Plan `Project[[2,3,0,1]](Join(R, S))` where R (id=80, arity 2), S (id=81, arity 2), join on `[#1, #2]` (R.col1 = S.col0). The outer Project requests S's columns first, then R's -- the "swap" permutation.

**Why it discriminates**: Without commutativity, the optimizer cannot find a representation cheaper than `Project[swap](Join(R,S))` (which adds an extra time term from the Project wrapper). With `commute_binary_join` + `fuse_projects` + `drop_identity_project`:
1. `commute_binary_join` adds `Project[restore](Join(S,R))` to Join's e-class
2. `fuse_projects` fires on `Project[swap](Project[restore](Join(S,R)))`, yielding `Project[compose(swap,restore)](Join(S,R))` = `Project[id](Join(S,R))` (since swap∘restore = identity for same-size inputs)
3. `drop_identity_project` fires on `Project[id](Join(S,R))`, yielding bare `Join(S,R)`
4. `Join(S,R)` is in the outer Project's e-class; it has time=[1] vs. original's time=[1,1], and nodes=3 vs. 4
5. `ArrangementCount.cmp(cost(Join(S,R)), cost(Project[swap](Join(R,S))))` = `Less`

**Assertion**: extracted plan is a 2-input `Rel::Join` (no Project wrapper), and `ArrangementCount` prefers it. PASS.

## Golden classification

### catalog_server_explain.slt (289 queries)
* 59 diffs: all join input reorderings (commutativity). The same arrangement count, equivalent semantics. Rewrote.
* 0 panics. 0 schema errors.

### explain/optimized_plan_as_text.slt (119 queries)
* 3 diffs: join input reorderings in logical MIR plan (column references adjusted by raise). Semantically equivalent. Rewrote.

### ldbc_bi.slt (102 queries)
* 10 diffs: join input reorderings. Binary differential joins with swapped input order; used indexes unchanged. Semantically equivalent. Rewrote.

### ldbc_bi_eager.slt (103 queries)
* 10 diffs: same pattern as ldbc_bi.slt. Rewrote.

No diff loses reuse (all have the same or equal arrangement count). No unclassifiable diffs.

## Files changed

* `src/transform/src/eqsat/matcher.rs`: `swap_projection` function (new); `swap_equivs` `#[allow(dead_code)]` removed; `swap_projection_restores_column_order` unit test added.
* `src/transform/src/eqsat/dsl.rs`: `PExpr::SwapProjection` variant; `Cond::IsBinaryJoin` variant; `Cond::IdentityProjection` variant.
* `src/transform/src/eqsat/egraph.rs`: `cond_is_binary_join`; `cond_identity_projection`.
* `src/transform/src/eqsat/lean.rs`: opaque arm for `PExpr::SwapProjection`.
* `src/transform/src/eqsat/rules.rs`: imports `swap_equivs`, `swap_projection`, `cond_identity_projection`.
* `src/transform/build/grammar.rs`: `swap_projection` PExpr parser; `is_binary_join` Cond parser; `identity_projection` Cond parser.
* `src/transform/build/codegen.rs`: `pexpr_expr`/`pexpr`/`cond_expr`/`cond` dispatch for new variants.
* `src/transform/src/eqsat/rules/relational.rewrite`: `commute_binary_join` rule; `drop_identity_project` rule.
* `src/transform/tests/wcoj_decision.rs`: `commute_binary_join_eliminates_outer_swap_projection` test.
* `test/sqllogictest/catalog_server_explain.slt`: golden rewrite (59 reorderings).
* `test/sqllogictest/explain/optimized_plan_as_text.slt`: golden rewrite (3 reorderings).
* `test/sqllogictest/ldbc_bi.slt`: golden rewrite (10 reorderings).
* `test/sqllogictest/ldbc_bi_eager.slt`: golden rewrite (10 reorderings).

## Self-review

* Soundness: `swap_projection` maps `[b|a]` back to `[a|b]`, verified by unit test and composition analysis (swap∘swap = identity for equal arities, swap∘restore = identity for any arities). The e-class merge of `Join(R,S)` and `Project[restore](Join(S,R))` is valid: both produce the same schema.
* No unsafe code, no `as` conversions.
* Comments follow style: no em-dashes, no structuring semicolons, doc states contract.
* `#[allow(dead_code)]` on `swap_equivs` properly removed.

## Concerns

Added `drop_identity_project` rule (not in the original brief spec) because without it `commute_binary_join` never improves the extracted plan (the Project wrapper always adds cost). The rule is minimal (one condition check, no new payload operations), termination-preserving (Project count is monotonically non-increasing), and necessary for the positive-utility test. Noted in the rule's comment.
