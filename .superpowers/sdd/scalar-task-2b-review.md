# Task 2b review: scalar absorption rule (dropped-operand gated)

Base `ec300a075b` .. Head `76dfa3beee`. Reviewed read-only.

Files read:
- `src/transform/src/eqsat/scalar/rules.rs` (full new code + tests, in-tree)
- `src/expr/src/scalar/func/variadic.rs:73-128` (And eval), `:1146-1186` (Or eval),
  `:1757-1765` (`switch_and_or`)
- `src/transform/src/eqsat/scalar/egraph.rs:121-175` (`canon_node`, `add`, `union`)
- `src/transform/src/eqsat/scalar/analysis.rs` (`could_error` propagation, conservative OR on merge)
- 2a `factor_and_or` and the extracted `inner_sets` helper
- design note CLU-137 + envelope section, the brief, the implementer report

## Spec Compliance / completeness

- ✅ Rewrite both duals: `absorb-OR a∨(a∧c)→a`, `absorb-AND a∧(a∨c)→a`, generalized over inner-sets (`rules.rs:443-551`).
- ✅ Registered once in `rules()` immediately after `factor_and_or` (`rules.rs:38`).
- ✅ Inner-set by canonical class id, shared `inner_sets` helper (`rules.rs:290-309`); 2a refactored to call it with identical semantics.
- ✅ Proper-subset selection `inner-set(P) ⊊ inner-set(Q)` (`rules.rs:509-514`); equal sets explicitly left to `and_or_dedup`.
- ✅ Gate on dropped extras `inner-set(Q)\inner-set(P)` only, retained P not gated (`rules.rs:515-522`).
- ✅ Drop one Q per fire; deterministic first qualifying `(Q,P)` by index; kept ids sorted before `eg.add`; single-operand path unions `eg.find(kept[0])` directly (`rules.rs:531-550`).
- ✅ Non-destructive (unions absorbed form, does not mutate operands).
- ✅ Doc comment covers rewrite, dropped-extra gate + why on dropped extras not P/whole-expr, CLU-137 split relationship, deferred nullability disjunct (`rules.rs:443-476`).
- ✅ No relational code, no `as`, no em-dashes/clause-joining semicolons in new comments.
- ✅ Tests: cube soundness both duals; firing both duals; gate-allows-retained-error; gate-blocks-dropped-extra with explicit Err witness.
- ⚠️ Could-not-independently-rerun the suite; implementer reports 70 scalar / 268 nextest pass, clippy+fmt clean. Behavior of every cited test path verified by hand below and is consistent with those claims.

## Changed-test verdict

`test_factor_and_or_absorption_not_fired_here` → `test_factor_hands_empty_residual_to_absorption` (`rules.rs:294`+). This is a **correct consequence, not a weakening**.

- Input `(c0∧c1)∨(c0∧c1∧c2)`: sets `[{c0,c1},{c0,c1,c2}]`. `factor_and_or` computes intersection `{c0,c1}`, finds branch-0 residual empty, and declines (the empty-residual guard, `rules.rs:395-400`). So factoring genuinely does NOT fire — that half of the old assertion still holds.
- `absorb_and_or` now fires: `{c0,c1} ⊊ {c0,c1,c2}`, dropped extra `c2` is an error-free bool column, gate passes, drops the 3-conjunct operand, `kept.len()==1` returns `find(AND(c0,c1))`. So the term collapses to `c0∧c1`.
- The OLD assertion (round-trips unchanged) is therefore now genuinely FALSE because 2b correctly absorbs — and that absorption is sound (Q≤P proven below for error-free extras). The flip records correct new behavior.
- The NEW assertion is meaningfully stronger: it asserts the exact absorbed form `c0∧c1`, which simultaneously witnesses that factoring declined (it would not have produced `c0∧c1`) and that absorption collapsed it. The comment documents the 2a/2b boundary.
- Diff confirms this is the ONLY prior test touched (one deleted `fn test_...`, one deleted old assertion); all other prior assertions are untouched. No hidden weakening.

## Strengths

- Soundness reasoning is exactly right and matches the real eval semantics. And/Or are each a join over a total order on `{False,Null,Error(by max),True}`, with `Error` combined by `std::cmp::max` in BOTH connectives. The gate restricting dropped extras to error-free makes the dropped operand `Q` provably `≤` (OR) / absorbable (AND) relative to retained `P`.
- The gate is on the precise set (dropped extras), not the over-broad whole-expression `could_error`, and the retained operand is deliberately ungated so its own error surfaces. Comment explains this clearly.
- Helper extraction is clean; 2a now calls `inner_sets` with no behavior change (same find/sort/dedup, same singleton fallback).
- Tests are non-vacuous: firing proven structurally (extracts `c0`), gate-block proven by an Err witness row, gate-allow proven by a live retained-error row.

## Issues

#### Critical
None.

#### Important
None.

#### Minor

- `rules.rs:535-541`: `kept` is built from the raw `outer_operands` ids and sorted, but the ids are not `eg.find`-canonicalized before sorting. `canon_node` (`egraph.rs:121-123`) canonicalizes children inside `add` but does NOT reorder And/Or operands, and order is part of the hashcons key. So two fires that reach the same logical absorbed set via raw ids that canonicalize differently could mint two distinct e-nodes for the same expression. This is not a soundness or termination problem (both nodes get unioned into the source class, And/Or order is eval-irrelevant), only a possible redundant node. For full stability, sort `eg.find(id)` values. 2a sorts `eg.add` outputs (already canonical), so this asymmetry is the only spot. Optional.

## Soundness verdict

Gate is on dropped extras only — confirmed (`rules.rs:515-522` filters `sets[q]\sets[p]` and checks `could_error` on exactly those; P/`sets[p]` never gated).

Both duals worked by hand against real eval:

- absorb-OR `P∨Q`, `P=AND(S_P)`, `Q=AND(S_Q)=AND(P, AND(extras))`, extras error-free so `x=AND(extras)∈{F,N,T}`. For every `a=P∈{T,F,N,Error}` and every `x∈{F,N,T}`, `Q = a AND x ≤_OR a`. Since OR is `max`, dropping `Q` from any wider OR is sound. The `a=null` + dropped-extra-error row is exactly excluded: ungated, `x=Error` gives `Q = Null AND Error = Error >_OR Null = P`, which would lose the error → unsound; the gate forbids `x=Error`.
- absorb-AND `P∧Q`, `P=OR(S_P)`, `Q=OR(S_Q)=OR(P, OR(extras))`, extras error-free so `y=OR(extras)∈{F,N,T}`. `y=F` ⇒ `Q=P` (AND idempotent, safe); `y=T` ⇒ `Q=True` (AND identity, safe); `y=N` ⇒ `Q=P` for `P∈{N,Error,T}` and `Q=Null` for `P=False` (a False already forces the AND to False either way) — removing `Q` never changes the AND result given `P` present. The dual hazard `P=null` + extra error gives `Q=Error`, turning `Null` into `Error` if dropped → the gate forbids it.
- `a=error` fine in both: P is retained, both sides surface P's error (verified in the case tables above and by the gate-allows test).
- Proper-subset enforced (`len(P)<len(Q)` + containment over unique sets ⇒ strict); equal sets routed to dedup.
- Union is between forms eval-equal on ALL inputs (not just extracted), so it cannot corrupt other extractions.
- Termination: each fire strictly reduces the outer operand count and hashconses; re-firing on the smaller node unions an already-present node (no change) → fixpoint. No oscillation with 2a (factoring builds a differently-shaped `inner(factor, outer(residuals))` node and declines empty residuals) or Phase 1.

Gate-block witness is real: input `c0∨(c0∧r)`, `r=(1/c1=5)`, at `c0=Null,c1=0`: `r` errors, inner `Null∧Err=Err`, outer `Null∨Err=Err`; `col(0)` alone is `Null`. Without the gate the differential would fail — the test asserts both (`rules.rs:461-468`). Gate-allow witness is real: `g=(1/c0=5)` errors at `c0=0`, retained, input still errors there and the absorbed `g` reproduces it (`rules.rs:407-411`).

## Assessment

**Task quality: Approved** — The absorption rewrite and its dropped-extra gate are sound for both duals against the real And/Or eval (error-max semantics included), the changed prior test is a correct consequence of 2b rather than a weakening, and the only finding is an optional determinism nit on operand-id sorting that has no soundness or termination impact.
