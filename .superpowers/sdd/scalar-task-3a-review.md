# Task 3a review: AND/OR associativity-flattening rule (`flatten_assoc`)

Base `72c4530a95` .. Head `f7cfac08de`. Read: brief, implementer report, the
diff, `src/transform/src/eqsat/scalar/rules.rs` (flatten_assoc + neighbours +
factor/absorb + test module + differential generator), `egraph.rs`
(add/find/union/rebuild/nodes/saturate + MAX_ENODES/MATCH_LIMIT/MAX_ITERS),
`src/expr/src/scalar.rs:758` (`flatten_associative`),
`src/expr/src/scalar/func/variadic.rs:80` (And eval) and the `is_associative`
set.

## Spec Compliance / completeness

- ✅ Rule added and registered ONCE (`rules.rs:42`, body `rules.rs:307-365`),
  placed before `factor_and_or` as the report claims.
- ✅ Gate `func.is_associative()` (`rules.rs:311`); `is_associative` confirmed on
  `VariadicFunc` (`variadic.rs:121` for And, plus Or/Coalesce/Greatest/Least).
- ✅ Flat-maps over operands preserving order, splicing same-func inner children,
  keeping non-matches (`rules.rs:316-356`). No sort, order-preserving as required.
- ✅ Reads into owned values (`eg.nodes(canon)` returns an owned `Vec<SNode>`,
  `inner_canons` collected) before any `eg.add` (`rules.rs:321,340,360`). Borrow
  discipline correct.
- ✅ Fires only when something was spliced (`spliced` flag, `rules.rs:357-359`);
  no self-union no-op. See "no-op" analysis below for why `spliced==true` always
  implies a genuinely different vector.
- ✅ `func.clone()` taken after the read loop; single `eg.add` of the flat node
  (`rules.rs:360-364`).
- ✅ Doc comment states the rewrite, UNCONDITIONAL soundness (order-independent
  join, error-by-max, short-circuit), the reduce parity, and the cheaper-extract
  /flat-conjunction rationale (`rules.rs:282-306`).
- ✅ Parity with `reduce`'s `flatten_associative`: reduce compares
  `*inner_func == *outer_func` (full equality incl. the And/Or field); the rule's
  `inner_func != func` (`rules.rs:329`) is the same full-equality test, so the
  scalar rule flattens exactly the chains reduce would, no more, no less.
- ⚠️ The rule fires for ALL associative variadics (And, Or, Coalesce, Greatest,
  Least). This is correct and matches reduce, but the doc comment justifies
  soundness only for And/Or. The Coalesce/Greatest/Least cases are sound (each is
  genuinely order/grouping-independent and reduce flattens them identically), but
  the comment's "And/Or join semilattice" wording undersells the rule's actual
  scope. Minor.

## Termination-guard verdict

**Prevents the reported blowup? YES.** Reconstructed the cycle from the code:
`OR(c0,c0)` --dedup--> `OR(c0)` --`and_or_single`--> `union(class_of_OR(c0), c0)`.
After rebuild, c0's class `K` holds `Column(0)` plus self-referential nodes
`Or([K])` and `Or([K,K])` (canonicalized children point at `K` itself). Without
the guard, processing work item `(K, Or([K,K]))` splices each operand `K` via the
`Or([K,K])` sibling, `K -> [K,K]`, producing `Or([K,K,K,K])`; next pass squares
again (`16`, `256`, ...). Each pass adds exactly ONE new e-node, so `node_count`
rises ~1/iteration and never crosses `MAX_ENODES=600` before `MAX_ITERS=100`; but
the operand vector grows ~`4^k`, so it OOM/timeouts around iteration ~20-30. The
global bounds genuinely do NOT catch this (confirmed by reading `saturate`,
`egraph.rs:339-380`).

The guard (`rules.rs:340-343`) canonicalizes the inner children and skips any
same-func sibling whose `inner_canons.contains(&canon)`. On `(K, Or([K,K]))`:
both `Or([K,K])` (inner_canons `[K,K]`) and `Or([K])` (inner_canons `[K]`)
contain `K`, so both are skipped, no operand matches, `spliced==false`, returns
`vec![]`. Growth stops. The guard catches exactly the direct self-loop that
produces the blowup.

**Sound? YES.** The guard only ever DECLINES to splice; declining keeps the
operand as a direct child, yielding a still-valid And/Or node that is trivially
equal to the input. It can never emit a wrong node. When it does fire, the added
node is congruent to the source by associativity and `union(id, flat)` asserts a
true equality. Skipping a self-referential sibling also asserts nothing false:
that sibling encodes the already-proven idempotence `c0 = c0 OR c0`; not using it
for flattening removes no information.

**Blocks legitimate flattening? NO (no false negatives in the acyclic case).**
For `And(c0, And(c1,c2))` the inner operand's class is the `And([c1,c2])` class;
its children canonicalize to `[c1,c2]`, which does not contain that class
(`c1 != And(c1,c2)`), so the guard does not fire and splicing proceeds. Same for
`And(c0, And(c0, c1))` (inner class is the 2-ary And, distinct from `c0`). The
guard fires only when an inner child IS the operand's own class, which is always a
genuine cycle, never legitimate nesting. Tests `firing_and/or` and `deep_nesting`
confirm normal flattening still works.

**All cycle shapes covered? DIRECT self-loop yes; transitive/2-cycle NOT covered
(residual risk, see Important #1).** The guard checks only one hop
(`inner_canons.contains(&canon)`). A 2-cycle of DISTINCT classes
`A = Or([B, p])`, `B = Or([A, q])` would not be caught: splicing `A -> [B,p]`
then `B -> [A,q]` reintroduces `A`, and the global bounds would again fail to
stop the operand-vector growth. I tried to construct such a cycle from the live
rule set and could not: cycles among same-func variadic nodes are created only by
unions whose output references the source class, and the only rules that do that
for And/Or are `and_or_single` (collapses `OR([x]) -> x`, always a DIRECT
self-loop on `x`) and the flatten/dedup re-adds of multi-copy self nodes (also
direct). `const_fold` cycles go through literal descendants, not same-func nodes.
`factor_and_or`/`absorb_and_or` build the DUAL connective at the top and union
descendants, so they do not manufacture a same-outer-func cross-cycle. So a
transitive same-func cycle appears unreachable with today's rules, but this is by
inspection, not proof, and any future rule that proves `X = f(.., Y, ..)` /
`Y = f(.., X, ..)` for distinct classes would resurrect the timeout. Flagged
Important, not Critical, because I could not demonstrate reachability.

**Termination overall.** In the acyclic case flatten produces strictly flatter
nodes (fewer interior same-func nodes), each hashconsed, bounded by the finite
algebra and `MAX_ENODES`. Self-loop cycles are neutralised by the guard (returns
`vec![]`, no growth). `and_or_single/dedup/factor/absorb` each carry their own
no-op guards, so flatten interacting with them does not introduce new runaway
unions. The one residual non-termination path is the transitive cycle above.

## Strengths

- The termination diagnosis is correct and precise, and the fix targets the exact
  mechanism (operand-vector squaring that escapes a node-count budget). The doc
  comment at `rules.rs:332-339` records the non-obvious reason clearly.
- Re-canonicalizing inner children with `eg.find` before the cycle check
  (`rules.rs:340`) is necessary and correct: mid-pass the stored children can be
  stale, and the parity comparison/cycle check both need current canon ids.
- Full-equality `inner_func != func` exactly mirrors reduce, so the scalar rule
  cannot over- or under-flatten relative to the production optimizer.
- Soundness reasoning (decline-only guard, congruent add) is airtight; the And
  eval (`variadic.rs:80-98`) confirms the result is order-independent even when a
  False co-occurs with an error (False always wins, errors combined by `max`), so
  unconditional flattening is exact-eval sound as the doc claims.
- Tests cover firing (And+Or, asserted flat), deep nesting via saturation,
  error-operand differential, and the no-op / no-cross-splice cases. Non-vacuous
  (they assert operand counts, absence of nested same-func, and eval-equivalence).

## Issues

#### Important

1. **No guard against transitive / multi-class same-func cycles; global bounds do
   not bound operand-vector size.** `rules.rs:340-343` + `egraph.rs:341-348`.
   The guard is one-hop. If a same-func cycle ever spans two distinct classes
   (`A∈B`, `B∈A`), splicing alternates and grows operand vectors with ~1 node
   added per pass, so neither `MAX_ENODES` (node count) nor `MAX_ITERS=100`
   (squaring outruns it) stops the timeout. I could not construct such a cycle
   from the current rule set (all same-func cycles today are direct self-loops
   from `and_or_single`), so this is a latent risk rather than a live bug.
   Fix options, in order of preference: (a) add a defensive absolute cap on the
   produced operand-vector length (e.g. skip the splice / bail if
   `new_exprs.len()` exceeds a constant), which bounds the worst case regardless
   of cycle shape and is cheap; or (b) document in the rule that the guard relies
   on the invariant "same-func cycles are always direct self-loops" and assert it
   so a future rule that breaks the invariant fails loudly rather than hanging.

#### Minor

2. **No deterministic regression test for the termination guard.**
   `rules.rs:1402-1567`. None of the five new tests constructs the cycle-prone
   shape (an AND/OR that collapses via `and_or_single`/`dedup` and is then
   flattened). The shapes use distinct columns (`c0,c1,c2`). The differential
   test (`rules.rs:3000`) *probably* hits it (its generator emits `And`/`Or` of
   2-3 recursively-generated bool operands, and a bool leaf is `col(2)` with prob
   ~1/5, so `AND(c2,c2)` / nested idempotent forms occur across 300 expressions),
   and its passing under 120ms is real evidence the guard works, but it is not
   guaranteed to cover the shape and would not localise a regression. Recommend a
   dedicated test, e.g. `canonicalize(&variadic(or(), vec![variadic(or(), vec![
   col(0), col(0)]), col(1)]))` (inner `OR(c0,c0)` collapses, outer forces
   flatten), asserting it terminates and yields a flat correct result. This locks
   the guard in place.

3. **Doc comment scopes soundness to And/Or, but the rule fires for all
   associative variadics.** `rules.rs:285-297`. Coalesce/Greatest/Least are also
   flattened (and soundly so, matching reduce). A one-line note that the
   unconditional-soundness argument generalises to every `is_associative`
   variadic (reduce parity) would make the comment match the code's scope.

4. **`eg.nodes(canon)` iterates a `HashSet`, so the "first matching sibling"
   choice is not order-stable across runs.** `rules.rs:321-348`. If a class holds
   two same-func nodes with different (but congruent) operand lists, which one is
   spliced varies. This is sound (both are equal to the source) and pre-existing
   (`inner_sets`, `and_or_single` do the same), and extraction's min-cost makes
   the final output stable, so no action needed. Noting for completeness.

No prior test assertion was weakened (diff is purely additive, 258 insertions,
no deletions).

## Assessment

**Task quality: Approved.**

Reasoning: The termination guard correctly and soundly eliminates the only
same-func cycle the current rule set can produce (the direct self-loop from
`and_or_single`), the rule itself is order-preserving, reduce-faithful, and
unconditionally exact-eval sound, and the tests are non-vacuous. The two
follow-ups worth doing before this lands more broadly are a defensive
operand-length cap (or an asserted invariant) for the unproven transitive-cycle
case, and a deterministic regression test for the cycle shape, but neither is a
confirmed defect against today's code.
