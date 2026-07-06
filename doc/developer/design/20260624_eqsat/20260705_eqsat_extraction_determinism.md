# Eqsat extraction determinism

Status: in progress (2026-07-05). Landing as a task series on branch
`claude/mir-equality-optimizer-sodbej`. This document records the motivation,
the root-cause investigation, the fix, the alternatives weighed, and enough
method to reproduce the diagnosis and the result.

## Motivation

The eqsat physical optimizer produces a different optimized plan for the same
SQL across two separate processes for a subset of queries. Measured at the
series' starting commit (`ae7ca1d8`, the ILP cycle-aware extraction landed but
before this work): two isolated runs of the same sqllogictest file diverge on
freshmart 4/36 (~11%) and catalog_server_explain 84/292 (~29%) of queries.

This blocks two things:

1. Regenerating the plan goldens. A golden captured in one process fails in the
   next, so the suite would flake in CI.
2. The "plan-determinizing" premise of the refinement-loop fix that precedes
   this work. That fix makes the refinement loop settle deterministically, but
   the plan it settles on is itself process-dependent for cost-tied large
   plans, so the guarantee is only as strong as extraction determinism, which
   was absent.

Determinism here means cross-process: the same input under a different process
(hence a different `std::collections` hash seed) must yield the same plan. It
does not mean insertion-order invariance. Plans stay coupled to insertion and
merge order, so a future refactor that legally changes that order may move
goldens once. That is acceptable churn, not flake. The decoupler (a
content-keyed extraction tie-break) is discussed under Alternatives.

## Investigation

### The solver is deterministic, so the input order is the suspect

The ILP extractor selects a plan with microlp, a simplex solver reached through
good_lp. microlp's dependencies are `log`, `sprs`, `web-time`, with no `rand`,
so it is a deterministic simplex: identical input matrix yields identical
solution. Per-process divergence therefore has to come from the solver's input
differing across processes, which is the variable and constraint order. That
order traces back through the extractor's `BTreeMap`/`BTreeSet` index maps to
the e-class ids assigned during saturation.

The greedy extractor does not diverge because its cost-tie-break resolves each
scalar id to its `EScalar` content through the scalar cache, so it picks by
content, which is invariant to how classes are numbered. The ILP hands a
cost-tie to microlp, which pivots by column order, which is id-dependent. That
asymmetry is why enabling the ILP exposed the divergence that greedy masked.

### Probe B: hash the structure the extractor sees, across two processes

The decisive experiment instruments `IlpExtractor::solve` to hash `reachable`
(the `BTreeMap<Id, Vec<ENode>>` of the subgraph the extractor optimizes) with a
fixed-seed `DefaultHasher`, and prints one line per solve. Run the same file
twice in separate processes, then diff the sorted multiset of hashes.

- Identical multisets would mean the id numbering is deterministic and any plan
  divergence is downstream in good_lp/microlp.
- Differing multisets mean the id numbering itself differs across processes, so
  the root is upstream in saturation.

Result on freshmart: 318 distinct reachable-structures per run, 170 shared, 148
divergent (47%). The signature is diagnostic: the divergent set has median 43
nodes and every one of the 148 carries congruence merges (nodes > classes); the
shared set has median 2 nodes and 143 of 170 are flat (nodes == classes, no
merges). Merges correlate perfectly with divergence.

To reproduce the probe: add, in `extract.rs::solve` after the `total_nodes`
size-cap check,

```rust
{
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    format!("{:?}", reachable).hash(&mut h);
    eprintln!("ILP_REACHHASH {:016x} nodes={} classes={}",
              h.finish(), total_nodes, reachable.len());
}
```

then `bin/sqllogictest --optimized -- test/sqllogictest/freshmart.slt 2> run1`
twice into `run1`/`run2`, and diff `grep ILP_REACHHASH runN | sort | uniq -c`.
Remove the probe afterward.

### Attribution

Pre-existing, latent in the ILP path, exposed by enabling the ILP. Before the
cycle-aware ILP landed, the ILP bailed to greedy on every cyclic join fragment,
so the committed goldens are effectively greedy-generated and the ILP path
never ran on them in CI. "The goldens pass today" was therefore never evidence
that ILP extraction is deterministic. The refinement strict-improvement and
round-cap fixes are not the cause: they do not touch the ILP path.

## Root cause

E-class ids are allocated by a monotone counter (`core::EGraph::new_class` uses
`uf.len()`), which is deterministic. Canonicalization, though, is hash-order
dependent, through two mechanisms that both read `EGraph::classes`, which was a
`std::collections::HashMap<Id, HashSet<Node>>`.

1. rebuild survivor selection (`core.rs`, `rebuild`). rebuild iterates
   `self.classes.keys()` (hash order) and each class's `HashSet<Node>` (hash
   order). On a congruence duplicate it calls `union(other, rep)`, and `union`
   makes the survivor the first-encountered rep (`self.uf[loser] = winner`). So
   which id becomes a merged class's canonical representative depends on the
   per-process hash seed.

2. saturation match and truncation order (`egraph/saturate.rs`). Matches are
   collected by walking the `rel_index` per-`Sym` `Vec`s, whose order derives
   from `rel_class_ids()` = `classes` iteration order, and are then applied in
   that order. The `MAX_ENODES` budget break mid-apply makes the truncation
   point itself order-dependent.

`reachable` is keyed by `find` (canonical id), so different representative ids
produce a different `BTreeMap<Id, _>` key order, which is the ILP's variable and
column order, which lands microlp on a different cost-equal vertex, which is a
different plan. This is why only merged (nodes > classes) subgraphs diverge:
without a congruence merge the canonical id is just the allocation counter, so
it is identical across processes.

## The fix

Convert every `std::collections` hash map and set in the eqsat module to a
container whose iteration is either sorted or statically absent.

- Iterated maps become `BTreeMap`/`BTreeSet`. The load-bearing one is
  `EGraph::classes: BTreeMap<Id, BTreeSet<Node>>`. `Node` already derives `Ord`.
  This single change makes rebuild visit classes and nodes in sorted order
  (deterministic survivor), makes `rel_class_ids` sorted (deterministic match
  and truncation order), and makes `reachable`'s keys deterministic canonical
  ids.
- Keyed-only maps become `mz_ore::collections::HashMap`/`HashSet`, the
  non-iterable wrapper. `EGraph::memo` is the hash-cons hot path and is never
  order-iterated, so it stays a hash map for O(1) lookup but on the wrapper. The
  wrapper removes `.iter()/.keys()/.values()/IntoIterator`, so the compiler
  proves the classification: a keyed-only map compiles against it, a genuinely
  iterated one fails to compile and must become a `BTreeMap`.

### Node: Ord is a substrate contract

The `Language::Node: Ord` bound already existed. It is documented as a
determinism contract on the trait, not the concrete language impls, because
deterministic canonicalization requires a total order on nodes, and a trait is
where such a contract belongs. Placing it on a concrete impl would leave a
future language free to reintroduce the nondeterminism structurally.

### Compiler-guided sweep and the lint gate

The whole eqsat module (~184 std-map sites across ~22 files) is swept, not just
`classes`. Partial conversion would risk residual flakes from the colored or
analysis paths, each costing a full regen plus two-process settle cycle to even
detect. The wrapper's compile-error-on-iteration turns "did I catch every site"
from judgment into type-checking, but only if the sweep is complete.

The module carried a `#![allow(clippy::disallowed_types, ...)]`
(`src/transform/src/eqsat.rs`) with a comment claiming the repo-wide std-hash
ban "does not apply to the ported engine." The determinism bug falsifies that
rationale. The sweep removes `clippy::disallowed_types` from the allow and
rewrites the comment, so a clean `cargo clippy` is the completeness oracle: any
remaining std hash map or set fails the lint.

## Latent bugs the fix exposed

Making iteration deterministic surfaced two pre-existing bugs that hash
randomness had masked. Both are exposed, not caused, by the fix.

### Colored union-find survivor pick (fixed in the colored sweep)

`colored/union_find.rs` stored `parent` in a std `HashMap<Id, Id>`. On removing a
root, it collects the root's children into a `Vec` and promotes the first as the
new root. Under hash order the promoted child was process-dependent, so the
colored canonical representative was nondeterministic. Converting `parent` to a
`BTreeMap` makes the child list ascending-id sorted, so the promoted root is
process-independent. A sibling site, `DerivedScopes.class_scope`, has the same
shape (two pre-rebuild classes can canonicalize onto one id, and a `collect`'s
last-write-wins picks the survivor); it is closed by `class_scope` being a
`BTreeMap`.

### flatten x dedup non-termination

Making iteration deterministic surfaced a pre-existing non-termination that
hash randomness had masked. It is exposed, not caused, by the fix.

The scalar rewrite `flatten_and` (associativity) and `and_dedup` (idempotence)
are mutually non-confluent. flatten splices `and(c0, and(c0,c1))` into the
operand list `[c0, c0, c1]`; `and_dedup` merges `and(c0,c0,c1) == and(c0,c1)`
into that class; the next round flatten finds the wider member of the class and
splices one more `c0`, giving `[c0,c0,c0,c1]`, then `[c0,c0,c0,c0,c1]`, one
extra operand and one extra e-node per iteration. `FLATTEN_MAX_OPERANDS = 4096`
caps this only after ~4096 iterations, far past the scalar loop's
`MAX_ITERS = 100`. Under random hash order the loop sometimes reached a
no-change round and stopped; under deterministic order it never does. In
production (through `canonicalize_combined`) `MAX_ITERS` caps it, so the effect
is wasted iterations and a bloated non-canonical scalar rather than a hang, but
it is still a real regression.

Fix: `rest_flatten` dedups its output for And and Or only, by canonical id,
keeping first occurrence and preserving order. Those are the only connectives
with a dedup rule (`and_dedup`/`or_dedup`) and so the only ones that can grow
this way. flatten is also wired for coalesce, greatest, and least, but those
have no dedup partner, so they hash-cons to a fixpoint without growth and are
left untouched (deduping them would assert a canonical-form change no rule
declares).

The dedup reuses the existing `rest_dedup_by_id` helper, whose semantics
(first-occurrence, canonical id, no sort) exactly match the `dedupById` Lean
model. Removing a later duplicate is error-safe: the first occurrence still
evaluates and surfaces the same error, and `and_dedup` is itself declared
unconditional (no could-error gate), so the system already treats duplicate
removal as within the error envelope. Reordering, which would change which
error surfaces, is not done.

Lean accounting: flatten-for-and/or and dedup are both already proven
fold-invariant (`denoteSFold_{and,or}_flatten` and `denoteSFold_{and,or}_dedup`
in `Semantics.lean`). The `lean.rs` render for and/or wraps the flatten term in
`dedupById`, and `choose_proof` discharges the composed obligation by chaining
the two existing lemmas via `Eq.trans`/`Eq.symm`. No new mathematics, and
`Semantics.lean` is not edited. The Lean checker is Docker-gated and was not run
in this environment; the composition was traced by hand and by review.

## Alternatives considered

- classes-only, then measure. Convert just `EGraph::classes`, re-run Probe B,
  and extend only if it still diverges. Faster to a first landing, but leaves
  the guarantee empirical rather than structural and risks extra regen cycles
  to discover residual sites in the colored or analysis paths. Rejected in
  favor of the full compiler-guided sweep, which is mechanical and makes
  completeness a compile/lint property.
- Content-keyed ILP extraction tie-break (the decoupler). Give the ILP a
  deterministic tie-break so a cost-tied vertex is resolved by content, the way
  greedy already is. This would make plans invariant to insertion and merge
  order, not merely deterministic per process. Deferred, not adopted here: the
  root fix removes the divergence, and a content tie-break is insurance against
  a future refactor's one-time golden churn. An earlier rejection of "Ord-min
  in the ILP" was against it as the refinement-flap fix (second solves were dear
  at ~47k solves per run, idempotence-under-resaturation was shaky); those
  objections do not bind golden stability, which needs determinism per input,
  not idempotence, and at the post-fix volume (~1.9 solves per optimize) a
  lexicographic second phase is affordable if we ever want it.
- flatten fix at the DSL level (`dedup(flatten(xs, and))`). The DSL's rest
  builtins take a named list, not a nested builtin, so a composition is not
  expressible in the DSL grammar. The fix is Rust-side in `rest_flatten` plus a
  Lean render that composes the two proven builtins.
- flatten novelty fire-guard (do not fire when the result is congruent to an
  existing node). More general but a whole-engine behavioral change to the
  fire/apply path to solve one rule family's canonical-form problem. Rejected on
  layering grounds.

## Reproduction and validation

- Probe B (see Investigation) reproduces the diagnosis and is the acceptance
  gate for the root fix: after the sweep, the two-process reachable-hash
  multisets for freshmart must be identical.
- Corpus settle-once. Because the sweep changes match and apply order, the
  truncation point, and survivor selection, the saturated e-graph itself differs
  from the prior hash-order build, so greedy-path plans can move too, not only
  ILP files. The settle audit therefore sweeps the whole eqsat golden corpus:
  regenerate each file with `--rewrite-results` in isolation (a single
  multi-file process bloats the catalog and fabricates moves), then re-run each
  moved file in a fresh process and require zero diffs.
- Perf gate (hard). `BTreeSet<Node>` insert and lookup are O(log n) with
  full-node `Ord` versus O(1) hash; the hash-cons hot path stays on the wrapper,
  so the exposure is the per-class set operations. The catalog EXPLAIN worst-
  case per-query optimize time and the E0 optimize-time gate must hold.

## Task series

1. `classes` to `BTreeMap`/`BTreeSet` plus the `Node: Ord` contract doc and a
   sorted-iteration contract test. (landed, reviewed)
1.5. flatten x dedup termination fix. (landed, reviewed)
2. `core.rs` keyed-only maps to the wrapper, `run_analysis` to `BTreeMap`.
   (landed, reviewed)
3. `egraph/` sweep. (landed, reviewed)
4. `colored/` sweep, including the union-find survivor fix. (landed, reviewed)
5. consumer and remainder sweep. (landed, reviewed)
6. remove the lint escape; `cargo clippy -p mz-transform` green is the
   completeness gate. (landed, clippy green)
7. corpus-wide two-process settle and golden regen. (in progress)
8. E0 and catalog-timing perf gate. (pending)

Each task ran the whole `mz-transform` unit suite before committing, not just
the eqsat subset, since a determinism sweep can expose latent order-dependent
bugs elsewhere (it exposed the flatten one). The suite stayed green (403 passed,
3 pre-existing ignored) throughout.

## Results

- Determinism acceptance (Probe-B-equivalent). freshmart, the 11% case,
  two-process settle-once (rewrite in process A, re-verify in fresh process B)
  is zero-diff after the sweep. Cross-process determinism achieved on the file
  that motivated the diagnosis.
- clippy completeness gate: `cargo clippy -p mz-transform` green with
  `clippy::disallowed_types` re-enabled on the module, so no std hash map or set
  remains in eqsat.
- Perf: the sweep is perf-neutral, proven by profile. A samply capture of the
  chbench 7-way-join query (which the regen surfaced as a 54s optimize) analyzed
  in pollard shows 95.5% of the optimize is `microlp::Problem::solve` under
  `IlpExtractor::solve`, with `add_constraint` at 87.8% and the hot self-time all
  in microlp/sprs simplex internals (`tri_solve`, `lu_factorize`, `pivot`,
  `restore_feasibility`). There are no e-graph, `BTreeSet`, `CNode::cmp`, or
  `rebuild` frames in the hot path. The `BTreeSet<CNode>` container change does
  not show up in the profile.
- Corpus regen and settle. With the cyclic-ILP gate in place, the full corpus
  regen (196 EXPLAIN-bearing files, each rewritten in its own process) completed
  with zero timeouts. 25 files moved, and all 25 pass settle-once (each moved
  file reproduces with zero diff in a fresh process), so the regenerated goldens
  are cross-process deterministic. ArrangeBy counts are near-neutral (most files
  unchanged, a few plus or minus one). The larger shifts are delta versus
  differential join markers, and for the big-join files these move the plans
  closer to the merge-base (for example chbench delta markers: base 13, the
  drifted committed golden 4, the regenerated golden 12), consistent with the
  gate producing greedy, directional-like join plans and undoing prior branch
  drift.
- Unit suite with the gate. The gate bails the cyclic ILP to greedy, so the four
  `eqsat::extract` tests that exercise the cyclic subtour-cut machinery
  (`ilp_handles_join_commutativity_cycle`, `ilp_size_{one,two,three}_*`) are
  shelved with `#[ignore]` and a note pointing back to the gate. The rest of the
  `mz-transform` suite is green. Un-ignoring those tests and removing the gate go
  together.

## Adjacent finding and fix: cyclic-join ILP is unboundedly slow

The corpus regen surfaced a pre-existing problem unrelated to determinism, but
one that had to be fixed for the regen to proceed. The cycle-aware ILP extractor
(the MTZ subtour-elimination encoding for cyclic join commutativity, landed
before this work under `enable_eqsat_ilp_extraction`) spends 54s solving the
MILP for chbench's 7-way delta join, past the 60s statement timeout. Because the
eqsat feature flags are on in tests, the big-join sqllogictest files
(chbench, ldbc, tpch) do not just regenerate slowly, they fail at HEAD with a
statement timeout. That is a branch breakage the determinism regen had to clear.

### Why the obvious gates do not work

The instrumented data refutes a size-based gate. Across the chbench queries the
ILP programs are small (<= 110 binary variables) and every cyclic SCC is size 2,
so the expensive MTZ big-M path is never even used. Solve time does not track
size: two 91-variable programs took 1.4s and 6.3s, a 4x spread at identical size,
and the times form a continuum from 300ms to 21.9s with no gap. The hardness is
intrinsic to each program's branch-and-bound search, not its size, so no
variable or constraint count cleanly separates fast from slow.

A wall-clock time limit (microlp exposes one through good_lp's `with_time_limit`)
is also rejected, for a determinism reason specific to this module's goal:
bailing on elapsed time makes the chosen plan depend on how fast the solver ran,
so the same query yields an ILP plan on a fast machine and a greedy plan on a
slow one. That is cross-machine golden nondeterminism, the same class of bug this
work otherwise eliminates, so it cannot be used to pick a plan that gets frozen
into a golden.

### The gate

The only deterministic, machine-independent, guaranteed-bounded option is a
structural gate: bail to the greedy extractor whenever the reachable subgraph is
cyclic (`extract.rs`, right after `cyclic_classes`). Greedy is the sound fallback
the ILP already uses on the size cap, and its content-keyed tie-break is
process-independent, so the plan stays deterministic. Because the committed
goldens predate the cycle-aware ILP (they are greedy plans), this gate also
restores them, so join files see no golden move and the determinism moves stay
isolated to non-join cost-tie files.

The cost is that the cycle-aware ILP no longer runs (cyclic joins are its only
input), so its value is shelved pending a bounded-time cyclic ILP. This is a
significant walk-back of that feature and is landed as its own reversible commit,
flagged for review. Removing the gate requires giving the cyclic ILP a bounded,
deterministic work budget (for example a node-count cap inside branch-and-bound,
which microlp does not currently expose, as opposed to a wall-clock limit).

### Relationship to the SCC-scoped DFJ plan

This gate overlaps an in-progress plan in
`20260705_eqsat_ilp_cycle_aware_extraction.md` (uncommitted at the time of this
writing) that independently measured the same slowness and designs exact
Dantzig-Fulkerson-Johnson cuts (pure-binary size-1 and size-2 cuts, per-SCC MTZ
only for size >= 3) to collapse the ILP solve toward the base cost, plus a
"timeout backstop" for a residual pathological tail. The current code already
carries those size-1 and size-2 cuts, and chbench's 7-way join, whose SCCs are
all size 2, is nonetheless in that pathological tail (54s), so the DFJ cuts alone
do not make it fast. The open decision is which backstop to use for that tail:
the wall-clock "timeout backstop" from that plan, which makes the chosen plan and
so the golden depend on solver speed (cross-machine nondeterminism, the failure
mode this whole document is about), or the deterministic structural gate landed
here, which bails all cyclic joins to greedy and so also forgoes the fast DFJ
cases. A middle option would be a deterministic, machine-independent work budget
inside the solver (a branch-and-bound node cap), which microlp does not currently
expose. This gate is a stopgap that keeps the branch green and the goldens
deterministic until that decision is made.
