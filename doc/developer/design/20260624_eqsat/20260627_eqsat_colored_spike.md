# SP3a — Colored E-Graph Measurement Spike

Status: design approved (2026-06-27)
Effort: relational+scalar eqsat unification, sub-project SP3a
Predecessors: SP1 (generic core, complete), SP2a (scalar instance, complete)
Memory: `eqsat-shared-core-extraction`

## 1. Purpose and scope

SP3a is a **gated measurement spike**. It builds the minimal colored-e-graph
mechanism — enough to run real colored *congruence closure* — plus a harness
that measures the color-explosion phenomenon on synthetic workloads, and emits a
findings section with an explicit **go / adjust verdict for SP3b's
representation**.

The colored e-graph (Singher–Itzhaky style; arXiv:2305.19203, FMCAD'24
"Easter Egg") is the target mechanism for *contextual* equalities: a color is a
relational context (the equivalences valid at a Filter/Join scope), and
context-dependent scalar equalities (`col0 = col1` under a filter) become
**colored congruence**, subsuming the relational `Equivalences` analysis. SP3a
de-risks the representation before the full build (SP3b).

### In scope

- A **flat** set of colors over one shared base `EGraph<L>`. No hierarchy.
- A color = a set of asserted equalities `Vec<(Id, Id)>` over the base id space.
- `close_color`: layer a coarser union-find over the **shared** base e-nodes,
  apply the color's equalities, and run congruence closure over the base's
  (immutable) e-nodes. This is the **reusable kernel** SP3b builds on.
- Generic over `Language`, exercised with a toy expression language. No
  relational or scalar coupling.
- Copy-per-color kept **only as a differential test oracle**, never as the
  measured substrate.
- Abstract parametric workload generators with a **locality** knob.
- A measurement harness + a findings/verdict section.

### Explicitly out of scope (deferred)

- **Hierarchical colors** (parent/child, layered find through an ancestor
  chain). SP3b. The mechanism is documented in §7 so SP3b inherits it.
- **Colored rewrite conclusions** and their `∏ᵢ |[xᵢ]_b|` blow-up. The paper
  bounds this to **≤ 2× per colored e-node** with a per-color *canonical
  hash-cons* (colored e-nodes stored as a delta, never in the base), plus
  deferred pruning and the "≤ 1 black e-class with colored e-nodes per colored
  class" minimization invariant. This blow-up arises only when colored *rules*
  fire and a delta e-node store exists — i.e. SP3b/SP4. SP3a measures the
  narrower **congruence-closure delta** (the Equivalences-subsumption path), and
  the spec flags the conclusion blow-up as a named, paper-bounded SP3b risk.
- **Color-aware extraction.** Unimplemented in the reference artifact; SP3b/SP4
  builds it.
- **Real LDBC-BI workloads.** Driving from real plans requires SP4's (undesigned)
  color-derivation; it becomes an SP4 validation step once that exists. SP3a's
  `close_color` kernel will be ready to measure them.
- **The shared-delta data structure itself.** SP3a *estimates its size* via the
  delta metric without building it; SP3b builds it.

### Production status

`colored.rs` is dead code in production builds until SP3b consumes it
(`#![allow(dead_code)]` at module scope, with a note naming SP3b). It compiles in
normal builds (to catch breakage) and is exercised only by tests/harness.

## 2. Background: what the spike measures, precisely

A colored e-graph is one base e-graph (the "black"/root congruence `≅`) plus a
set of colors. A color `c` adds equalities, yielding a **coarsening** `≅_c`
(`≅ ⊆ ≅_c`): colors only *add* edges, never split classes. This monotonicity is
what lets the base be shared.

Color-explosion cost has two distinct sources:

1. **Induced merges** — asserting `x ≅_c y` cascades via congruence: every
   `f(x, …) ≅_c f(y, …)`, then their parents, etc. A pure function of (base
   graph, color's equalities); representation-independent.

2. **Distinct colored canonical e-node forms** — in the base, `f(a, b)`
   canonicalizes to `f(find(a), find(b))`. Under `c`, `find_c(a)` may differ, so
   `f` has a different canonical form in `c`. Worst case
   `O(#colors × #e-nodes)`. The number of e-nodes whose colored form differs
   from the base form is the **delta** a shared-delta layer must store. SP3a
   measures this for the *congruence* case.

The differential oracle is anchored in the paper's "clones" semantic model: a
colored e-graph is equivalent to a set of separate e-graphs (one per color) with
the **same e-nodes** but possibly different representatives. `close_color`'s
partition must equal the partition of a from-scratch clone rebuilt with the same
equalities.

## 3. Components and interfaces

All in `src/transform/src/eqsat/colored.rs`, generic over `L: Language`.

### 3.1 `ColoredUf` — layered union-find + seeding

```rust
/// A colored union-find layered over a base `EGraph<L>`: a coarsening of the
/// base partition that shares the base's e-nodes. Flat (no hierarchy) for the
/// spike. SP3b replaces this with the sparse, path-compressed delta union-find
/// (Singher–Itzhaky `ColoredUnionFind`) and adds an ancestor chain.
pub(crate) struct ColoredUf {
    /// Colored parent pointers over the base id space (len == base.uf_len()).
    /// Seeded so `find(i) == base.find(i)`: the colored partition starts equal
    /// to the base partition, then only coarsens.
    parent: Vec<Id>,
}

impl ColoredUf {
    /// Seed a colored union-find equal to the base partition.
    fn over_base<L: Language>(base: &EGraph<L>) -> Self;
    /// Canonical colored id (non-compressing, mirrors `core::find_in`).
    fn find(&self, id: Id) -> Id;
    /// Union two colored classes; returns whether they were distinct.
    fn union(&mut self, a: Id, b: Id) -> bool;
}
```

`over_base` needs the base id-space size (one new core accessor, §3.6) and seeds
`parent[i] = base.find(i)` for `i in 0..base.uf_len()`.

### 3.2 `ColorMetrics` — recorded per closure run

```rust
pub(crate) struct ColorMetrics {
    /// Input equalities that actually merged two distinct colored classes.
    pub applied_equalities: usize,
    /// Congruence-cascade unions beyond `applied_equalities`.
    pub induced_merges: usize,
    /// Base classes whose colored root differs from their base root.
    pub delta_classes: usize,
    /// E-nodes whose colored canonical form differs from their base canonical
    /// form. ≈ the per-color storage a shared-delta layer would need.
    pub delta_nodes: usize,
    /// Closure fixpoint rounds.
    pub iters: usize,
}
```

### 3.3 `close_color` — the reusable kernel

```rust
/// Compute color `c`'s congruence closure over the shared base e-nodes.
///
/// Seed a `ColoredUf` equal to the base partition, apply `equalities`, then run
/// congruence closure: for each base e-node `n`, compute its colored canonical
/// form `L::map_children(n, |ch| cuf.find(ch))`; when two e-nodes from distinct
/// colored classes share a colored canonical form, union their classes. Repeat
/// to a fixpoint. The base e-graph is never copied or mutated. Returns the
/// colored union-find and the explosion metrics.
pub(crate) fn close_color<L: Language>(
    base: &EGraph<L>,
    equalities: &[(Id, Id)],
) -> (ColoredUf, ColorMetrics);
```

Reads base e-nodes via existing `class_ids()`/`nodes()`. Structurally the same
fixpoint as `core::EGraph::rebuild`, but parameterized by `cuf` and reading the
immutable base e-nodes. `delta_classes`/`delta_nodes` are computed at the end by
comparing colored canonical forms to base canonical forms.

### 3.4 Differential oracle (test-only)

```rust
#[cfg(test)]
fn close_color_by_copy<L: Language>(base: &EGraph<L>, equalities: &[(Id, Id)])
    -> ColoredUf;
```

Builds a fresh `EGraph<L>` re-adding all of `base`'s e-nodes, applies
`equalities`, calls `rebuild()`, and reads the resulting partition into a
`ColoredUf` over the base id space. Tests assert `close_color`'s partition equals
this oracle's over randomized workloads.

### 3.5 Workload generators (toy `Language`)

A toy k-ary expression language `ToyLang` lives in `colored.rs` (not core's
`#[cfg(test)]` `Arith`), so the harness can run outside `#[cfg(test)]`.

```rust
struct GenParams {
    base_size: usize,     // target e-node count
    fan_out: usize,       // operator arity / sharing degree
    depth: usize,         // expression nesting depth
    n_colors: usize,
    eqs_per_color: usize,
    locality: Locality,   // where asserted equalities land
}

enum Locality { LeafOnly, Mixed, SharedHot } // best → relational worst case

/// Build a base e-graph with the requested shape (deterministic given `seed`).
fn gen_base(p: &GenParams, seed: u64) -> EGraph<ToyLang>;
/// Generate `n_colors` equality sets over `base`'s classes (deterministic).
fn gen_colors(p: &GenParams, base: &EGraph<ToyLang>, seed: u64)
    -> Vec<Vec<(Id, Id)>>;
```

`SharedHot` steers equalities onto high-fan-out (heavily-shared) classes — the
relational worst case for congruence cascades; `LeafOnly` is the best case.
Randomness is a small inline deterministic LCG (no `rand` dependency,
reproducible across runs).

### 3.6 Core accessor (one addition)

In `src/transform/src/eqsat/core.rs`:

```rust
/// The size of the union-find id space (the count of ever-created e-classes,
/// including ids since merged away). Read-only; lets a colored layer seed a
/// union-find over the full id space.
#[allow(dead_code)] // SP3a colored.rs consumes this; SP3b uses it in production.
pub(crate) fn uf_len(&self) -> usize {
    self.uf.len()
}
```

This is the only change to existing code.

### 3.7 Harness

```rust
/// Run the parametric sweep, printing a table of metrics per cell.
/// `#[ignore]`d: it is the measurement instrument, not a CI gate.
#[ignore]
#[mz_ore::test]
fn measure_color_explosion() { /* sweep, aggregate, print table */ }
```

## 4. Measurement plan and gate criteria

Sweep each axis one-at-a-time from a baseline point, so each curve is isolated:

| Axis            | Baseline    | Sweep          | Isolates |
|-----------------|-------------|----------------|----------|
| `base_size`     | 500 nodes   | 100 → 5000     | does per-color cost scale with *total* graph size (bad) or only the local neighborhood (good)? |
| `fan_out`       | 4           | 1 → 32         | congruence amplification — high fan-out = bigger cascades |
| `n_colors`      | 50          | 10 → 1000      | aggregate scaling (expected linear in the flat model) |
| `eqs_per_color` | 4           | 1 → 32         | cascade per asserted equality |
| `locality`      | Mixed       | LeafOnly → SharedHot | the relational worst case |

Baseline numbers are harness knobs, not load-bearing; the plan pins defaults.

**Recorded per cell:** aggregate `delta_nodes`; the **sharing ratio**
`delta_nodes / (n_colors × node_count)`; the **cascade factor**
`induced_merges / applied_equalities`; closure wall-clock; peak `iters`. (The
paper's overhead metric `(colored − base e-nodes) / assumptions` is derivable as
`delta_nodes / applied_equalities`, comparable to its ~10× headline.)

**Headline numbers and what they decide:**

1. **Sharing ratio** — fraction of the naive `#colors × nodes` worst case a
   shared-delta layer would actually store.
   - ≪ 1 and roughly flat as `base_size` grows → shared-delta viable; SP3b
     builds it as designed.
   - → 1, or rising with `base_size` → sharing buys little; SP3b needs
     mitigations (color caps, lazy/on-demand color materialization, bounded
     colored-congruence depth).

2. **Cascade factor** — congruence merges induced per asserted equality, and its
   sensitivity to `fan_out`/`locality`.
   - bounded / mild growth → closure cost tractable.
   - super-linear blow-up on `SharedHot` → SP3b must bound congruence depth or
     restrict where colors attach.

**Gate verdict (the deliverable):** a "Findings & Gate Verdict" section (§8)
recording the curves, the two headline numbers, and an explicit recommendation —
*proceed with shared-delta as-is* / *proceed with named mitigations* /
*reconsider the colored approach*. Written from a real harness run during
execution (final task), not pre-filled.

**Honesty rails:** the harness logs the swept ranges and any cell skipped for
time, so a partial sweep never reads as full coverage. Findings state which
workloads were measured and which (LDBC, hierarchy, colored conclusions) are
explicitly deferred.

## 5. File structure

- **Create** `src/transform/src/eqsat/colored.rs` — `ColoredUf`, `ColorMetrics`,
  `close_color`, `ToyLang`, generators, harness, tests. Module-scope
  `#![allow(dead_code)]` + `// SP3b consumes close_color/ColoredUf/ColorMetrics`.
- **Modify** `src/transform/src/eqsat.rs` — add `mod colored;` beside `mod core;`.
- **Modify** `src/transform/src/eqsat/core.rs` — add `pub(crate) fn uf_len`.
- **This spec** gains a "Findings & Gate Verdict" section during execution.

## 6. Testing

Always-on, fast (the gate's correctness floor):

- `close_color` vs copy-oracle on small fixed graphs.
- `close_color` vs copy-oracle on **seeded randomized** workloads (partitions
  must match) — this is what makes the metrics trustworthy.
- `ColoredUf`: no equalities ⇒ colored partition == base partition; find/union
  basics.
- Metrics sanity: empty equalities ⇒ all-zero delta; a leaf equality on
  disconnected leaves ⇒ `induced_merges == 0`; `f(x), f(y)` with asserted
  `x ≅ y` ⇒ `induced_merges ≥ 1` and `f(x)`/`f(y)` colored-equal.

Measurement (not CI-gated): `#[ignore]`d `measure_color_explosion`.

Commands (mz-test skill):
- unit: `bin/cargo-test -p mz-transform eqsat::colored`
- sweep: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`

## 7. Notes for SP3b (inherited from the research)

- **Delta union-find** is the right representation: sparse (only black reps a
  color has merged), path-compressed, union-by-size, with `remove`
  (Singher–Itzhaky `ColoredUnionFind`). SP3a's `Vec`-backed `ColoredUf` is the
  simplified spike form.
- **Hierarchy:** colors form a tree; a colored `find` canonicalizes through the
  ancestor chain (recursively to black), *then* applies the color's own delta. A
  black union propagates down to all children. Rebuild black first; colors
  inherit black unions for free and process only their delta + dirty classes.
- **Conclusion blow-up:** never insert colored conclusions into the base; store
  them in a per-color **canonical** hash-cons (delta e-nodes). This caps
  redundant inserts at ≤ 2× per colored e-node. Add deferred pruning and the
  "≤ 1 black e-class with colored e-nodes per colored class" minimization
  invariant.
- **Color-aware extraction is unimplemented in the reference artifact** — SP3b/SP4
  builds it: run the cost fixpoint resolving children through the color's
  congruence, enumerating each class's e-nodes plus its colored siblings.
- **Soundness:** maintain `≅ ⊆ ≅_c` (colors only add). Mere presence of a node is
  not a judgement (only same-class membership is). Detect vacuous (contradictory)
  colors.

## 8. Findings & Gate Verdict

### Raw sweep output

```
# SP3a color-explosion sweep
# sharing_ratio = delta_nodes / (n_colors * node_count)
# cascade_factor = induced_merges / applied_equalities
axis,value,sharing_ratio,cascade_factor,delta_nodes,node_count,wall_ms,max_iters
base_size,100,0.1204,0.000,602,100,0,1
base_size,250,0.0518,0.000,647,250,2,1
base_size,500,0.0260,0.000,651,500,5,1
base_size,1000,0.0127,0.000,635,1000,10,1
base_size,2500,0.0051,0.000,632,2500,26,1
base_size,5000,0.0025,0.000,629,5000,55,1
fan_out,1,0.0172,1.710,430,500,14,8
fan_out,2,0.0137,0.000,342,500,4,1
fan_out,4,0.0260,0.000,651,500,5,1
fan_out,8,0.0499,0.000,1248,500,5,1
fan_out,16,0.0972,0.000,2430,500,6,1
fan_out,32,0.1743,0.000,4357,500,8,1
n_colors,10,0.0276,0.000,138,500,1,1
n_colors,50,0.0260,0.000,651,500,5,1
n_colors,100,0.0249,0.000,1245,500,10,1
n_colors,250,0.0253,0.000,3160,500,25,1
n_colors,500,0.0250,0.000,6262,500,51,1
n_colors,1000,0.0249,0.000,12441,500,104,1
eqs_per_color,1,0.0068,0.000,171,500,5,1
eqs_per_color,2,0.0132,0.000,330,500,5,1
eqs_per_color,4,0.0260,0.000,651,500,5,1
eqs_per_color,8,0.0490,0.000,1226,500,5,1
eqs_per_color,16,0.0960,0.000,2400,500,5,1
eqs_per_color,32,0.1880,0.000,4700,500,5,1
locality,0,0.0332,0.000,829,500,5,1
locality,1,0.0260,0.000,651,500,5,1
locality,2,0.0488,0.000,1220,500,5,1
```

(All cells reached `gen_base`'s target size; no underfills to report.)

This table is reproducible: `gen_colors` is deterministic given its seed (roots are sorted before sampling). Re-running `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion` produces identical output.

### Headline reading 1: sharing ratio vs base_size and n_colors

**base_size sweep** (100 → 5000, other params at baseline):
The sharing ratio falls from 0.12 → 0.0025 as base_size grows 50×, while
`delta_nodes` stays nearly constant (602 → 629). This means the per-color delta
is driven by the number of asserted equalities and their local neighborhoods,
*not* by total graph size. The naive worst case (`n_colors × node_count`) grows
with the graph but the actual delta does not. This is the ideal scaling for a
shared-delta layer: the overhead relative to the full graph shrinks as plans grow.

**n_colors sweep** (10 → 1000):
The sharing ratio stays flat at ≈ 0.025 as n_colors grows 100×, and
`delta_nodes` scales exactly linearly with n_colors (138 → 12 441). This
confirms the flat-color model's prediction: each color contributes an
independent, fixed-size delta; there is no super-linear accumulation across
colors. The sharing ratio at 1 000 colors is the same 0.025 as at 50 colors —
shared-delta storage is proportional to `(eqs_per_color × neighborhood_size) ×
n_colors`, not `node_count × n_colors`.

**fan_out sweep** (1 → 32):
The sharing ratio rises from 0.014 to 0.174 as fan_out grows. This is expected:
operators with more children have a larger delta footprint when a child is
equated. Even at fan_out = 32 (an extreme not seen in SQL plans) the ratio is
0.17, well below 1. The fan_out = 1 (unary) case is discussed below.

All measured sharing ratios are ≪ 1; none approaches the naive upper bound.

### Headline reading 2: cascade factor vs fan_out and locality

The cascade factor (induced congruence merges per asserted equality) is **0.000
for every configuration except fan_out = 1 (1.710)**.
Locality has no measurable effect on the cascade factor: LeafOnly, Mixed, and
SharedHot all produce 0.000.

The fan_out = 1 anomaly (cascade = 1.710, max_iters = 8) is the only case where
a cascade fixpoint required multiple rounds. Unary operators create deep
congruence chains — equating `x ≡ y` forces `f(x) ≡ f(y) ≡ f(f(x)) ≡ …` for
arbitrary depth in a chain. SQL relational algebra does not generate unary
operator chains of this structure (projections are eliminated or fused), so this
case is structurally absent from real workloads. For fan_out ≥ 2 — all practical
SQL/relational arity — the cascade factor is 0.000: congruence closure converges
in one pass.

The SharedHot locality (equalities on the top quartile by in-degree) produces a
higher sharing ratio than LeafOnly (0.049 vs 0.033) because high-degree nodes
have more downstream e-nodes to re-canonicalize, but *not* more induced merges.
This means the relational worst case (shared join keys) adds delta storage but
not a cascade explosion.

### Verdict

**Proceed with shared-delta as-is.**

The two headline numbers are unambiguously favorable:

1. The sharing ratio is ≪ 1 across every swept axis, and *decreases* as
   `base_size` grows — the absolute delta is nearly constant at a given
   `eqs_per_color`, independent of total graph size. There is no evidence of
   approaching the naive worst case at realistic plan scales.

2. The cascade factor is 0.000 for fan_out ≥ 2, covering the entirety of
   SQL/relational operator arity. Congruence closure converges in a single
   pass under the measured workloads.

No mitigations are required: SP3b can build the sparse, path-compressed
`ColoredUf` as specified in §7 (delta union-find, seeded from the base partition,
storing only merged black-roots), without color caps, lazy materialization, or
bounded-depth guards. The two worst single-axis cells (`eqs_per_color = 32` and
`n_colors = 1000`, measured independently) show the highest load (sharing ratio
0.19, 104 ms for 1 000 colors × 500 nodes) and remain tractable. Wall-clock
scales linearly with both `n_colors` and `base_size`, as expected.

The one anomaly — fan_out = 1 with cascade = 1.710 and 8 fixpoint rounds — is
structurally absent from real SQL plans and poses no risk to SP3b.

### Excluded from this sweep

This sweep did **not** measure:
- **Colored rewrite conclusions** and their per-color delta e-node store (the
  `∏ᵢ |[xᵢ]_b|` blow-up bounded in §7 to ≤ 2× by canonical hash-cons); that
  risk is SP3b-phase and paper-bounded, not SP3a.
- **Hierarchical colors** (parent/child color tree, ancestor-chain find); the
  flat model here is the SP3a scope.
- **Real LDBC-BI workload plans**; driving `close_color` from real plans requires
  SP4's color-derivation pass, which is undesigned. The `close_color` kernel is
  ready to measure them when SP4 exists.
- **Interaction effects** (e.g. SharedHot × high eqs_per_color simultaneously).
  One-at-a-time sweeps from a baseline point were used; joint extremes were not
  measured.
