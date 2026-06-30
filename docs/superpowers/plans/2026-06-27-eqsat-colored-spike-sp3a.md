# SP3a Colored E-Graph Measurement Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the minimal colored-e-graph mechanism (shared base + flat per-color equalities + a `close_color` congruence kernel) plus a measurement harness, and produce a gate verdict for SP3b's representation.

**Architecture:** A new `eqsat/colored.rs` module layers a coarsening union-find (`ColoredUf`) over the existing generic `EGraph<L>` from `eqsat/core`, sharing the base e-nodes (never copying them). `close_color` runs congruence closure for one color over the immutable base e-nodes and records explosion metrics. A toy `Language` (`ToyLang`) + seeded parametric generators feed an `#[ignore]`d harness that sweeps the cost axes; a `#[cfg(test)]` all-pairs oracle validates `close_color` differentially.

**Tech Stack:** Rust, the `mz-transform` crate, `eqsat/core` (`EGraph<L>`, `Language`, `Id`), `#[mz_ore::test]`.

**Spec:** `doc/developer/design/20260627_eqsat_colored_spike.md`

## Global Constraints

- **Congruence-only.** No colored rewrite conclusions, no per-color delta e-node store, no hierarchy, no color-aware extraction. Those are SP3b.
- **The base e-graph is never copied or mutated by the mechanism.** `close_color` reads base e-nodes; only the differential test oracle constructs an independent partition (still without copying the `EGraph`).
- **Only one change to existing code:** add `pub(crate) fn uf_len` to `core.rs`. No other edits to existing files except the `mod colored;` declaration in `eqsat.rs`.
- **`colored.rs` is dead code in production** until SP3b: module-scope `#![allow(dead_code)]` with a comment naming SP3b. `uf_len` carries `#[allow(dead_code)]` with the same note.
- **Determinism:** all randomness is a seeded inline SplitMix64 (`Lcg`). No `rand` dependency. Same seed ⇒ identical output.
- **Test macro:** use `#[mz_ore::test]` (not bare `#[test]`). The harness is `#[mz_ore::test]` + `#[ignore]` (precedent: `src/transform/src/eqsat/validation.rs:817`).
- **Test commands** (mz-test skill): unit `bin/cargo-test -p mz-transform eqsat::colored`; sweep `bin/cargo-test -p mz-transform eqsat::colored::measure_color_explosion -- --ignored --nocapture`.

---

### Task 1: Core `uf_len` accessor + `colored` module skeleton (`ToyLang`, `ColoredUf`)

**Files:**
- Modify: `src/transform/src/eqsat/core.rs` (add `uf_len` to `impl<L: Language> EGraph<L>`, near `node_count`/`data` around line 213–225)
- Modify: `src/transform/src/eqsat.rs:24` (add `mod colored;` after `mod core;`)
- Create: `src/transform/src/eqsat/colored.rs`

**Interfaces:**
- Consumes: `crate::eqsat::core::{EGraph, Id, Language}`; `EGraph::new`, `add`, `find`, `rebuild`, `class_ids` (pub(crate)), `node_count` (pub(crate)).
- Produces (for Tasks 2–4):
  - `core::EGraph::uf_len(&self) -> usize` (pub(crate)).
  - `ToyLang` (a `Language` with `Node = ToyNode`, `Sym = ToySym`, `GraphData = ()`); `ToyNode::{Leaf(u32), Op(u16, Vec<Id>)}`.
  - `struct ColoredUf { parent: Vec<Id> }` with `ColoredUf::over_base::<L>(&EGraph<L>) -> ColoredUf`, `fn find(&self, Id) -> Id`, `fn union(&mut self, Id, Id) -> bool`.

- [ ] **Step 1: Add the `uf_len` accessor to core**

In `src/transform/src/eqsat/core.rs`, inside `impl<L: Language> EGraph<L>` (right after the `node_count` method, ~line 215), add:

```rust
    /// The size of the union-find id space (count of ever-created e-classes,
    /// including ids since merged away). Read-only; lets a colored layer seed a
    /// union-find over the full id space.
    #[allow(dead_code)] // SP3a colored.rs consumes this; SP3b uses it in production.
    pub(crate) fn uf_len(&self) -> usize {
        self.uf.len()
    }
```

- [ ] **Step 2: Declare the module**

In `src/transform/src/eqsat.rs`, after the line `mod core;` (line 24), add:

```rust
mod colored;
```

- [ ] **Step 3: Create `colored.rs` with header, `ToyLang`, and `ColoredUf` (no tests yet)**

Create `src/transform/src/eqsat/colored.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

#![allow(dead_code)] // SP3a spike: SP3b consumes close_color/ColoredUf/ColorMetrics/ToyLang.

//! SP3a colored e-graph measurement spike.
//!
//! A color is a coarsening of the base congruence: a flat set of asserted
//! equalities over the base id space. [`ColoredUf`] is a union-find layered over
//! a base [`EGraph`] that shares the base's e-nodes. [`close_color`] (Task 2)
//! runs congruence closure for one color over the immutable base e-nodes and
//! records explosion metrics. Generators + harness (Tasks 3–4) measure the
//! color-explosion phenomenon to gate SP3b. See
//! `doc/developer/design/20260627_eqsat_colored_spike.md`.

use crate::eqsat::core::{EGraph, Id, Language};

/// A toy k-ary expression language for the spike. Leaves carry a distinct tag;
/// operators carry an op-code and child ids.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
enum ToyNode {
    Leaf(u32),
    Op(u16, Vec<Id>),
}

/// Operator-symbol buckets for [`ToyNode`].
#[derive(Clone, PartialEq, Eq, Hash)]
enum ToySym {
    Leaf,
    Op(u16),
}

struct ToyLang;

impl Language for ToyLang {
    type Node = ToyNode;
    type Sym = ToySym;
    type GraphData = ();

    fn children(node: &ToyNode) -> Vec<Id> {
        match node {
            ToyNode::Leaf(_) => vec![],
            ToyNode::Op(_, ch) => ch.clone(),
        }
    }

    fn map_children(node: &ToyNode, f: impl Fn(Id) -> Id) -> ToyNode {
        match node {
            ToyNode::Leaf(t) => ToyNode::Leaf(*t),
            ToyNode::Op(c, ch) => ToyNode::Op(*c, ch.iter().map(|&x| f(x)).collect()),
        }
    }

    fn symbol(node: &ToyNode) -> ToySym {
        match node {
            ToyNode::Leaf(_) => ToySym::Leaf,
            ToyNode::Op(c, _) => ToySym::Op(*c),
        }
    }
}

/// A colored union-find layered over a base [`EGraph`]: a coarsening of the base
/// partition that shares the base's e-nodes. Flat (no hierarchy) for the spike.
/// SP3b replaces this with the sparse, path-compressed delta union-find and adds
/// an ancestor chain.
pub(crate) struct ColoredUf {
    /// Colored parent pointers over the base id space (len == base.uf_len()).
    /// Seeded so `find(i) == base.find(i)`: the colored partition starts equal to
    /// the base partition, then only coarsens.
    parent: Vec<Id>,
}

impl ColoredUf {
    /// Seed a colored union-find equal to the base partition.
    pub(crate) fn over_base<L: Language>(base: &EGraph<L>) -> Self {
        let parent = (0..base.uf_len()).map(|i| base.find(i)).collect();
        ColoredUf { parent }
    }

    /// Canonical colored id (non-compressing).
    pub(crate) fn find(&self, mut id: Id) -> Id {
        while self.parent[id] != id {
            id = self.parent[id];
        }
        id
    }

    /// Union two colored classes; returns whether they were distinct.
    pub(crate) fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.parent[rb] = ra;
        true
    }
}
```

- [ ] **Step 4: Write the failing tests for Task 1**

Append to `src/transform/src/eqsat/colored.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Build a small fixed base: leaves a=Leaf(0), b=Leaf(1), x=Leaf(2),
    /// fa=Op(0,[a]), fb=Op(0,[b]). Returns (egraph, [a,b,x,fa,fb]).
    fn fixed_base() -> (EGraph<ToyLang>, Vec<Id>) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let x = eg.add(ToyNode::Leaf(2));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        let fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();
        (eg, vec![a, b, x, fa, fb])
    }

    #[mz_ore::test]
    fn colored_uf_starts_equal_to_base() {
        let (eg, ids) = fixed_base();
        let cuf = ColoredUf::over_base(&eg);
        for &id in &ids {
            assert_eq!(
                cuf.find(id),
                eg.find(id),
                "fresh colored uf must match base partition for id {id}"
            );
        }
    }

    #[mz_ore::test]
    fn colored_uf_union_merges_classes() {
        let (eg, ids) = fixed_base();
        let (a, b) = (ids[0], ids[1]);
        let mut cuf = ColoredUf::over_base(&eg);
        assert_ne!(cuf.find(a), cuf.find(b), "a and b start distinct");
        assert!(cuf.union(a, b), "union of distinct classes returns true");
        assert_eq!(cuf.find(a), cuf.find(b), "a and b merged after union");
        assert!(!cuf.union(a, b), "re-union of merged classes returns false");
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (both tests). First run compiles the crate (allow up to 10 min).

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/core.rs src/transform/src/eqsat.rs src/transform/src/eqsat/colored.rs
git commit -m "eqsat: SP3a colored module skeleton (uf_len, ToyLang, ColoredUf)"
```

---

### Task 2: `close_color` kernel + `ColorMetrics` + differential oracle

**Files:**
- Modify: `src/transform/src/eqsat/colored.rs`

**Interfaces:**
- Consumes: `ColoredUf`, `ToyLang`, `ToyNode`; `EGraph::{class_ids, nodes, node_count, find}`; `Language::map_children`.
- Produces (for Tasks 3–4):
  - `struct ColorMetrics { applied_equalities, induced_merges, delta_classes, delta_nodes, iters: usize }` (all `pub`).
  - `fn close_color<L: Language>(base: &EGraph<L>, equalities: &[(Id, Id)]) -> (ColoredUf, ColorMetrics)` (pub(crate)).

- [ ] **Step 1: Write the failing tests**

In `src/transform/src/eqsat/colored.rs`, add to `mod tests` (after the Task 1 tests):

```rust
    /// Independent oracle: colored congruence closure by all-pairs structural
    /// comparison (no hash-cons), over the base id space. Algorithmically distinct
    /// from `close_color` (which buckets by a per-pass memo), so agreement is
    /// meaningful. O(n^2 * iters) — test-sized graphs only.
    fn close_color_by_copy<L: Language>(base: &EGraph<L>, equalities: &[(Id, Id)]) -> ColoredUf {
        let mut uf = ColoredUf::over_base(base);
        for &(a, b) in equalities {
            uf.union(a, b);
        }
        let roots: Vec<Id> = base.class_ids().into_iter().filter(|&id| base.find(id) == id).collect();
        let nodes: Vec<(Id, L::Node)> = roots
            .iter()
            .flat_map(|&id| base.nodes(id).into_iter().map(move |n| (id, n)))
            .collect();
        loop {
            let mut merged = false;
            for i in 0..nodes.len() {
                for j in (i + 1)..nodes.len() {
                    let fi = L::map_children(&nodes[i].1, |c| uf.find(c));
                    let fj = L::map_children(&nodes[j].1, |c| uf.find(c));
                    if fi == fj && uf.union(nodes[i].0, nodes[j].0) {
                        merged = true;
                    }
                }
            }
            if !merged {
                break;
            }
        }
        uf
    }

    /// Whether two colored union-finds over the same id space induce the same
    /// equivalence on `ids`.
    fn same_partition(a: &ColoredUf, b: &ColoredUf, ids: &[Id]) -> bool {
        for i in 0..ids.len() {
            for j in 0..ids.len() {
                if (a.find(ids[i]) == a.find(ids[j])) != (b.find(ids[i]) == b.find(ids[j])) {
                    return false;
                }
            }
        }
        true
    }

    #[mz_ore::test]
    fn close_color_no_equalities_is_base_partition() {
        let (eg, ids) = fixed_base();
        let (cuf, m) = close_color(&eg, &[]);
        for &id in &ids {
            assert_eq!(cuf.find(id), eg.find(id), "no equalities ⇒ base partition");
        }
        assert_eq!(m.applied_equalities, 0);
        assert_eq!(m.induced_merges, 0);
        assert_eq!(m.delta_classes, 0);
        assert_eq!(m.delta_nodes, 0);
    }

    #[mz_ore::test]
    fn close_color_leaf_equality_no_cascade() {
        // a≡x where a,x are disconnected leaves ⇒ one merge, no congruence cascade.
        let (eg, ids) = fixed_base();
        let (a, x) = (ids[0], ids[2]);
        let (_cuf, m) = close_color(&eg, &[(a, x)]);
        assert_eq!(m.applied_equalities, 1);
        assert_eq!(m.induced_merges, 0, "leaf equality induces no congruence merges");
    }

    #[mz_ore::test]
    fn close_color_propagates_congruence() {
        // a≡b ⇒ Op(0,[a]) ≡ Op(0,[b]) by congruence.
        let (eg, ids) = fixed_base();
        let (a, b, fa, fb) = (ids[0], ids[1], ids[3], ids[4]);
        let (cuf, m) = close_color(&eg, &[(a, b)]);
        assert_eq!(cuf.find(fa), cuf.find(fb), "fa and fb colored-equal after a≡b");
        assert!(m.induced_merges >= 1, "congruence induced at least one merge");
        assert!(m.delta_nodes >= 1, "fa/fb canonical forms differ from base");
    }

    #[mz_ore::test]
    fn close_color_matches_oracle_fixed() {
        let (eg, ids) = fixed_base();
        let cases: &[&[(Id, Id)]] = &[&[], &[(ids[0], ids[1])], &[(ids[0], ids[2])], &[(ids[0], ids[1]), (ids[3], ids[4])]];
        for eqs in cases {
            let (cuf, _m) = close_color(&eg, eqs);
            let oracle = close_color_by_copy(&eg, eqs);
            assert!(same_partition(&cuf, &oracle, &ids), "close_color disagrees with oracle on {eqs:?}");
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: FAIL — `close_color` and `ColorMetrics` are not defined.

- [ ] **Step 3: Implement `ColorMetrics` and `close_color`**

In `src/transform/src/eqsat/colored.rs`, add (before the `#[cfg(test)] mod tests` block), after the `ColoredUf` impl:

```rust
use std::collections::HashMap;

/// Metrics recorded by one [`close_color`] run.
pub(crate) struct ColorMetrics {
    /// Input equalities that actually merged two distinct colored classes.
    pub applied_equalities: usize,
    /// Congruence-cascade unions beyond `applied_equalities`.
    pub induced_merges: usize,
    /// Base roots whose colored root differs from themselves (classes absorbed
    /// into another colored class).
    pub delta_classes: usize,
    /// E-nodes whose colored canonical form differs from their base canonical
    /// form. ≈ the per-color storage a shared-delta layer would need.
    pub delta_nodes: usize,
    /// Closure fixpoint rounds.
    pub iters: usize,
}

/// Compute color `c`'s congruence closure over the shared base e-nodes.
///
/// Seed a [`ColoredUf`] equal to the base partition, apply `equalities`, then run
/// congruence closure: for each base e-node `n`, compute its colored canonical
/// form `L::map_children(n, |ch| cuf.find(ch))`; when two e-nodes from distinct
/// colored classes share a colored canonical form, union their classes. Repeat to
/// a fixpoint. The base e-graph is never copied or mutated.
///
/// `base` must have been `rebuild()`-ed so its class ids are canonical roots.
pub(crate) fn close_color<L: Language>(
    base: &EGraph<L>,
    equalities: &[(Id, Id)],
) -> (ColoredUf, ColorMetrics) {
    let mut cuf = ColoredUf::over_base(base);
    let mut applied_equalities = 0;
    for &(a, b) in equalities {
        if cuf.union(a, b) {
            applied_equalities += 1;
        }
    }

    let roots: Vec<Id> = base
        .class_ids()
        .into_iter()
        .filter(|&id| base.find(id) == id)
        .collect();

    // Congruence closure over the shared, immutable base e-nodes.
    let mut induced_merges = 0;
    let mut iters = 0;
    loop {
        iters += 1;
        let mut memo: HashMap<L::Node, Id> = HashMap::new();
        let mut merged = false;
        for &id in &roots {
            for n in base.nodes(id) {
                let cn = L::map_children(&n, |c| cuf.find(c));
                let rep = cuf.find(id);
                if let Some(&other) = memo.get(&cn) {
                    if cuf.union(other, rep) {
                        induced_merges += 1;
                        merged = true;
                    }
                } else {
                    memo.insert(cn, rep);
                }
            }
        }
        if !merged {
            break;
        }
    }

    // Delta metrics: compare the colored partition to the base partition.
    let delta_classes = roots.iter().filter(|&&id| cuf.find(id) != id).count();
    let mut delta_nodes = 0;
    for &id in &roots {
        for n in base.nodes(id) {
            let base_form = L::map_children(&n, |c| base.find(c));
            let colored_form = L::map_children(&n, |c| cuf.find(c));
            if base_form != colored_form {
                delta_nodes += 1;
            }
        }
    }

    let metrics = ColorMetrics {
        applied_equalities,
        induced_merges,
        delta_classes,
        delta_nodes,
        iters,
    };
    (cuf, metrics)
}
```

Note: move the `use std::collections::HashMap;` to the top `use` block beside `use crate::eqsat::core::...` rather than mid-file if the implementer prefers; either compiles, but top-of-file is the convention here.

- [ ] **Step 4: Run the tests**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (all Task 1 + Task 2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/colored.rs
git commit -m "eqsat: SP3a close_color congruence kernel + ColorMetrics + oracle"
```

---

### Task 3: Workload generators (`Lcg`, `GenParams`, `Locality`, `gen_base`, `gen_colors`)

**Files:**
- Modify: `src/transform/src/eqsat/colored.rs`

**Interfaces:**
- Consumes: `ToyLang`, `ToyNode`, `EGraph::{new, add, rebuild, class_ids, nodes, node_count, find}`, `Language::children`.
- Produces (for Task 4):
  - `struct Lcg` with `Lcg::new(u64)`, `fn next_u64(&mut self) -> u64`, `fn below(&mut self, usize) -> usize`.
  - `#[derive(Clone)] struct GenParams { base_size, fan_out, depth, n_colors, eqs_per_color: usize, locality: Locality }`.
  - `#[derive(Clone, Copy)] enum Locality { LeafOnly, Mixed, SharedHot }`.
  - `fn gen_base(p: &GenParams, seed: u64) -> EGraph<ToyLang>`.
  - `fn gen_colors(p: &GenParams, base: &EGraph<ToyLang>, seed: u64) -> Vec<Vec<(Id, Id)>>`.

- [ ] **Step 1: Write the failing tests**

In `src/transform/src/eqsat/colored.rs`, add to `mod tests`:

```rust
    fn small_params() -> GenParams {
        GenParams {
            base_size: 80,
            fan_out: 3,
            depth: 3,
            n_colors: 5,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        }
    }

    #[mz_ore::test]
    fn gen_base_is_deterministic() {
        let p = small_params();
        let a = gen_base(&p, 7);
        let b = gen_base(&p, 7);
        assert_eq!(a.node_count(), b.node_count(), "same seed ⇒ same node count");
        // Same seed ⇒ same class structure: compare sorted node sets per partition size.
        assert_eq!(a.class_ids().len(), b.class_ids().len(), "same seed ⇒ same class count");
    }

    #[mz_ore::test]
    fn gen_base_reaches_target_size() {
        let p = small_params();
        let eg = gen_base(&p, 1);
        assert!(eg.node_count() >= p.base_size, "base reaches target node count");
    }

    #[mz_ore::test]
    fn gen_colors_shape() {
        let p = small_params();
        let base = gen_base(&p, 1);
        let colors = gen_colors(&p, &base, 2);
        assert_eq!(colors.len(), p.n_colors, "one equality set per color");
        for eqs in &colors {
            assert_eq!(eqs.len(), p.eqs_per_color, "eqs_per_color pairs per color");
            for &(a, b) in eqs {
                assert!(a < base.uf_len() && b < base.uf_len(), "ids in range");
            }
        }
    }

    #[mz_ore::test]
    fn locality_steers_toward_shared_classes() {
        // SharedHot should, on average, touch classes of higher in-degree than
        // LeafOnly on the same base.
        let base_p = GenParams { n_colors: 30, eqs_per_color: 4, ..small_params() };
        let base = gen_base(&base_p, 1);
        let deg = super::indegree(&base);
        let avg_deg = |colors: &Vec<Vec<(Id, Id)>>| -> f64 {
            let mut sum = 0usize;
            let mut n = 0usize;
            for eqs in colors {
                for &(a, b) in eqs {
                    sum += *deg.get(&base.find(a)).unwrap_or(&0);
                    sum += *deg.get(&base.find(b)).unwrap_or(&0);
                    n += 2;
                }
            }
            sum as f64 / n.max(1) as f64
        };
        let leaf = gen_colors(&GenParams { locality: Locality::LeafOnly, ..base_p.clone() }, &base, 2);
        let hot = gen_colors(&GenParams { locality: Locality::SharedHot, ..base_p.clone() }, &base, 2);
        assert!(
            avg_deg(&hot) > avg_deg(&leaf),
            "SharedHot ({}) must touch higher-degree classes than LeafOnly ({})",
            avg_deg(&hot),
            avg_deg(&leaf)
        );
    }

    #[mz_ore::test]
    fn close_color_matches_oracle_random() {
        // The trust anchor for the metrics: close_color must agree with the
        // independent all-pairs oracle across seeded random workloads.
        let p = GenParams {
            base_size: 60,
            fan_out: 2,
            depth: 3,
            n_colors: 8,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        };
        for seed in 0..8u64 {
            let base = gen_base(&p, seed);
            let ids = base.class_ids();
            let colors = gen_colors(&p, &base, seed + 100);
            for eqs in &colors {
                let (cuf, _m) = close_color(&base, eqs);
                let oracle = close_color_by_copy(&base, eqs);
                assert!(
                    same_partition(&cuf, &oracle, &ids),
                    "close_color disagrees with oracle: seed {seed}, eqs {eqs:?}"
                );
            }
        }
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: FAIL — `GenParams`, `Locality`, `gen_base`, `gen_colors`, `indegree` not defined.

- [ ] **Step 3: Implement the generators**

In `src/transform/src/eqsat/colored.rs`, add (before `#[cfg(test)] mod tests`):

```rust
/// A deterministic SplitMix64 PRNG (no `rand` dependency; reproducible).
pub(crate) struct Lcg {
    state: u64,
}

impl Lcg {
    pub(crate) fn new(seed: u64) -> Self {
        Lcg { state: seed }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n` (n must be > 0).
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % (n as u64)) as usize
    }
}

/// Where a color's asserted equalities land: best case (leaves) → relational
/// worst case (high-fan-out shared classes).
#[derive(Clone, Copy)]
pub(crate) enum Locality {
    LeafOnly,
    Mixed,
    SharedHot,
}

/// Synthetic-workload knobs for the spike.
#[derive(Clone)]
pub(crate) struct GenParams {
    pub base_size: usize,
    pub fan_out: usize,
    pub depth: usize,
    pub n_colors: usize,
    pub eqs_per_color: usize,
    pub locality: Locality,
}

/// In-degree (number of e-nodes referencing each class as a child) per base root.
pub(crate) fn indegree(base: &EGraph<ToyLang>) -> HashMap<Id, usize> {
    let mut deg: HashMap<Id, usize> = HashMap::new();
    for id in base.class_ids() {
        for n in base.nodes(id) {
            for c in ToyLang::children(&n) {
                *deg.entry(base.find(c)).or_insert(0) += 1;
            }
        }
    }
    deg
}

/// Build a base e-graph with the requested shape (deterministic given `seed`).
pub(crate) fn gen_base(p: &GenParams, seed: u64) -> EGraph<ToyLang> {
    let mut eg = EGraph::<ToyLang>::new();
    let mut rng = Lcg::new(seed);

    let n_leaves = (p.base_size / (p.depth + 1)).max(p.fan_out + 1);
    let mut all: Vec<Id> = Vec::new();
    for k in 0..n_leaves {
        all.push(eg.add(ToyNode::Leaf(k as u32)));
    }
    // Recent ids, to bias new operators toward forming depth.
    let mut frontier: Vec<Id> = all.clone();

    // Guard against pathological non-growth (all-duplicate hash-cons hits).
    let max_attempts = p.base_size.saturating_mul(20).max(1000);
    let mut attempts = 0;
    while eg.node_count() < p.base_size && attempts < max_attempts {
        attempts += 1;
        let mut children = Vec::with_capacity(p.fan_out.max(1));
        for _ in 0..p.fan_out.max(1) {
            let use_frontier = rng.below(2) == 0 && !frontier.is_empty();
            let pool = if use_frontier { &frontier } else { &all };
            children.push(pool[rng.below(pool.len())]);
        }
        let id = eg.add(ToyNode::Op(0, children));
        all.push(id);
        frontier.push(id);
        if frontier.len() > n_leaves {
            frontier.remove(0);
        }
    }
    eg.rebuild();
    eg
}

/// Generate `n_colors` equality sets over `base`'s classes (deterministic).
pub(crate) fn gen_colors(
    p: &GenParams,
    base: &EGraph<ToyLang>,
    seed: u64,
) -> Vec<Vec<(Id, Id)>> {
    let mut rng = Lcg::new(seed ^ 0x00C0_FFEE);
    let roots: Vec<Id> = base.class_ids().into_iter().filter(|&id| base.find(id) == id).collect();

    // Classes that contain a leaf e-node.
    let leaves: Vec<Id> = roots
        .iter()
        .copied()
        .filter(|&id| base.nodes(id).iter().any(|n| matches!(n, ToyNode::Leaf(_))))
        .collect();

    // Classes ranked by in-degree, descending; take the top quartile as "hot".
    let deg = indegree(base);
    let mut ranked: Vec<Id> = roots.clone();
    ranked.sort_by(|a, b| deg.get(b).unwrap_or(&0).cmp(deg.get(a).unwrap_or(&0)).then(a.cmp(b)));
    let hot_n = (ranked.len() / 4).max(1);
    let hot: Vec<Id> = ranked.into_iter().take(hot_n).collect();

    // Pick the candidate pool for this locality; fall back to all roots if empty.
    let pool: &[Id] = match p.locality {
        Locality::LeafOnly if !leaves.is_empty() => &leaves,
        Locality::SharedHot if !hot.is_empty() => &hot,
        _ => &roots,
    };

    let mut colors = Vec::with_capacity(p.n_colors);
    for _ in 0..p.n_colors {
        let mut eqs = Vec::with_capacity(p.eqs_per_color);
        for _ in 0..p.eqs_per_color {
            let a = pool[rng.below(pool.len())];
            let b = pool[rng.below(pool.len())];
            eqs.push((a, b));
        }
        colors.push(eqs);
    }
    colors
}
```

(`indegree` is referenced by the Task 3 test as `super::indegree`; it is defined here at module scope.)

- [ ] **Step 4: Run the tests**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (all tests through Task 3).

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/colored.rs
git commit -m "eqsat: SP3a workload generators (Lcg, GenParams, gen_base, gen_colors)"
```

---

### Task 4: Measurement harness + run sweep + write Findings & Gate Verdict

**Files:**
- Modify: `src/transform/src/eqsat/colored.rs`
- Modify: `doc/developer/design/20260627_eqsat_colored_spike.md` (§8 "Findings & Gate Verdict")

**Interfaces:**
- Consumes: `GenParams`, `Locality`, `gen_base`, `gen_colors`, `close_color`, `ColorMetrics`, `EGraph::node_count`.
- Produces: the `measure_color_explosion` harness and the findings/verdict in the spec.

- [ ] **Step 1: Implement the harness (no failing-test cycle — it is an `#[ignore]`d instrument)**

In `src/transform/src/eqsat/colored.rs`, add to `mod tests` (the harness uses test-only `println!` output and the generators):

```rust
    /// Run one sweep cell: build a base + colors, close every color, aggregate.
    /// Prints one CSV row. `value` is the swept axis value for labeling.
    fn run_cell(axis: &str, value: usize, p: &GenParams) {
        let base = gen_base(p, 1);
        let colors = gen_colors(p, &base, 2);
        let node_count = base.node_count();

        let start = std::time::Instant::now();
        let mut tot_delta = 0usize;
        let mut tot_applied = 0usize;
        let mut tot_induced = 0usize;
        let mut max_iters = 0usize;
        for eqs in &colors {
            let (_uf, m) = close_color(&base, eqs);
            tot_delta += m.delta_nodes;
            tot_applied += m.applied_equalities;
            tot_induced += m.induced_merges;
            max_iters = max_iters.max(m.iters);
        }
        let wall_ms = start.elapsed().as_millis();

        let naive = colors.len().saturating_mul(node_count).max(1);
        let sharing = tot_delta as f64 / naive as f64;
        let cascade = if tot_applied > 0 {
            tot_induced as f64 / tot_applied as f64
        } else {
            0.0
        };
        println!(
            "{axis},{value},{sharing:.4},{cascade:.3},{tot_delta},{node_count},{wall_ms},{max_iters}"
        );
    }

    /// Color-explosion measurement sweep. Not a CI gate — the spike instrument.
    /// Run: `bin/cargo-test -p mz-transform \
    ///   eqsat::colored::measure_color_explosion -- --ignored --nocapture`
    #[mz_ore::test]
    #[ignore]
    fn measure_color_explosion() {
        let baseline = GenParams {
            base_size: 500,
            fan_out: 4,
            depth: 4,
            n_colors: 50,
            eqs_per_color: 4,
            locality: Locality::Mixed,
        };
        println!("# SP3a color-explosion sweep");
        println!("# sharing_ratio = delta_nodes / (n_colors * node_count)");
        println!("# cascade_factor = induced_merges / applied_equalities");
        println!("axis,value,sharing_ratio,cascade_factor,delta_nodes,node_count,wall_ms,max_iters");

        for v in [100usize, 250, 500, 1000, 2500, 5000] {
            run_cell("base_size", v, &GenParams { base_size: v, ..baseline.clone() });
        }
        for v in [1usize, 2, 4, 8, 16, 32] {
            run_cell("fan_out", v, &GenParams { fan_out: v, ..baseline.clone() });
        }
        for v in [10usize, 50, 100, 250, 500, 1000] {
            run_cell("n_colors", v, &GenParams { n_colors: v, ..baseline.clone() });
        }
        for v in [1usize, 2, 4, 8, 16, 32] {
            run_cell("eqs_per_color", v, &GenParams { eqs_per_color: v, ..baseline.clone() });
        }
        for (label, loc) in [(0usize, Locality::LeafOnly), (1, Locality::Mixed), (2, Locality::SharedHot)] {
            run_cell("locality", label, &GenParams { locality: loc, ..baseline.clone() });
        }
    }
```

- [ ] **Step 2: Verify the suite still compiles and unit tests pass (harness excluded by `#[ignore]`)**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS; `measure_color_explosion` reported as ignored.

- [ ] **Step 3: Run the sweep and capture output**

Run: `bin/cargo-test -p mz-transform eqsat::colored::measure_color_explosion -- --ignored --nocapture`
Expected: the CSV header + one row per swept cell printed to stdout. Save the full table.

- [ ] **Step 4: Write the Findings & Gate Verdict**

Replace the placeholder body of §8 in `doc/developer/design/20260627_eqsat_colored_spike.md` (the line `_To be written by the final task from a real measure_color_explosion run._`) with:
- The captured CSV table (verbatim, in a code block).
- The two headline readings: how the **sharing ratio** behaves as `base_size` and `n_colors` grow (flat/≪1 ⇒ shared-delta viable; rising/→1 ⇒ mitigations needed), and how the **cascade factor** responds to `fan_out`/`locality`.
- An explicit verdict, one of: *proceed with shared-delta as-is* / *proceed with named mitigations (list them)* / *reconsider the colored approach*.
- An "Excluded from this sweep" line naming what was not measured (colored rewrite conclusions, hierarchy, real LDBC workloads) so the partial coverage is explicit.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/colored.rs doc/developer/design/20260627_eqsat_colored_spike.md
git commit -m "eqsat: SP3a color-explosion harness + findings/gate verdict"
```

---

## Notes for the executor

- **First build is slow** (cold `mz-transform` compile): allow a 10-minute timeout on the first `bin/cargo-test`.
- **No behavior-neutral slt sweep needed:** this is additive dead code in production (`#![allow(dead_code)]`); it touches no production path. The only existing-code change is the read-only `uf_len` accessor. A normal `bin/cargo-test -p mz-transform eqsat` at the end confirms nothing else regressed.
- **If `gen_base` fails `gen_base_reaches_target_size`** for some params (hash-cons collisions starving growth), the `max_attempts` guard means it returns a smaller graph; the baseline params in the harness are chosen to grow comfortably, but if a swept cell underfills, that is acceptable for measurement — note it in findings rather than forcing exact sizes.
- **The verdict is the deliverable.** Do not skip Step 3–4 of Task 4; the spike exists to produce measured numbers and a recommendation, not just code.
