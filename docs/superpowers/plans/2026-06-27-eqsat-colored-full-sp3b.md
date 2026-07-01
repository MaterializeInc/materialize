# SP3b — Full Colored E-Graph Mechanism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the production colored e-graph mechanism (sparse delta union-find, color hierarchy, colored congruence, colored-conclusion hash-cons, color-aware extraction) on `core::EGraph<L>`, generic over `Language`, exercised with `ToyLang` + a toy cost.

**Architecture:** Replace SP3a's spike (`colored.rs`: `Vec`-backed `ColoredUf`, batch `close_color`) with a `colored/` submodule realizing the Singher–Itzhaky mechanism. The base `EGraph<L>` is borrowed immutably and never copied. Correctness is anchored by an independent all-pairs-congruence "clones" oracle. Batch congruence (incremental deferred to SP4); pluggable `CostModel` (real cost deferred to SP4).

**Tech Stack:** Rust, `mz-transform` crate, `core::EGraph<L>` / `Language` from SP1, `#[mz_ore::test]` harness.

**Spec:** `doc/developer/design/20260624_eqsat/20260627_eqsat_colored_full_sp3b.md`

## Global Constraints

- The colored module is dead code until SP4: module-scope `#![allow(dead_code)] // SP3b: SP4 consumes the colored mechanism.` at the top of `colored/mod.rs`.
- The base `EGraph<L>` is borrowed (`&'b EGraph<L>`) and **never** copied or mutated.
- Only existing-code changes permitted: `src/transform/src/eqsat.rs` (`mod colored;` already present) and `src/transform/src/eqsat/core.rs` (add `lookup`; `uf_len` already present from SP3a). Do not touch any other existing file.
- Every type used across the `colored/` files is `pub(crate)` and derives `Debug` (the `private_interfaces` and `missing_debug_implementations` lints are CI-breaking under `cargo clippy --all-targets -- -D warnings`).
- Tests use `#[mz_ore::test]`, never bare `#[test]`. The measurement harness uses `#[mz_ore::test] #[ignore]`.
- Randomness is the seeded inline `Lcg` (SplitMix64) carried from SP3a; no `rand` dependency.
- Two id spaces: base ids `0..base.uf_len()`; colored ids from a counter starting at `base.uf_len()`. A colored id is its own black-level root.
- Unit test command: `bin/cargo-test -p mz-transform eqsat::colored`. Sweep: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::tests::measure_color_explosion`. (Per the mz-test skill: nextest, not libtest `--ignored`.)
- Before deleting SP3a's `colored.rs`, confirm nothing **outside** `colored` references its items (it is a private `mod`, `#![allow(dead_code)]`, with no re-exports; SP3a's only external touch was `core::uf_len`).

---

### Task 1: Module restructure + toy language + `ColoredUnionFind`

Convert SP3a's single `colored.rs` into the `colored/` submodule, carry the toy language + generators across verbatim, and build the sparse delta union-find with unit tests.

**Files:**
- Delete: `src/transform/src/eqsat/colored.rs`
- Create: `src/transform/src/eqsat/colored/mod.rs`
- Create: `src/transform/src/eqsat/colored/toy.rs`
- Create: `src/transform/src/eqsat/colored/union_find.rs`
- Unchanged: `src/transform/src/eqsat/core.rs` (`uf_len` already present), `src/transform/src/eqsat/eqsat.rs` (`mod colored;` already resolves to the dir)

**Interfaces:**
- Consumes: `crate::eqsat::core::{EGraph, Id, Language}`.
- Produces:
  - `colored/toy.rs`: `pub(crate) enum ToyNode { Leaf(u32), Op(u16, Vec<Id>) }`, `pub(crate) enum ToySym { Leaf, Op(u16) }`, `pub(crate) struct ToyLang` (`impl Language` with `GraphData = ()`), `pub(crate) struct Lcg`, `pub(crate) enum Locality { LeafOnly, Mixed, SharedHot }`, `pub(crate) struct GenParams { base_size, fan_out, depth, n_colors, eqs_per_color: usize, locality: Locality }`, `pub(crate) fn indegree(&EGraph<ToyLang>) -> HashMap<Id, usize>`, `pub(crate) fn gen_base(&GenParams, u64) -> EGraph<ToyLang>`, `pub(crate) fn gen_colors(&GenParams, &EGraph<ToyLang>, u64) -> Vec<Vec<(Id, Id)>>`.
  - `colored/union_find.rs`: `pub(crate) struct ColoredUnionFind` with `new`, `find_local(&mut self, Id) -> Id`, `union_local(&mut self, Id, Id) -> bool`, `remove(&mut self, Id)`.

- [ ] **Step 1: Create `colored/mod.rs` skeleton**

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

#![allow(dead_code)] // SP3b: SP4 consumes the colored mechanism.

//! Production colored e-graph mechanism (Singher–Itzhaky; arXiv:2305.19203,
//! FMCAD'24 "Easter Egg") layered over the shared, immutable base
//! `core::EGraph<L>`. A color is a coarsening of the base congruence
//! (`≅ ⊆ ≅_c`): colors only add edges. Colors form a tree; a layered `find`
//! canonicalizes through the ancestor chain to black, then applies each color's
//! own delta. See `doc/developer/design/20260624_eqsat/20260627_eqsat_colored_full_sp3b.md`.
//!
//! Components: [`union_find`] (sparse delta UF), the color tree + layered find
//! (this file, Task 2), `congruence` (Task 3), `conclusions` (Task 4),
//! `extract` (Task 5). [`toy`] carries the SP3a toy language + workload
//! generators; `oracle` (test-only) is the independent clones oracle.

mod toy;
mod union_find;
```

- [ ] **Step 2: Create `colored/toy.rs` by moving SP3a content**

Move — verbatim — from SP3a's `colored.rs` into `colored/toy.rs`: the license header, `ToyNode`, `ToySym`, `ToyLang` (+ its `impl Language`), `Lcg`, `Locality`, `GenParams`, `indegree`, `gen_base`, `gen_colors`. Prepend the imports and a module doc. Do **not** move `ColoredUf`, `ColorMetrics`, `close_color`, the oracle, the tests, or the harness (those are replaced by later tasks). Resulting file head:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Toy k-ary expression language and seeded synthetic-workload generators for
//! the colored e-graph tests and the measurement harness. Carried from the SP3a
//! spike unchanged.

use std::collections::HashMap;

use crate::eqsat::core::{EGraph, Id, Language};
```

Then the carried-over items (identical bodies to SP3a):
- `ToyNode` (`#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]`), `ToySym` (`#[derive(Clone, PartialEq, Eq, Hash, Debug)]`), `ToyLang` + `impl Language for ToyLang`.
- `Lcg` (`#[derive(Debug)]`, `new`/`next_u64`/`below`).
- `Locality` (`#[derive(Clone, Copy, Debug)]`), `GenParams` (`#[derive(Clone, Debug)]`, fields `pub`).
- `indegree`, `gen_base`, `gen_colors` (with the `roots.sort_unstable()` determinism fix from SP3a).

Copy these from the current `src/transform/src/eqsat/colored.rs` (lines for `ToyNode`..`gen_colors`); they compile unchanged because they depend only on `core` and `std`.

- [ ] **Step 3: Write the failing test for `ColoredUnionFind`**

Create `src/transform/src/eqsat/union_find.rs`? No — create `src/transform/src/eqsat/colored/union_find.rs` with the struct stub + tests so the test fails to compile/pass first:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A sparse, path-compressed, union-by-size union-find storing only the
//! black-roots a single color has merged. A missing key means "this id is its
//! own representative" — the color inherits the base/parent partition there.
//! SP3b replaces SP3a's dense `Vec`-backed `ColoredUf` with this.

use std::collections::HashMap;

use crate::eqsat::core::Id;

#[derive(Clone, Debug, Default)]
pub(crate) struct ColoredUnionFind {
    parent: HashMap<Id, Id>,
    size: HashMap<Id, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn fresh_is_identity() {
        let mut uf = ColoredUnionFind::new();
        assert_eq!(uf.find_local(7), 7, "untracked id is its own rep");
    }

    #[mz_ore::test]
    fn union_then_find_agree() {
        let mut uf = ColoredUnionFind::new();
        assert!(uf.union_local(1, 2), "union of distinct returns true");
        assert_eq!(uf.find_local(1), uf.find_local(2), "1 and 2 share a rep");
        assert!(!uf.union_local(1, 2), "re-union returns false");
    }

    #[mz_ore::test]
    fn union_by_size_attaches_smaller_under_larger() {
        let mut uf = ColoredUnionFind::new();
        // Build a 3-element class {1,2,3}, then union the singleton {4}.
        uf.union_local(1, 2);
        uf.union_local(1, 3);
        let big_root = uf.find_local(1);
        uf.union_local(4, 1);
        assert_eq!(uf.find_local(4), big_root, "singleton joins the larger class's root");
    }

    #[mz_ore::test]
    fn remove_detaches_element() {
        let mut uf = ColoredUnionFind::new();
        uf.union_local(1, 2);
        uf.remove(2);
        assert_eq!(uf.find_local(2), 2, "removed element reverts to its own rep");
        assert_eq!(uf.find_local(1), 1, "remaining element keeps a valid rep");
    }

    #[mz_ore::test]
    fn remove_keeps_other_members_together() {
        let mut uf = ColoredUnionFind::new();
        // {1,2,3}; remove 1 (which may be the root): 2 and 3 must stay together.
        uf.union_local(1, 2);
        uf.union_local(1, 3);
        uf.remove(1);
        assert_eq!(uf.find_local(2), uf.find_local(3), "remaining members stay merged");
        assert_eq!(uf.find_local(1), 1, "removed element reverts to its own rep");
    }
}
```

- [ ] **Step 4: Run tests, verify they fail**

Run: `bin/cargo-test -p mz-transform eqsat::colored::union_find`
Expected: compile error (`new`/`find_local`/`union_local`/`remove` not found).

- [ ] **Step 5: Implement `ColoredUnionFind`**

Add to `colored/union_find.rs` (inside `impl ColoredUnionFind`, before the `#[cfg(test)]` module):

```rust
impl ColoredUnionFind {
    pub(crate) fn new() -> Self {
        ColoredUnionFind::default()
    }

    /// Local representative of `id` within this color's delta. Path-compresses.
    /// `id` must already be canonicalized through any ancestor chain.
    pub(crate) fn find_local(&mut self, id: Id) -> Id {
        // Find the root (a self-loop or an absent key).
        let mut root = id;
        while let Some(&p) = self.parent.get(&root) {
            if p == root {
                break;
            }
            root = p;
        }
        // Path-compress: point everything on the path directly at the root.
        let mut cur = id;
        while let Some(&p) = self.parent.get(&cur) {
            if p == cur {
                break;
            }
            self.parent.insert(cur, root);
            cur = p;
        }
        root
    }

    /// Union the classes of `a` and `b` (union-by-size). Returns whether they
    /// were distinct. Roots are stored as explicit self-loops.
    pub(crate) fn union_local(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find_local(a), self.find_local(b));
        if ra == rb {
            return false;
        }
        let sa = *self.size.get(&ra).unwrap_or(&1);
        let sb = *self.size.get(&rb).unwrap_or(&1);
        let (win, lose) = if sa >= sb { (ra, rb) } else { (rb, ra) };
        self.parent.insert(win, win);
        self.parent.insert(lose, win);
        self.size.insert(win, sa + sb);
        self.size.remove(&lose);
        true
    }

    /// Remove `id` as a tracked element: its direct children reattach so the
    /// remaining members stay together, and `id` reverts to its own rep. Used to
    /// drop a color-local edge a coarser layer (black/parent) already provides.
    pub(crate) fn remove(&mut self, id: Id) {
        let kids: Vec<Id> = self
            .parent
            .iter()
            .filter(|&(&k, &v)| v == id && k != id)
            .map(|(&k, _)| k)
            .collect();
        // Does `id` have a real parent (i.e. is it a non-root)?
        let parent = self.parent.get(&id).copied().filter(|&p| p != id);
        match parent {
            Some(p) => {
                // Non-root: reattach children above `id` to `id`'s parent's root.
                let target = self.find_local(p);
                for k in kids {
                    self.parent.insert(k, target);
                }
            }
            None => {
                // Root (or untracked): promote the first child as the new root.
                if let Some((&first, rest)) = kids.split_first() {
                    self.parent.insert(first, first);
                    for &k in rest {
                        self.parent.insert(k, first);
                    }
                    if let Some(s) = self.size.remove(&id) {
                        self.size.insert(first, s.saturating_sub(1).max(1));
                    }
                }
            }
        }
        self.parent.remove(&id);
        self.size.remove(&id);
    }
}
```

- [ ] **Step 6: Carry over the toy generators' tests**

Append to `colored/toy.rs` a `#[cfg(test)] mod tests` with the SP3a generator tests (verbatim bodies): `gen_base_is_deterministic`, `gen_base_reaches_target_size`, `gen_colors_shape`, `gen_colors_is_deterministic`, `locality_steers_toward_shared_classes`, plus the `small_params()` helper. These depend only on `gen_base`/`gen_colors`/`indegree` and `core`, so they move unchanged from SP3a's `colored.rs`.

- [ ] **Step 7: Delete the old file and run everything**

Delete `src/transform/src/eqsat/colored.rs`. (`mod colored;` in `eqsat.rs` now resolves to `colored/mod.rs`.)

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (union_find tests + toy generator tests); no `colored.rs`-era tests remain.

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/colored.rs src/transform/src/eqsat/colored/
git commit -m "SP3b task 1: colored/ submodule, toy lang, ColoredUnionFind"
```

---

### Task 2: Color tree + layered find (`ColoredEGraph`)

Build the colored e-graph container — the color tree, two id spaces, and the layered `find`/`union` — in `colored/mod.rs`.

**Files:**
- Modify: `src/transform/src/eqsat/colored/mod.rs`

**Interfaces:**
- Consumes: `ColoredUnionFind` (Task 1); `core::{EGraph, Id, Language}`; `ToyLang` + generators (Task 1, for tests).
- Produces:
  - `pub(crate) struct ColorId(pub(crate) usize)` (`#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]`).
  - `pub(crate) struct ColoredEGraph<'b, L: Language>` with `new(&'b EGraph<L>) -> Self`, `new_color(&mut self, Option<ColorId>) -> ColorId`, `find(&mut self, ColorId, Id) -> Id`, `union(&mut self, ColorId, Id, Id) -> bool`.
  - `struct ColorData<L: Language> { parent: Option<ColorId>, uf: ColoredUnionFind, delta_nodes: HashMap<L::Node, Id> }` (private to the module).

- [ ] **Step 1: Write the failing tests**

Add to `colored/mod.rs` a `#[cfg(test)] mod color_tree_tests` (separate name to avoid clashing with later test modules in this file):

```rust
#[cfg(test)]
mod color_tree_tests {
    use super::*;
    use crate::eqsat::core::EGraph;
    use crate::eqsat::colored::toy::{ToyLang, ToyNode};

    /// a=Leaf(0), b=Leaf(1), x=Leaf(2), fa=Op(0,[a]), fb=Op(0,[b]).
    fn fixed_base() -> (EGraph<ToyLang>, [Id; 5]) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let x = eg.add(ToyNode::Leaf(2));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        let fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();
        (eg, [a, b, x, fa, fb])
    }

    #[mz_ore::test]
    fn fresh_color_equals_base_partition() {
        let (eg, ids) = fixed_base();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        for &id in &ids {
            assert_eq!(ceg.find(c, id), eg.find(id), "fresh color matches base for {id}");
        }
    }

    #[mz_ore::test]
    fn union_merges_within_color() {
        let (eg, ids) = fixed_base();
        let [a, b, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        assert_ne!(ceg.find(c, a), ceg.find(c, b));
        assert!(ceg.union(c, a, b));
        assert_eq!(ceg.find(c, a), ceg.find(c, b), "a≅b within color c");
    }

    #[mz_ore::test]
    fn child_inherits_parent_unions() {
        let (eg, ids) = fixed_base();
        let [a, b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let parent = ceg.new_color(None);
        ceg.union(parent, a, b);
        let child = ceg.new_color(Some(parent));
        // Child sees parent's a≅b for free...
        assert_eq!(ceg.find(child, a), ceg.find(child, b), "child inherits parent union");
        // ...and can add its own without affecting the parent.
        ceg.union(child, a, x);
        assert_eq!(ceg.find(child, a), ceg.find(child, x), "child has its own union");
        assert_ne!(ceg.find(parent, a), ceg.find(parent, x), "parent unaffected by child");
    }

    #[mz_ore::test]
    fn siblings_are_independent() {
        let (eg, ids) = fixed_base();
        let [a, b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let s1 = ceg.new_color(None);
        let s2 = ceg.new_color(None);
        ceg.union(s1, a, b);
        ceg.union(s2, a, x);
        assert_eq!(ceg.find(s1, a), ceg.find(s1, b));
        assert_ne!(ceg.find(s1, a), ceg.find(s1, x), "s1 does not see s2's union");
        assert_eq!(ceg.find(s2, a), ceg.find(s2, x));
        assert_ne!(ceg.find(s2, a), ceg.find(s2, b), "s2 does not see s1's union");
    }
}
```

- [ ] **Step 2: Run, verify failure**

Run: `bin/cargo-test -p mz-transform eqsat::colored::color_tree_tests`
Expected: compile error (`ColoredEGraph`/`ColorId`/`new`/`new_color`/`find`/`union` not found).

- [ ] **Step 3: Implement the color tree + layered find**

Add to `colored/mod.rs` (after the `mod` declarations, before the test module). Note the added imports:

```rust
use std::collections::HashMap;

use crate::eqsat::core::{EGraph, Id, Language};
use crate::eqsat::colored::union_find::ColoredUnionFind;

/// Index into a [`ColoredEGraph`]'s color tree. Black (the base congruence) is
/// the implicit root every color chain bottoms out at; it is not a `ColorId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ColorId(pub(crate) usize);

/// One color's state: its parent in the tree, its own delta union-find over the
/// parent's partition, and its colored-conclusion hash-cons (Task 4).
#[derive(Debug)]
struct ColorData<L: Language> {
    /// Parent color; `None` ⇒ this color's parent is black (the base).
    parent: Option<ColorId>,
    /// This color's own delta over its parent's partition.
    uf: ColoredUnionFind,
    /// Colored e-nodes that exist only in this color, kept canonical (Task 4).
    delta_nodes: HashMap<L::Node, Id>,
}

/// A colored e-graph: one shared, immutable base + a tree of colors. Generic
/// over `L`; built on `core::EGraph<L>`. The base is borrowed, never copied.
#[derive(Debug)]
pub(crate) struct ColoredEGraph<'b, L: Language> {
    base: &'b EGraph<L>,
    colors: Vec<ColorData<L>>,
    /// Allocator for colored e-node ids; starts at `base.uf_len()` so colored
    /// ids never alias base ids.
    next_colored_id: Id,
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    pub(crate) fn new(base: &'b EGraph<L>) -> Self {
        let next_colored_id = base.uf_len();
        ColoredEGraph {
            base,
            colors: Vec::new(),
            next_colored_id,
        }
    }

    /// Create a new color whose parent is `parent` (`None` ⇒ child of black).
    /// Colors are created parent-first, so the `colors` index order is a valid
    /// topological (pre-)order.
    pub(crate) fn new_color(&mut self, parent: Option<ColorId>) -> ColorId {
        let id = ColorId(self.colors.len());
        self.colors.push(ColorData {
            parent,
            uf: ColoredUnionFind::new(),
            delta_nodes: HashMap::new(),
        });
        id
    }

    /// The ancestor chain of `c`, top-down: the color whose parent is black
    /// first, `c` last.
    fn chain_top_down(&self, c: ColorId) -> Vec<ColorId> {
        let mut chain = Vec::new();
        let mut cur = Some(c);
        while let Some(cid) = cur {
            chain.push(cid);
            cur = self.colors[cid.0].parent;
        }
        chain.reverse();
        chain
    }

    /// Layered canonical id of `x` under color `c`: canonicalize through black,
    /// then apply each ancestor's and `c`'s own delta, top-down.
    pub(crate) fn find(&mut self, c: ColorId, x: Id) -> Id {
        // Black step: a base id canonicalizes through the base; a colored id is
        // its own black-level root.
        let mut r = if x < self.base.uf_len() {
            self.base.find(x)
        } else {
            x
        };
        for cid in self.chain_top_down(c) {
            r = self.colors[cid.0].uf.find_local(r);
        }
        r
    }

    /// Assert `x ≅_c y` in color `c` (inherited by all descendants). Returns
    /// whether the two were distinct under `c`.
    pub(crate) fn union(&mut self, c: ColorId, x: Id, y: Id) -> bool {
        let rx = self.find(c, x);
        let ry = self.find(c, y);
        self.colors[c.0].uf.union_local(rx, ry)
    }
}
```

- [ ] **Step 4: Make `toy` and `union_find` items reachable**

In `colored/mod.rs`, the test imports `crate::eqsat::colored::toy::{ToyLang, ToyNode}` and the impl imports `union_find::ColoredUnionFind`. Both `mod toy;`/`mod union_find;` are already declared (Task 1). Confirm `ToyLang`/`ToyNode` and `ColoredUnionFind` are `pub(crate)` (they are, from Task 1).

- [ ] **Step 5: Run tests, verify they pass**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (color_tree_tests + Task 1 tests).

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/colored/mod.rs
git commit -m "SP3b task 2: ColoredEGraph color tree + layered find"
```

---

### Task 3: Colored congruence + clones oracle

Add the colored congruence closure (`close`/`close_all`) generalizing SP3a's kernel, and the independent all-pairs "clones" oracle that anchors correctness.

**Files:**
- Create: `src/transform/src/eqsat/colored/congruence.rs`
- Create: `src/transform/src/eqsat/colored/oracle.rs`
- Modify: `src/transform/src/eqsat/colored/mod.rs` (add `mod congruence;` and `#[cfg(test)] mod oracle;`)

**Interfaces:**
- Consumes: `ColoredEGraph`, `ColorId`, `find`/`union` (Task 2); `toy` generators (Task 1); `core::{EGraph, Id, Language}`.
- Produces:
  - `pub(crate) struct ColorMetrics { applied_equalities, induced_merges, delta_classes, delta_nodes, iters: usize }` (fields `pub`).
  - On `ColoredEGraph`: `close(&mut self, ColorId, &[(Id, Id)]) -> ColorMetrics`, `close_all(&mut self, &[(ColorId, Vec<(Id, Id)>)]) -> Vec<(ColorId, ColorMetrics)>`, and the private helpers `visible_nodes(&mut self, ColorId) -> Vec<(Id, L::Node)>` and `recanonicalize_delta_nodes(&mut self, ColorId)`.
  - `oracle.rs` (test-only): `pub(crate) struct OracleColor<L: Language>` with `rep(&self, Id) -> Id`; `pub(crate) fn oracle_close<L: Language>(&EGraph<L>, &[(Id, Id)], &[(Id, L::Node)]) -> OracleColor<L>`; `pub(crate) fn same_partition<L: Language>(&mut ColoredEGraph<L>, ColorId, &OracleColor<L>, &[Id]) -> bool`.

- [ ] **Step 1: Add the module declarations**

In `colored/mod.rs`, after the existing `mod` lines:

```rust
mod congruence;
#[cfg(test)]
mod oracle;
```

- [ ] **Step 2: Write `colored/congruence.rs` (metrics + close), tests last**

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Colored congruence closure: SP3a's `close_color` kernel generalized to the
//! sparse delta union-find, the color hierarchy, and colored delta nodes. Batch
//! (incremental dirty-tracking is deferred to SP4); colors are closed in tree
//! pre-order so each child inherits its ancestors' induced merges.

use std::collections::HashMap;

use crate::eqsat::core::{Id, Language};
use crate::eqsat::colored::{ColorId, ColoredEGraph};

/// Metrics recorded by one [`ColoredEGraph::close`] run (carried from SP3a).
#[derive(Clone, Debug, Default)]
pub(crate) struct ColorMetrics {
    /// Input equalities that actually merged two distinct colored classes.
    pub applied_equalities: usize,
    /// Congruence-cascade unions beyond `applied_equalities`.
    pub induced_merges: usize,
    /// Base roots whose colored root differs from their base root.
    pub delta_classes: usize,
    /// Base e-nodes whose colored canonical form differs from their base form.
    pub delta_nodes: usize,
    /// Closure fixpoint rounds.
    pub iters: usize,
}

impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// All e-nodes visible to color `c`: every base e-node (paired with its base
    /// class) plus every colored delta node of `c` and its ancestors (paired
    /// with its colored id).
    fn visible_nodes(&mut self, c: ColorId) -> Vec<(Id, L::Node)> {
        let mut out: Vec<(Id, L::Node)> = Vec::new();
        let base_roots = self.base_roots();
        for &id in &base_roots {
            for n in self.base.nodes(id) {
                out.push((id, n));
            }
        }
        for cid in self.chain_top_down(c) {
            for (n, &nid) in &self.colors[cid.0].delta_nodes {
                out.push((nid, n.clone()));
            }
        }
        out
    }

    /// Canonical base roots (a base id equal to its own base representative).
    fn base_roots(&self) -> Vec<Id> {
        self.base
            .class_ids()
            .into_iter()
            .filter(|&id| self.base.find(id) == id)
            .collect()
    }

    /// Re-key color `c`'s delta_nodes against the current colored partition
    /// (the ≤2× minimization). A no-op until colored conclusions exist (Task 4).
    fn recanonicalize_delta_nodes(&mut self, c: ColorId) {
        let old = std::mem::take(&mut self.colors[c.0].delta_nodes);
        let mut new: HashMap<L::Node, Id> = HashMap::with_capacity(old.len());
        for (n, id) in old {
            let cn = L::map_children(&n, |ch| self.find(c, ch));
            new.entry(cn).or_insert(id);
        }
        self.colors[c.0].delta_nodes = new;
    }

    /// Apply `equalities` in color `c`, then close congruence to a fixpoint over
    /// every e-node visible to `c`. Assumes `c`'s ancestors are already closed.
    pub(crate) fn close(&mut self, c: ColorId, equalities: &[(Id, Id)]) -> ColorMetrics {
        let mut applied_equalities = 0;
        for &(a, b) in equalities {
            if self.union(c, a, b) {
                applied_equalities += 1;
            }
        }

        let visible = self.visible_nodes(c);

        let mut induced_merges = 0;
        let mut iters = 0;
        loop {
            iters += 1;
            let mut memo: HashMap<L::Node, Id> = HashMap::new();
            let mut merged = false;
            for (owner, n) in &visible {
                let cn = L::map_children(n, |ch| self.find(c, ch));
                let rep = self.find(c, *owner);
                if let Some(&other) = memo.get(&cn) {
                    if self.union(c, other, rep) {
                        induced_merges += 1;
                        merged = true;
                    }
                } else {
                    memo.insert(cn, rep);
                }
            }
            if !merged {
                break;
            }
        }

        // Delta metrics over the base, comparing colored vs base canonical forms.
        let base_roots = self.base_roots();
        let delta_classes = base_roots
            .iter()
            .filter(|&&id| self.find(c, id) != id)
            .count();
        let mut delta_nodes = 0;
        for &id in &base_roots {
            for n in self.base.nodes(id) {
                let base_form = L::map_children(&n, |ch| self.base.find(ch));
                let colored_form = L::map_children(&n, |ch| self.find(c, ch));
                if base_form != colored_form {
                    delta_nodes += 1;
                }
            }
        }

        self.recanonicalize_delta_nodes(c);

        ColorMetrics {
            applied_equalities,
            induced_merges,
            delta_classes,
            delta_nodes,
            iters,
        }
    }

    /// Close colors in tree pre-order (ascending `ColorId`, which is parent-first
    /// because colors are created parent-first), so each child inherits its
    /// ancestors' induced merges. The base must be `rebuild()`-ed by the caller.
    pub(crate) fn close_all(
        &mut self,
        per_color: &[(ColorId, Vec<(Id, Id)>)],
    ) -> Vec<(ColorId, ColorMetrics)> {
        let mut order: Vec<&(ColorId, Vec<(Id, Id)>)> = per_color.iter().collect();
        order.sort_by_key(|(c, _)| c.0);
        let mut out = Vec::with_capacity(order.len());
        for (c, eqs) in order {
            let m = self.close(*c, eqs);
            out.push((*c, m));
        }
        out
    }
}
```

Note: `visible_nodes`/`base_roots`/`chain_top_down`/`recanonicalize_delta_nodes` and the field accesses (`self.base`, `self.colors`) are within the same crate module tree, so `congruence.rs` can use the private `ColorData`/fields via the parent module only if they are visible. **`chain_top_down` and the `base`/`colors`/`next_colored_id` fields are private to `mod.rs`.** Make `chain_top_down` `pub(crate)` (in `mod.rs`, change `fn chain_top_down` → `pub(crate) fn chain_top_down`) and make the `ColoredEGraph` fields `base`, `colors`, `next_colored_id` and `ColorData`'s fields visible to sibling files by leaving them as-is (they are private to `mod colored`, and `congruence.rs` is a child `mod congruence` of `mod colored`). Child modules **can** read the parent module's private items, so no visibility change is needed for the fields; only items referenced by *name path* must resolve. Keep `chain_top_down` private — it is reachable from the child module. (If the compiler reports a privacy error on `chain_top_down` or the fields, promote exactly that item to `pub(crate)` and note it in the report.)

- [ ] **Step 3: Write `colored/oracle.rs`**

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Independent "clones" oracle: an all-pairs colored congruence over the base
//! id space + visible delta nodes, sharing no code with the sparse-UF /
//! layered-find mechanism under test. Ground truth for partition and (Task 5)
//! extraction. O(n²·iters); test-sized graphs only. Realizes the same semantic
//! model as SP3a's `close_color_by_copy`.

use std::collections::HashMap;

use crate::eqsat::core::{EGraph, Id, Language};
use crate::eqsat::colored::{ColorId, ColoredEGraph};

/// The result of an oracle closure: a flat representative map over the relevant
/// id space, plus the visible `(owner, node)` pairs (for extraction in Task 5).
pub(crate) struct OracleColor<L: Language> {
    parent: HashMap<Id, Id>,
    pub(crate) nodes: Vec<(Id, L::Node)>,
}

impl<L: Language> OracleColor<L> {
    /// Canonical representative of `id` (non-compressing read).
    pub(crate) fn rep(&self, mut id: Id) -> Id {
        while let Some(&p) = self.parent.get(&id) {
            if p == id {
                break;
            }
            id = p;
        }
        id
    }

    fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.rep(a), self.rep(b));
        if ra == rb {
            return false;
        }
        self.parent.insert(ra, rb);
        self.parent.insert(rb, rb);
        true
    }
}

/// Ground truth for a color: apply `equalities` and all-pairs congruence over the
/// base e-nodes + `delta_nodes`. `equalities` must be the union of the asserted
/// equalities along the color's chain (the test supplies them).
pub(crate) fn oracle_close<L: Language>(
    base: &EGraph<L>,
    equalities: &[(Id, Id)],
    delta_nodes: &[(Id, L::Node)],
) -> OracleColor<L> {
    let base_roots: Vec<Id> = base
        .class_ids()
        .into_iter()
        .filter(|&id| base.find(id) == id)
        .collect();
    let mut nodes: Vec<(Id, L::Node)> = Vec::new();
    for &id in &base_roots {
        for n in base.nodes(id) {
            nodes.push((id, n));
        }
    }
    nodes.extend(delta_nodes.iter().cloned());

    // Seed the rep map: base ids resolve to their base root; delta ids are their
    // own roots.
    let mut parent: HashMap<Id, Id> = HashMap::new();
    for id in 0..base.uf_len() {
        parent.insert(id, base.find(id));
    }
    for &(id, _) in delta_nodes {
        parent.entry(id).or_insert(id);
    }
    let mut oracle = OracleColor { parent, nodes };

    for &(a, b) in equalities {
        oracle.union(a, b);
    }
    // All-pairs congruence to a fixpoint.
    loop {
        let mut merged = false;
        let n = oracle.nodes.len();
        for i in 0..n {
            for j in (i + 1)..n {
                let fi = L::map_children(&oracle.nodes[i].1, |c| oracle.rep(c));
                let fj = L::map_children(&oracle.nodes[j].1, |c| oracle.rep(c));
                if fi == fj {
                    let (oi, oj) = (oracle.nodes[i].0, oracle.nodes[j].0);
                    if oracle.union(oi, oj) {
                        merged = true;
                    }
                }
            }
        }
        if !merged {
            break;
        }
    }
    oracle
}

/// Whether the colored mechanism and the oracle induce the same equivalence on
/// `ids` under color `c`.
pub(crate) fn same_partition<L: Language>(
    ceg: &mut ColoredEGraph<L>,
    c: ColorId,
    oracle: &OracleColor<L>,
    ids: &[Id],
) -> bool {
    for i in 0..ids.len() {
        for j in 0..ids.len() {
            let mech = ceg.find(c, ids[i]) == ceg.find(c, ids[j]);
            let orac = oracle.rep(ids[i]) == oracle.rep(ids[j]);
            if mech != orac {
                return false;
            }
        }
    }
    true
}
```

- [ ] **Step 4: Write the congruence tests in `oracle.rs`**

Append to `colored/oracle.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::colored::toy::{gen_base, gen_colors, GenParams, Locality, ToyLang, ToyNode};

    fn fixed_base() -> (EGraph<ToyLang>, [Id; 5]) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let x = eg.add(ToyNode::Leaf(2));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        let fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();
        (eg, [a, b, x, fa, fb])
    }

    #[mz_ore::test]
    fn close_no_equalities_is_base() {
        let (eg, ids) = fixed_base();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[]);
        for &id in &ids {
            assert_eq!(ceg.find(c, id), eg.find(id));
        }
        assert_eq!(m.applied_equalities, 0);
        assert_eq!(m.induced_merges, 0);
        assert_eq!(m.delta_classes, 0);
        assert_eq!(m.delta_nodes, 0);
    }

    #[mz_ore::test]
    fn close_leaf_equality_no_cascade() {
        let (eg, ids) = fixed_base();
        let [a, _b, x, ..] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[(a, x)]);
        assert_eq!(m.applied_equalities, 1);
        assert_eq!(m.induced_merges, 0, "leaf equality induces no congruence");
    }

    #[mz_ore::test]
    fn close_propagates_congruence() {
        let (eg, ids) = fixed_base();
        let [a, b, _x, fa, fb] = ids;
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let m = ceg.close(c, &[(a, b)]);
        assert_eq!(ceg.find(c, fa), ceg.find(c, fb), "Op(a)≅Op(b) after a≅b");
        assert!(m.induced_merges >= 1);
        assert!(m.delta_nodes >= 1);
    }

    #[mz_ore::test]
    fn close_matches_oracle_fixed() {
        let (eg, ids) = fixed_base();
        let [a, b, x, fa, fb] = ids;
        let cases: &[&[(Id, Id)]] = &[&[], &[(a, b)], &[(a, x)], &[(a, b), (fa, fb)]];
        for eqs in cases {
            let mut ceg = ColoredEGraph::new(&eg);
            let c = ceg.new_color(None);
            ceg.close(c, eqs);
            let oracle = oracle_close(&eg, eqs, &[]);
            assert!(same_partition(&mut ceg, c, &oracle, &ids), "disagree on {eqs:?}");
        }
    }

    #[mz_ore::test]
    fn close_matches_oracle_random() {
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
                let mut ceg = ColoredEGraph::new(&base);
                let c = ceg.new_color(None);
                ceg.close(c, eqs);
                let oracle = oracle_close(&base, eqs, &[]);
                assert!(
                    same_partition(&mut ceg, c, &oracle, &ids),
                    "disagree: seed {seed}, eqs {eqs:?}"
                );
            }
        }
    }

    #[mz_ore::test]
    fn close_matches_oracle_hierarchy() {
        // Two-level chain: parent gets eqs_p, child gets eqs_c. The oracle for
        // the child uses the UNION of both equality sets (the chain).
        let p = GenParams {
            base_size: 60,
            fan_out: 2,
            depth: 3,
            n_colors: 2,
            eqs_per_color: 3,
            locality: Locality::Mixed,
        };
        for seed in 0..8u64 {
            let base = gen_base(&p, seed);
            let ids = base.class_ids();
            let colors = gen_colors(&p, &base, seed + 100);
            let (eqs_p, eqs_c) = (&colors[0], &colors[1]);

            let mut ceg = ColoredEGraph::new(&base);
            let parent = ceg.new_color(None);
            let child = ceg.new_color(Some(parent));
            ceg.close(parent, eqs_p);
            ceg.close(child, eqs_c);

            let mut chain: Vec<(Id, Id)> = eqs_p.clone();
            chain.extend(eqs_c.iter().copied());
            let oracle = oracle_close(&base, &chain, &[]);
            assert!(
                same_partition(&mut ceg, child, &oracle, &ids),
                "hierarchy disagree: seed {seed}"
            );
        }
    }
}
```

- [ ] **Step 5: Run, fix, verify**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS. If a privacy error appears for `chain_top_down` or `ColoredEGraph` fields, promote exactly that item to `pub(crate)` and re-run; record it in the task report.

- [ ] **Step 6: Commit**

```bash
git add src/transform/src/eqsat/colored/
git commit -m "SP3b task 3: colored congruence (close/close_all) + clones oracle"
```

---

### Task 4: Colored conclusions

Add the per-color colored-conclusion hash-cons (`add_colored`) and the one core accessor it needs (`lookup`). Exercise the delta-node path through congruence.

**Files:**
- Modify: `src/transform/src/eqsat/core.rs` (add `lookup`)
- Create: `src/transform/src/eqsat/colored/conclusions.rs`
- Modify: `src/transform/src/eqsat/colored/mod.rs` (add `mod conclusions;`)

**Interfaces:**
- Consumes: `ColoredEGraph` fields (`base`, `colors`, `next_colored_id`), `find`, `chain_top_down` (Tasks 2–3); `core::EGraph::lookup` (this task); `close` (Task 3, for the delta-path test).
- Produces:
  - `core::EGraph::lookup(&self, &L::Node) -> Option<Id>`.
  - On `ColoredEGraph`: `add_colored(&mut self, ColorId, L::Node) -> Id`.

- [ ] **Step 1: Write the failing test for `core::lookup`**

In `src/transform/src/eqsat/core.rs`, inside the existing `#[cfg(test)] mod tests`, add (uses the existing `ArithLang`):

```rust
    #[mz_ore::test]
    fn lookup_finds_present_node_and_misses_absent() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let s = eg.add(Arith::Add(a, b));
        assert_eq!(eg.lookup(&Arith::Add(a, b)), Some(eg.find(s)), "present node");
        assert_eq!(eg.lookup(&Arith::Num(99)), None, "absent node");
    }
```

- [ ] **Step 2: Run, verify failure**

Run: `bin/cargo-test -p mz-transform eqsat::core::tests::lookup_finds_present_node_and_misses_absent`
Expected: compile error (`lookup` not found).

- [ ] **Step 3: Implement `core::lookup`**

In `core.rs`, inside `impl<L: Language> EGraph<L>`, after `uf_len`:

```rust
    /// The canonical class of `node` if it is present as a (canonical) e-node in
    /// the base, else `None`. Read-only; lets a colored layer detect when a
    /// colored conclusion is already represented in black.
    #[allow(dead_code)] // SP3b colored/ consumes this; SP4 uses it in production.
    pub(crate) fn lookup(&self, node: &L::Node) -> Option<Id> {
        self.memo.get(&self.canon(node)).map(|&id| self.find(id))
    }
```

- [ ] **Step 4: Add the module declaration + write the failing conclusions tests**

In `colored/mod.rs`, after the other `mod` lines: `mod conclusions;`.

Create `src/transform/src/eqsat/colored/conclusions.rs` with the tests first (struct method stubbed by absence):

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Colored conclusions: a per-color canonical hash-cons for e-nodes that exist
//! only in a color (what colored rewrite *conclusions* will produce in SP4).
//! Stored as a delta, never inserted into the base — the ≤2× bound. Driven
//! synthetically here via [`ColoredEGraph::add_colored`]; SP4 supplies a rule
//! driver.

use crate::eqsat::core::{Id, Language};
use crate::eqsat::colored::{ColorId, ColoredEGraph};

#[cfg(test)]
mod tests {
    use crate::eqsat::core::EGraph;
    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored::toy::{ToyLang, ToyNode};

    fn base_with_two_leaves() -> (EGraph<ToyLang>, Id, Id) {
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        eg.rebuild();
        (eg, a, b)
    }

    #[mz_ore::test]
    fn add_colored_dedups_canonically() {
        let (eg, a, _b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let n1 = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        let n2 = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        assert_eq!(n1, n2, "identical colored conclusions hash-cons to one id");
        assert!(n1 >= eg.uf_len(), "colored id is allocated above the base id space");
    }

    #[mz_ore::test]
    fn add_colored_hits_base_when_present() {
        // Op(0,[a]) already exists in base ⇒ add_colored returns the base class,
        // not a fresh colored id.
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let fa = eg.add(ToyNode::Op(0, vec![a]));
        eg.rebuild();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let got = ceg.add_colored(c, ToyNode::Op(0, vec![a]));
        assert_eq!(got, eg.find(fa), "form present in base returns the base class");
        assert!(got < eg.uf_len(), "no colored id allocated");
    }

    #[mz_ore::test]
    fn add_colored_respects_color_equalities() {
        // Under c, a≅b ⇒ Op(7,[a]) and Op(7,[b]) canonicalize to the same form
        // and dedup to one colored id.
        let (eg, a, b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.union(c, a, b);
        let na = ceg.add_colored(c, ToyNode::Op(7, vec![a]));
        let nb = ceg.add_colored(c, ToyNode::Op(7, vec![b]));
        assert_eq!(na, nb, "a≅b ⇒ Op(7,[a]) and Op(7,[b]) are the same colored node");
    }

    #[mz_ore::test]
    fn at_most_two_x_storage_bound() {
        // For N distinct colored Op nodes over base leaves, storage is N colored
        // ids (no duplication): the ≤2× bound (one base form + one colored form
        // per e-node) is respected — here there is no base form, so exactly N.
        let mut eg = EGraph::<ToyLang>::new();
        let leaves: Vec<Id> = (0..10).map(|k| eg.add(ToyNode::Leaf(k))).collect();
        eg.rebuild();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let mut ids = std::collections::HashSet::new();
        for &leaf in &leaves {
            ids.insert(ceg.add_colored(c, ToyNode::Op(9, vec![leaf])));
            // Re-adding the same node must not allocate again.
            ids.insert(ceg.add_colored(c, ToyNode::Op(9, vec![leaf])));
        }
        assert_eq!(ids.len(), leaves.len(), "no duplicate colored storage");
    }

    #[mz_ore::test]
    fn close_sees_delta_nodes() {
        // Two colored conclusions Op(5,[a]), Op(5,[b]); asserting a≅b must make
        // close() merge the two colored nodes by congruence.
        let (eg, a, b) = base_with_two_leaves();
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        let na = ceg.add_colored(c, ToyNode::Op(5, vec![a]));
        let nb = ceg.add_colored(c, ToyNode::Op(5, vec![b]));
        assert_ne!(ceg.find(c, na), ceg.find(c, nb), "distinct before a≅b");
        ceg.close(c, &[(a, b)]);
        assert_eq!(ceg.find(c, na), ceg.find(c, nb), "congruence merges colored nodes");
    }
}
```

- [ ] **Step 5: Run, verify failure**

Run: `bin/cargo-test -p mz-transform eqsat::colored::conclusions`
Expected: compile error (`add_colored` not found).

- [ ] **Step 6: Implement `add_colored`**

In `conclusions.rs`, before the test module:

```rust
impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Insert `node` as a colored e-node in `c`. Children are canonicalized
    /// through `find(c,·)`. If the canonical form is already a base e-node,
    /// returns the base class (no colored storage). If it is already in `c`'s or
    /// an ancestor's delta store, returns that id. Otherwise allocates a fresh
    /// colored id (> `base.uf_len()`) and stores it canonically in `c`.
    pub(crate) fn add_colored(&mut self, c: ColorId, node: L::Node) -> Id {
        let cn = L::map_children(&node, |ch| self.find(c, ch));

        // Already represented in black? Only meaningful when every child is a
        // base id (a colored-id child cannot exist in the base).
        let all_base_children = L::children(&cn).iter().all(|&ch| ch < self.base.uf_len());
        if all_base_children {
            if let Some(base_id) = self.base.lookup(&cn) {
                return self.find(c, base_id);
            }
        }

        // Already in c's or an ancestor's delta store?
        for cid in self.chain_top_down(c) {
            if let Some(&id) = self.colors[cid.0].delta_nodes.get(&cn) {
                return id;
            }
        }

        // Allocate a fresh colored id and store canonically in c.
        let id = self.next_colored_id;
        self.next_colored_id += 1;
        self.colors[c.0].delta_nodes.insert(cn, id);
        id
    }
}
```

`add_colored` reads `self.base`, `self.colors`, `self.next_colored_id`, and calls `self.chain_top_down` / `self.find` — all reachable from this child module of `mod colored`. The unused `use crate::eqsat::core::Id;` at the file top is used by the signature; keep it.

- [ ] **Step 7: Run, verify pass (core + colored)**

Run: `bin/cargo-test -p mz-transform eqsat::core eqsat::colored`
Expected: PASS (core `lookup` test + all colored tests).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/core.rs src/transform/src/eqsat/colored/
git commit -m "SP3b task 4: colored conclusions (add_colored) + core lookup"
```

---

### Task 5: Color-aware extraction

Add the pluggable `CostModel` trait, the color-aware `extract_colored`, the toy cost, and the extraction differential test.

**Files:**
- Create: `src/transform/src/eqsat/colored/extract.rs`
- Modify: `src/transform/src/eqsat/colored/toy.rs` (add `ToyCost`)
- Modify: `src/transform/src/eqsat/colored/mod.rs` (add `mod extract;`)
- Modify: `src/transform/src/eqsat/colored/oracle.rs` (add an oracle extraction for the test)

**Interfaces:**
- Consumes: `ColoredEGraph`, `find`, `chain_top_down`, `base`/`colors` fields (Tasks 2–4); `core::{Id, Language, MAX_ANALYSIS_ITERS}`; `OracleColor` (Task 3).
- Produces:
  - `pub(crate) trait CostModel<L: Language> { type Cost: Clone + Ord; fn cost(&self, &L::Node, &dyn Fn(Id) -> Self::Cost) -> Self::Cost; }`.
  - On `ColoredEGraph`: `extract_colored<C: CostModel<L>>(&mut self, ColorId, &C) -> HashMap<Id, (L::Node, C::Cost)>`.
  - `toy.rs`: `pub(crate) struct ToyCost` (`impl CostModel<ToyLang>`, `Cost = u64`, tree-size).
  - `oracle.rs`: `pub(crate) fn oracle_extract<L, C>(&OracleColor<L>, &C) -> HashMap<Id, C::Cost>` (test-only).

- [ ] **Step 1: Make `MAX_ANALYSIS_ITERS` reachable**

Confirm `core::MAX_ANALYSIS_ITERS` is `pub(crate)` (it is). `extract.rs` will import it.

- [ ] **Step 2: Add `mod extract;` and write the failing extraction tests**

In `colored/mod.rs`: `mod extract;`.

Create `src/transform/src/eqsat/colored/extract.rs` with the trait + tests; implement after.

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Color-aware extraction: a generic least-cost extractor parameterized by a
//! pluggable [`CostModel`]. Resolves children through `find(c,·)` and considers,
//! per colored class, the base e-nodes plus the colored delta nodes (siblings),
//! so a colored equality can expose a cheaper representative. SP3b ships a toy
//! additive cost; SP4 supplies the real relational/scalar cost.

use std::collections::HashMap;

use crate::eqsat::core::{Id, Language, MAX_ANALYSIS_ITERS};
use crate::eqsat::colored::{ColorId, ColoredEGraph};

/// A pluggable cost model over language `L`.
pub(crate) trait CostModel<L: Language> {
    type Cost: Clone + Ord;
    /// Cost of `node` given the current best cost of each child class. `child`
    /// is the child id as it appears in `node`; the extractor resolves it.
    fn cost(&self, node: &L::Node, child_cost: &dyn Fn(Id) -> Self::Cost) -> Self::Cost;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::core::EGraph;
    use crate::eqsat::colored::oracle::{oracle_close, oracle_extract};
    use crate::eqsat::colored::toy::{ToyCost, ToyLang, ToyNode};

    #[mz_ore::test]
    fn extract_matches_oracle() {
        // Base: a=Leaf, b=Leaf, fa=Op(0,[a]), fb=Op(0,[b]). Color: a≅b.
        let mut eg = EGraph::<ToyLang>::new();
        let a = eg.add(ToyNode::Leaf(0));
        let b = eg.add(ToyNode::Leaf(1));
        let _fa = eg.add(ToyNode::Op(0, vec![a]));
        let _fb = eg.add(ToyNode::Op(0, vec![b]));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.close(c, &[(a, b)]);

        let got = ceg.extract_colored(c, &ToyCost);
        let oracle = oracle_close(&eg, &[(a, b)], &[]);
        let want = oracle_extract(&oracle, &ToyCost);

        // Compare costs per class (keyed by the mechanism's reps, mapped through
        // the oracle's reps for the same ids).
        for (&cls, (_node, cost)) in &got {
            let oracle_cost = want.get(&oracle.rep(cls)).expect("oracle has class");
            assert_eq!(cost, oracle_cost, "cost mismatch for class {cls}");
        }
    }

    #[mz_ore::test]
    fn colored_equality_lowers_cost() {
        // Base has a cheap Leaf(0) and an expensive Op tree both reachable from a
        // class only after a colored equality. With c: target≅cheap, the
        // extracted cost of `target`'s class drops to the leaf cost.
        let mut eg = EGraph::<ToyLang>::new();
        let cheap = eg.add(ToyNode::Leaf(0));
        let m = eg.add(ToyNode::Leaf(1));
        let expensive = eg.add(ToyNode::Op(0, vec![m, m])); // cost 1 + 1 + 1 = 3
        eg.rebuild();

        // Base extraction: `expensive`'s class costs 3.
        let mut base_ceg = ColoredEGraph::new(&eg);
        let bc = base_ceg.new_color(None);
        let base = base_ceg.extract_colored(bc, &ToyCost);
        assert_eq!(base[&eg.find(expensive)].1, 3, "base cost of expensive tree");

        // Colored: expensive ≅ cheap ⇒ the class can pick the Leaf, cost 1.
        let mut ceg = ColoredEGraph::new(&eg);
        let c = ceg.new_color(None);
        ceg.close(c, &[(expensive, cheap)]);
        let colored = ceg.extract_colored(c, &ToyCost);
        let rep = ceg.find(c, expensive);
        assert_eq!(colored[&rep].1, 1, "colored equality exposes the cheap Leaf");
    }
}
```

- [ ] **Step 3: Run, verify failure**

Run: `bin/cargo-test -p mz-transform eqsat::colored::extract`
Expected: compile error (`extract_colored`, `ToyCost`, `oracle_extract` not found).

- [ ] **Step 4: Implement `extract_colored`**

In `extract.rs`, after the trait (before the tests):

```rust
impl<'b, L: Language> ColoredEGraph<'b, L> {
    /// Extract the lowest-cost e-node per class under color `c`. Children resolve
    /// through `find(c,·)`; each class's candidate set is its base e-nodes plus
    /// its colored delta nodes (siblings). A bounded least-fixpoint.
    pub(crate) fn extract_colored<C: CostModel<L>>(
        &mut self,
        c: ColorId,
        model: &C,
    ) -> HashMap<Id, (L::Node, C::Cost)> {
        // Gather visible nodes (base + c/ancestor delta), grouped by colored
        // class. For each node, precompute the resolution child_id -> rep so the
        // fixpoint never needs `&mut self`.
        let mut by_class: HashMap<Id, Vec<(L::Node, HashMap<Id, Id>)>> = HashMap::new();
        let visible = self.visible_nodes(c);
        for (owner, n) in visible {
            let rep = self.find(c, owner);
            let cmap: HashMap<Id, Id> = L::children(&n)
                .into_iter()
                .map(|ch| (ch, self.find(c, ch)))
                .collect();
            by_class.entry(rep).or_default().push((n, cmap));
        }

        let mut best: HashMap<Id, C::Cost> = HashMap::new();
        let mut best_node: HashMap<Id, L::Node> = HashMap::new();
        for _ in 0..MAX_ANALYSIS_ITERS {
            let mut changed = false;
            for (&cls, nodes) in &by_class {
                for (n, cmap) in nodes {
                    // Skip until every child class has a cost.
                    if !cmap.values().all(|r| best.contains_key(r)) {
                        continue;
                    }
                    let cost = model.cost(n, &|ch| {
                        let r = cmap.get(&ch).copied().unwrap_or(ch);
                        best.get(&r).cloned().expect("child cost present")
                    });
                    if best.get(&cls).map_or(true, |c0| &cost < c0) {
                        best.insert(cls, cost);
                        best_node.insert(cls, n.clone());
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        by_class
            .keys()
            .filter_map(|&cls| {
                Some((cls, (best_node.get(&cls)?.clone(), best.get(&cls)?.clone())))
            })
            .collect()
    }
}
```

`visible_nodes` is the private helper added in Task 3 (in `congruence.rs`, a sibling child module). It is reachable from `extract.rs`. If a privacy error arises, promote `visible_nodes` to `pub(crate)` in `congruence.rs` and note it.

- [ ] **Step 5: Implement `ToyCost` in `toy.rs`**

Append to `colored/toy.rs` (it needs the `CostModel` trait + `Id`):

```rust
use crate::eqsat::colored::extract::CostModel;

/// Tree-size cost over [`ToyLang`]: a leaf costs 1; an operator costs 1 + the
/// sum of its children's costs. The toy cost used to validate extraction.
#[derive(Debug)]
pub(crate) struct ToyCost;

impl CostModel<ToyLang> for ToyCost {
    type Cost = u64;

    fn cost(&self, node: &ToyNode, child_cost: &dyn Fn(Id) -> u64) -> u64 {
        match node {
            ToyNode::Leaf(_) => 1,
            ToyNode::Op(_, children) => {
                1 + children.iter().map(|&c| child_cost(c)).sum::<u64>()
            }
        }
    }
}
```

- [ ] **Step 6: Implement `oracle_extract` in `oracle.rs`**

Append to `colored/oracle.rs` (above its `#[cfg(test)] mod tests`, but make it available to tests — mark it `#[cfg(test)]` since the whole `oracle` module is test-only via `#[cfg(test)] mod oracle;`):

```rust
use crate::eqsat::colored::extract::CostModel;

/// Independent least-cost extraction over the oracle's partition + visible nodes,
/// mirroring `extract_colored` with a from-scratch grouping. Returns cost per
/// oracle representative.
pub(crate) fn oracle_extract<L: Language, C: CostModel<L>>(
    oracle: &OracleColor<L>,
    model: &C,
) -> HashMap<Id, C::Cost> {
    let mut by_class: HashMap<Id, Vec<(L::Node, HashMap<Id, Id>)>> = HashMap::new();
    for (owner, n) in &oracle.nodes {
        let rep = oracle.rep(*owner);
        let cmap: HashMap<Id, Id> = L::children(n)
            .into_iter()
            .map(|ch| (ch, oracle.rep(ch)))
            .collect();
        by_class.entry(rep).or_default().push((n.clone(), cmap));
    }
    let mut best: HashMap<Id, C::Cost> = HashMap::new();
    loop {
        let mut changed = false;
        for (&cls, nodes) in &by_class {
            for (n, cmap) in nodes {
                if !cmap.values().all(|r| best.contains_key(r)) {
                    continue;
                }
                let cost = model.cost(n, &|ch| {
                    let r = cmap.get(&ch).copied().unwrap_or(ch);
                    best.get(&r).cloned().expect("child cost present")
                });
                if best.get(&cls).map_or(true, |c0| &cost < c0) {
                    best.insert(cls, cost);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    best
}
```

(`oracle.rs` already imports `Id`, `Language`, `HashMap`; add the `CostModel` import.)

- [ ] **Step 7: Run, verify pass**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS (extraction tests + all prior).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/colored/
git commit -m "SP3b task 5: color-aware extraction (CostModel + extract_colored)"
```

---

### Task 6: Port the measurement harness + re-validate the gate

Port SP3a's `measure_color_explosion` to the new mechanism and confirm the SP3a §8 verdict still reproduces (the gate of record).

**Files:**
- Create: `src/transform/src/eqsat/colored/measure.rs`
- Modify: `src/transform/src/eqsat/colored/mod.rs` (add `#[cfg(test)] mod measure;`)

**Interfaces:**
- Consumes: `ColoredEGraph`, `new_color`, `close` (Tasks 2–3); `toy` generators (Task 1); `ColorMetrics` (Task 3).
- Produces: the `#[ignore]`d `measure_color_explosion` harness (test-only).

- [ ] **Step 1: Add the module declaration**

In `colored/mod.rs`: `#[cfg(test)] mod measure;`.

- [ ] **Step 2: Write the harness, ported to `ColoredEGraph`**

Create `src/transform/src/eqsat/colored/measure.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! SP3a's color-explosion measurement sweep, ported to the SP3b mechanism. Not
//! a CI gate — the spike instrument. Each color is a flat color (parent =
//! black) in one `ColoredEGraph`, closed independently; metrics are aggregated
//! exactly as in SP3a so the §8 verdict reproduces.
//!
//! Run: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture \
//!   eqsat::colored::measure::measure_color_explosion`

#[cfg(test)]
mod tests {
    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored::toy::{gen_base, gen_colors, GenParams, Locality};

    /// Run one sweep cell: build a base + colors, close every color, aggregate.
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
            let mut ceg = ColoredEGraph::new(&base);
            let c = ceg.new_color(None);
            let m = ceg.close(c, eqs);
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
        println!("# SP3b color-explosion sweep (ported from SP3a)");
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
}
```

- [ ] **Step 3: Run the unit suite (harness compiles, stays ignored)**

Run: `bin/cargo-test -p mz-transform eqsat::colored`
Expected: PASS; `measure_color_explosion` reported as ignored (not run).

- [ ] **Step 4: Run the sweep and compare to SP3a §8**

Run: `bin/cargo-test -p mz-transform --run-ignored ignored-only --no-capture eqsat::colored::measure::measure_color_explosion`
Expected: a CSV table. Verify the qualitative gate holds (it must, since the generators and congruence semantics are unchanged): sharing ratio ≪ 1 and falling across the `base_size` sweep; `delta_nodes` ~constant (~600–650) across `base_size`; linear in `n_colors`; cascade factor 0.000 for `fan_out ≥ 2` and elevated only at `fan_out = 1`. Record the actual table in the task report. If the numbers diverge **qualitatively** from SP3a §8 (e.g. sharing ratio rising with base_size, or non-zero cascade at fan_out ≥ 2), STOP and report — it signals a mechanism bug, not a spike re-run.

- [ ] **Step 5: Commit**

```bash
git add src/transform/src/eqsat/colored/
git commit -m "SP3b task 6: port color-explosion measurement harness to SP3b mechanism"
```

---

## Self-Review

**1. Spec coverage:**
- §4 sparse delta UF → Task 1. §5 color tree + layered find → Task 2. §6 colored congruence (close/close_all, pre-order, visible nodes, delta metrics, recanonicalize) → Task 3. §7 colored conclusions (add_colored, base-hit, ancestor search, ≤2× via canonical hash-cons + recanonicalize) → Task 4. §8 extraction (CostModel, extract_colored, toy cost) → Task 5. §9 core `lookup` → Task 4; `uf_len` already present. §10 oracle + all 7 test groups → Tasks 1/3/4/5/6. §3 module split → Task 1 (+ each task adds its file). §11 conventions → Global Constraints. §12 risks are design notes (no task). All covered.
- `remove`'s "subsumed by black, partition unchanged" property: covered structurally by Task 1's `remove_*` tests. **RESOLVED with the human (2026-06-27):** the ≤2× minimization uses `recanonicalize_delta_nodes` only (the load-bearing node-storage bound, tested in Task 4); UF-edge pruning via `remove` in `close` is **deferred to SP4**. Spec §6 step 5 + §12 updated to match. `remove` is built + unit-tested but intentionally uncalled in `close` — this is now the agreed design, and the updated spec (§6 step 5, §12) is the contract reviewers check against.

**2. Placeholder scan:** No TBD/TODO. Every code step has complete code. Test bodies are concrete with explicit assertions.

**3. Type consistency:** `ColorId(pub(crate) usize)` indexed via `.0` throughout. `find`/`union`/`close`/`add_colored`/`extract_colored` all take `&mut self` (path compression) — consistent across tasks. `ColorMetrics` fields match SP3a. `CostModel::Cost: Clone + Ord`; `ToyCost::Cost = u64`. `oracle_close(base, equalities, delta_nodes)` and `oracle_extract(oracle, model)` signatures match their call sites in Tasks 3/5. `visible_nodes`/`chain_top_down`/`base_roots` are defined in Tasks 2–3 and reused in Task 5.

**Cross-task privacy note (pre-flight for the reviewer):** `congruence.rs`, `conclusions.rs`, `extract.rs`, `oracle.rs`, `measure.rs` are child modules of `mod colored`; they can read `mod.rs`'s private `ColoredEGraph` fields and private helpers. Rust allows child modules to access ancestor-module private items, so no extra `pub(crate)` is needed for field access. Items referenced by an explicit `use` path across sibling files (e.g. `ColoredUnionFind`, `ColorId`, `ColoredEGraph`, `ColorMetrics`, `oracle_close`, `oracle_extract`, `CostModel`, `ToyLang`, generators) **are** `pub(crate)`. If any privacy error surfaces during a task, the fix is to promote exactly the named item to `pub(crate)` and note it — do not widen beyond that.
