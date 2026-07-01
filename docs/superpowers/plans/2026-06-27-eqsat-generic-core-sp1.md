# Eqsat Generic Core (SP1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the relational e-graph's data structure, congruence closure, and analysis framework into a generic, language-parameterized core (`EGraph<L: Language>`, `Analysis<L>`), with the relational engine as the sole, behavior-neutral instance.

**Architecture:** A new `src/transform/src/eqsat/core` module owns the language-agnostic substrate (`Id`, the `Language` trait, `EGraph<L>` with `find`/`add`/`union`/`rebuild`/`canon`/`index`, the `Analysis<L>` trait and its fixpoint driver). The relational engine keeps its `ENode`/`Sym`/`Rel` types and all its specialized machinery (saturation loop, rule codegen, cost/extraction) unchanged; `EGraph` becomes `pub type EGraph = core::EGraph<RelLang>`, and `RelLang` implements `Language` by delegating to `ENode`'s existing inherent methods, so existing call sites and generated code keep compiling untouched.

**Tech Stack:** Rust (mz-transform crate), `bin/cargo-test`, `bin/sqllogictest`. Uses associated-type-position `impl Trait` (APIT) and a GAT (`type Ctx<'a>`) — both stable.

## Global Constraints

- **Behavior-neutral.** The relational engine's observable behavior must not change. Acceptance: the existing eqsat unit + datadriven tests pass with **no `REWRITE=1` needed**, the sqllogictest transform suite shows **zero golden diffs**, and `cargo build` (the `build.rs` rule-codegen path) still builds.
- **Do NOT touch (out of scope for SP1):** the saturation loop (`EGraph::saturate` — Phase-2a, backoff, eq-cache, `AnalysisNeeds`); the rule machinery (`dsl.rs`, `matcher.rs`, `build.rs` codegen, `rules.rs`, `CompiledRule`, `EBindings`, `Payload`, `lean.rs`); cost/extraction (`cost.rs`, `extract.rs`, `objective.rs`, `EGraph::extract*`); the scalar engine (`scalar/`); `RecAnalysis` (it folds over `Rel`, not e-nodes — leave it on the relational side).
- **No new workspace dependencies.** If any new crate were needed, it must be added to `[workspace.dependencies]` in the root `Cargo.toml` first (`bin/lint-cargo` enforces) — but none is expected.
- **Generated docs are read-only.** Do not edit anything under `doc/developer/generated/`.
- **Keep `ENode`'s inherent `children`/`map_children`/`sym` methods.** `RelLang`'s `Language` impl delegates to them; do not delete or re-inline them.

---

## File Structure

- **Create** `src/transform/src/eqsat/core.rs` — the generic substrate: `Id`, `Language`, `EGraph<L>` + congruence + `index`, `Analysis<L>` + fixpoint driver, and toy-language unit tests. One module, one responsibility (the language-agnostic engine).
- **Modify** `src/transform/src/eqsat/egraph.rs` — `RelLang: Language` impl, `RelGraphData`, `pub type EGraph = core::EGraph<RelLang>`, relational inherent-impl block; delete the now-generic congruence/index/analysis-driver method bodies (they move to `core`).
- **Modify** `src/transform/src/eqsat/analysis.rs` — `Analysis` trait moves to `core`; the five analyses become `impl Analysis<RelLang>` with a `Ctx` associated type.
- **Modify** `src/transform/src/eqsat.rs` (the module root where `eqsat` submodules are declared) — add `mod core;` and re-export `Id`/`Language`/`EGraph` as needed.

The relational call sites for `add_rel`, `saturate`, `set_available`, `arity`, `extract*`, `run_analysis*` stay textually identical because `EGraph` remains a type named `EGraph` and the relational-specific methods stay in an inherent `impl core::EGraph<RelLang>` block.

---

### Task 1: Generic core module (`Id`, `Language`, `EGraph<L>`, congruence, `index`) + toy-language tests

This task is **purely additive** — it adds the generic core and proves it on a toy language without touching the relational engine. The crate must still build and all existing tests still pass after it.

**Files:**
- Create: `src/transform/src/eqsat/core.rs`
- Modify: `src/transform/src/eqsat.rs` (declare `mod core;` alongside the other `pub mod`/`mod` lines, ~line 41)

**Interfaces:**
- Produces (consumed by Tasks 2 and 3):
  - `pub type Id = usize;`
  - `pub trait Language` with assoc types `Node: Clone + Eq + Hash + Ord + Debug`, `Sym: Eq + Hash + Clone`, `GraphData: Default`; methods `fn children(node: &Self::Node) -> Vec<Id>`, `fn map_children(node: &Self::Node, f: impl Fn(Id) -> Id) -> Self::Node`, `fn symbol(node: &Self::Node) -> Self::Sym`.
  - `pub struct EGraph<L: Language>` with `pub(crate) classes` and `pub(crate) data` fields, and methods `fn new() -> Self`, `fn find(&self, Id) -> Id`, `fn add(&mut self, L::Node) -> Id`, `fn union(&mut self, Id, Id) -> bool`, `fn rebuild(&mut self)`, `fn canon(&self, &L::Node) -> L::Node`, `fn index(&self) -> HashMap<L::Sym, Vec<(Id, L::Node)>>`.

- [ ] **Step 1: Write the failing toy-language tests**

Create `src/transform/src/eqsat/core.rs` with ONLY the test module at first (the types it references won't exist yet, so it fails to compile — that is the intended "red"):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
    enum Arith {
        Num(i64),
        Add(Id, Id),
        Mul(Id, Id),
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    enum ArithSym {
        Num,
        Add,
        Mul,
    }

    struct ArithLang;

    impl Language for ArithLang {
        type Node = Arith;
        type Sym = ArithSym;
        type GraphData = ();

        fn children(node: &Arith) -> Vec<Id> {
            match node {
                Arith::Num(_) => vec![],
                Arith::Add(a, b) | Arith::Mul(a, b) => vec![*a, *b],
            }
        }

        fn map_children(node: &Arith, f: impl Fn(Id) -> Id) -> Arith {
            match node {
                Arith::Num(x) => Arith::Num(*x),
                Arith::Add(a, b) => Arith::Add(f(*a), f(*b)),
                Arith::Mul(a, b) => Arith::Mul(f(*a), f(*b)),
            }
        }

        fn symbol(node: &Arith) -> ArithSym {
            match node {
                Arith::Num(_) => ArithSym::Num,
                Arith::Add(..) => ArithSym::Add,
                Arith::Mul(..) => ArithSym::Mul,
            }
        }
    }

    #[mz_ore::test]
    fn add_hashconses_identical_nodes() {
        let mut eg = EGraph::<ArithLang>::new();
        let a1 = eg.add(Arith::Num(1));
        let a2 = eg.add(Arith::Num(1));
        assert_eq!(a1, a2, "identical leaf nodes must hashcons to one class");
    }

    #[mz_ore::test]
    fn congruence_collapses_after_union() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let x = eg.add(Arith::Num(3));
        let add_ax = eg.add(Arith::Add(a, x));
        let add_bx = eg.add(Arith::Add(b, x));
        assert_ne!(eg.find(add_ax), eg.find(add_bx));
        eg.union(a, b);
        eg.rebuild();
        // Congruence: once a ≡ b, Add(a,x) ≡ Add(b,x).
        assert_eq!(eg.find(add_ax), eg.find(add_bx));
    }

    #[mz_ore::test]
    fn index_buckets_by_symbol() {
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        let _ = eg.add(Arith::Add(a, b));
        let _ = eg.add(Arith::Mul(a, b));
        let idx: HashMap<ArithSym, Vec<(Id, Arith)>> = eg.index();
        assert_eq!(idx[&ArithSym::Num].len(), 2);
        assert_eq!(idx[&ArithSym::Add].len(), 1);
        assert_eq!(idx[&ArithSym::Mul].len(), 1);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail (do not compile)**

Run: `bin/cargo-test -p mz-transform eqsat::core`
Expected: compile error — `Language`, `EGraph`, `Id` are undefined.

- [ ] **Step 3: Implement the generic core above the test module**

Prepend to `src/transform/src/eqsat/core.rs` (above the `#[cfg(test)] mod tests`):

```rust
//! The language-agnostic equality-saturation substrate shared by the relational
//! and scalar engines: the e-class id type, the [`Language`] trait that
//! describes a node language, the [`EGraph`] data structure with its congruence
//! closure, and the per-class [`Analysis`] framework.
//!
//! SP1 introduces this core and instantiates it once, for the relational
//! engine (`EGraph<RelLang>`). The scalar engine and colored e-graphs are added
//! in later sub-projects. See `doc/developer/design/20260624_eqsat/20260627_eqsat_generic_core.md`.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

/// An e-class id. A bare index into the union-find; not a newtype.
pub type Id = usize;

/// A node language for the generic e-graph: the e-node type plus the structural
/// operations the core needs (child traversal, child remapping, operator
/// symbol). One implementor per sort (relational `RelLang`; scalar later).
pub trait Language {
    /// The e-node type. Operator nodes carry child `Id`s; leaves carry payload.
    type Node: Clone + Eq + Hash + Ord + Debug;
    /// The operator symbol used to bucket e-nodes in the matcher index.
    type Sym: Eq + Hash + Clone;
    /// Language-owned auxiliary state stored on the e-graph (relational: the
    /// index-availability oracle; toy/scalar: `()`).
    type GraphData: Default;

    /// Child e-class ids in operand order (empty for leaves).
    fn children(node: &Self::Node) -> Vec<Id>;
    /// Rewrite a node's child ids (used to canonicalize against the union-find).
    fn map_children(node: &Self::Node, f: impl Fn(Id) -> Id) -> Self::Node;
    /// The operator symbol for index bucketing.
    fn symbol(node: &Self::Node) -> Self::Sym;
}

/// The relational view of the e-graph the matchers scan: every e-node grouped
/// by its operator symbol, paired with its canonical parent class.
pub type Index<L> = HashMap<<L as Language>::Sym, Vec<(Id, <L as Language>::Node)>>;

/// An e-graph over node language `L`: a union-find of e-classes, each a set of
/// e-nodes, with a hash-cons memo for deduplication.
pub struct EGraph<L: Language> {
    uf: Vec<Id>,
    pub(crate) classes: HashMap<Id, HashSet<L::Node>>,
    memo: HashMap<L::Node, Id>,
    pub(crate) data: L::GraphData,
}

// Manual `Default`/`new` so we do not impose `L: Default` (the derive would).
impl<L: Language> Default for EGraph<L> {
    fn default() -> Self {
        EGraph {
            uf: Vec::new(),
            classes: HashMap::new(),
            memo: HashMap::new(),
            data: L::GraphData::default(),
        }
    }
}

impl<L: Language> EGraph<L> {
    pub fn new() -> Self {
        EGraph::default()
    }

    /// The canonical id of `id`.
    pub fn find(&self, mut id: Id) -> Id {
        while self.uf[id] != id {
            id = self.uf[id];
        }
        id
    }

    fn new_class(&mut self) -> Id {
        let id = self.uf.len();
        self.uf.push(id);
        self.classes.insert(id, HashSet::new());
        id
    }

    /// A copy of `n` with its children canonicalized against the union-find.
    pub fn canon(&self, n: &L::Node) -> L::Node {
        L::map_children(n, |c| self.find(c))
    }

    /// Add an e-node, returning its (canonical) e-class. Hash-conses.
    pub fn add(&mut self, node: L::Node) -> Id {
        let node = self.canon(&node);
        if let Some(&id) = self.memo.get(&node) {
            return self.find(id);
        }
        let id = self.new_class();
        self.classes.get_mut(&id).unwrap().insert(node.clone());
        self.memo.insert(node, id);
        id
    }

    /// Union the classes of `a` and `b`; returns whether they were distinct.
    pub fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.uf[rb] = ra;
        let nodes = self.classes.remove(&rb).unwrap_or_default();
        self.classes.entry(ra).or_default().extend(nodes);
        true
    }

    /// Restore canonical-children and congruence invariants after unions.
    pub fn rebuild(&mut self) {
        loop {
            let mut merged = false;
            let mut memo: HashMap<L::Node, Id> = HashMap::new();
            let ids: Vec<Id> = self.classes.keys().copied().collect();
            for id in ids {
                let rep = self.find(id);
                let nodes: Vec<L::Node> = self
                    .classes
                    .get(&id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for n in nodes {
                    let cn = self.canon(&n);
                    if let Some(&other) = memo.get(&cn) {
                        if self.union(other, rep) {
                            merged = true;
                        }
                    } else {
                        memo.insert(cn, rep);
                    }
                }
            }
            let mut new_classes: HashMap<Id, HashSet<L::Node>> = HashMap::new();
            let old: Vec<(Id, HashSet<L::Node>)> = self.classes.drain().collect();
            for (id, nodes) in old {
                let rep = self.find(id);
                let entry = new_classes.entry(rep).or_default();
                for n in nodes {
                    entry.insert(self.canon(&n));
                }
            }
            self.classes = new_classes;
            if !merged {
                break;
            }
        }
        self.memo.clear();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                self.memo.insert(n.clone(), id);
            }
        }
    }

    /// Build the operator-symbol index over all e-nodes.
    pub fn index(&self) -> Index<L> {
        let mut idx: Index<L> = HashMap::new();
        for (&id, nodes) in &self.classes {
            for n in nodes {
                idx.entry(L::symbol(n)).or_default().push((id, n.clone()));
            }
        }
        idx
    }
}
```

This is the relational congruence logic (`egraph.rs` `find` :399, `new_class` :406, `canon_enode` :413, `add` :418, `union` :615, `rebuild` :629, `index` :874) lifted verbatim, with `ENode → L::Node`, `n.sym() → L::symbol(n)`, `n.map_children → L::map_children`, and `canon_enode → canon`.

- [ ] **Step 4: Declare the module**

In `src/transform/src/eqsat.rs` (the `eqsat` module root, alongside the existing `pub mod egraph;` etc. near line 41), add:

```rust
mod core;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bin/cargo-test -p mz-transform eqsat::core`
Expected: PASS (`add_hashconses_identical_nodes`, `congruence_collapses_after_union`, `index_buckets_by_symbol`).

- [ ] **Step 6: Verify the whole crate still builds and existing tests still pass**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: PASS — no existing test changed (Task 1 is additive).

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/core.rs src/transform/src/eqsat.rs
git commit -m "eqsat: add generic language-parameterized e-graph core (SP1 task 1)"
```

---

### Task 2: Instantiate the relational engine on the core (`EGraph<RelLang>`)

Make the relational `EGraph` an instantiation of `core::EGraph<RelLang>`, delete the now-duplicated congruence/index methods from `egraph.rs`, move `available` into `RelGraphData`, and keep all relational-specific methods in an inherent impl. Behavior-neutral.

**Files:**
- Modify: `src/transform/src/eqsat/egraph.rs`

**Interfaces:**
- Consumes (from Task 1): `core::{Id, Language, EGraph}`.
- Produces: `pub type EGraph = core::EGraph<RelLang>`; `pub struct RelLang`; `impl Language for RelLang`; `pub struct RelGraphData { pub available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> }`. All existing relational method names (`add_rel`, `saturate`, `set_available`, `arity`, `column_types`, `index`, `find`, `add`, `union`, `rebuild`, `reachable`, `child_classes`, `seed_indexed_filters`, `extract*`) remain callable on `EGraph` exactly as before.

- [ ] **Step 1: Add the `RelLang` language impl and `RelGraphData`**

In `egraph.rs`, after the `ENode`/`Sym` definitions and their inherent `impl ENode` block (which keeps `children`/`map_children`/`sym` — do NOT delete them), add:

```rust
use crate::eqsat::core::{self, Language};

/// Per-e-graph relational auxiliary state: the index-availability oracle read by
/// the indexed-filter pull-up condition.
#[derive(Default)]
pub struct RelGraphData {
    pub available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}

/// The relational node language.
pub struct RelLang;

impl Language for RelLang {
    type Node = ENode;
    type Sym = Sym;
    type GraphData = RelGraphData;

    fn children(node: &ENode) -> Vec<Id> {
        node.children()
    }
    fn map_children(node: &ENode, f: impl Fn(Id) -> Id) -> ENode {
        node.map_children(f)
    }
    fn symbol(node: &ENode) -> Sym {
        node.sym()
    }
}

/// The relational e-graph: the generic core instantiated at [`RelLang`].
pub type EGraph = core::EGraph<RelLang>;

/// The relational matcher index (kept as a named alias so generated rule code
/// and `CompiledRule::find` continue to refer to `Index`).
pub(crate) type Index = core::Index<RelLang>;
```

(If `Id` is currently defined in `egraph.rs` as `pub type Id = usize;`, replace that definition with `pub use crate::eqsat::core::Id;` so there is one `Id`. Update no call sites — the path `Id` still resolves. `core::Index<RelLang>` is definitionally `HashMap<Sym, Vec<(Id, ENode)>>`, identical to the old `Index`, so all `Index` references keep their meaning.)

- [ ] **Step 2: Delete the now-generic methods from the old `impl EGraph` block**

In `egraph.rs`, the inherent `impl EGraph { … }` block currently defines methods that now live in `core`. Delete these method definitions (they are provided by `core::EGraph<L>`): `find` (:399), `new_class` (:406), `canon_enode` (:413), `add` (:418), `union` (:615), `rebuild` (:629), `index` (:874). Also delete the `struct EGraph { … }` definition (:372–381), the old `type Index = …` alias (:386) — **replaced** by the `pub(crate) type Index = core::Index<RelLang>;` added in Step 1, not removed outright — and `impl EGraph { pub fn new() … }` (:389–391), since `EGraph` is now the type alias and `new` comes from core. Keep every other method in the block; it becomes `impl core::EGraph<RelLang> { … }` (see Step 4). Any internal use of `self.canon_enode(x)` in retained methods becomes `self.canon(x)`.

- [ ] **Step 3: Move `available` into `RelGraphData`**

Update the two `self.available` sites:
- `set_available` (:394–396): body becomes `self.data.available = available;`
- the read at :1245: `if self.data.available.get(gid).is_some_and(|keys| !keys.is_empty())`

Verify there are no others: `grep -n "self.available" src/transform/src/eqsat/egraph.rs` must return nothing after this step.

- [ ] **Step 4: Re-key the retained inherent impl to the concrete instantiation**

Change the retained `impl EGraph {` header to `impl core::EGraph<RelLang> {` (equivalently `impl EGraph {` still works because `EGraph` is the alias — prefer leaving the header as `impl EGraph {` for minimal diff). Confirm the retained methods (`add_rel`, `arity`, `arity_guarded`, `column_types`, `column_types_guarded`, `reachable`, `child_classes`, `seed_indexed_filters`, `saturate`, `run_analysis`, `run_analysis_bounded`, `extract`, `extract_with`, …) compile: they may freely use the core methods (`self.add`, `self.find`, `self.union`, `self.rebuild`, `self.index`, `self.canon`) and the `pub(crate)` fields `self.classes`, `self.data`.

- [ ] **Step 5: Build and run the eqsat tests (behavior-neutral check)**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: PASS with no test modified and **no `REWRITE=1`**. Datadriven golden files unchanged (`git status` shows no `.txt`/`testdata` changes).

- [ ] **Step 6: Verify the rule-codegen build path still works**

Run: `bin/cargo-test -p mz-transform --no-run`
Expected: builds (this exercises `build.rs` generating `eqsat_rules.rs`, whose `fn(&EGraph, …)` pointers resolve through the alias).

- [ ] **Step 7: sqllogictest behavior-neutral smoke**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/*.slt`
Expected: all pass, zero failures (no golden diffs). If a transient infra error appears, ensure `COCKROACH_URL` / `METADATA_BACKEND_URL` point at the running cockroach (`postgres://root@localhost:26257`).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/egraph.rs
git commit -m "eqsat: instantiate relational engine on the generic core (SP1 task 2)"
```

---

### Task 3: Move the `Analysis` framework into the core (`Analysis<L>` + driver)

Move the `Analysis` trait and the `run_analysis`/`run_analysis_bounded` fixpoint driver into `core`, parameterized over `L` with a per-analysis `Ctx` associated type (replacing the relational-specific `arity` closure parameter). Migrate the five relational analyses. Behavior-neutral.

**Files:**
- Modify: `src/transform/src/eqsat/core.rs`
- Modify: `src/transform/src/eqsat/analysis.rs`
- Modify: `src/transform/src/eqsat/egraph.rs` (saturate's `run_analysis` call sites; remove the old driver if not already removed)

**Interfaces:**
- Produces: `pub trait Analysis<L: Language>` with `type Domain: Clone + Eq; type Ctx<'a>; fn bottom(&self) -> Self::Domain; fn make(&self, node: &L::Node, get: &dyn Fn(Id) -> Self::Domain, ctx: Self::Ctx<'_>) -> Self::Domain; fn merge(&self, a, b) -> Self::Domain;` and driver methods on `EGraph<L>`: `fn run_analysis_bounded<A: Analysis<L>>(&self, a: &A, ctx: A::Ctx<'_>, max_iters: usize) -> HashMap<Id, A::Domain>` and `fn run_analysis<A: Analysis<L>>(&self, a: &A, ctx: A::Ctx<'_>) -> HashMap<Id, A::Domain>`.
- The five relational analyses set `type Ctx<'a> = &'a dyn Fn(Id) -> usize;` (the arity provider). `Ctx` must be a copyable reference so it can be re-passed into each `make` call within the driver loop.

- [ ] **Step 1: Add the `Analysis<L>` trait and driver to `core.rs`**

Append to the non-test part of `core.rs`:

```rust
/// A per-e-class lattice analysis over language `L`. `Domain` is the lattice
/// element; `make` is the per-node transfer function (reading children via
/// `get` and a language-specific per-run `ctx`); `merge` joins two e-nodes'
/// facts within a class.
pub trait Analysis<L: Language> {
    type Domain: Clone + Eq;
    /// Per-run context handed to every `make` call (relational: an arity
    /// provider; scalar later: column types). A copyable borrow.
    type Ctx<'a>: Copy;

    fn bottom(&self) -> Self::Domain;
    fn make(
        &self,
        node: &L::Node,
        get: &dyn Fn(Id) -> Self::Domain,
        ctx: Self::Ctx<'_>,
    ) -> Self::Domain;
    fn merge(&self, a: Self::Domain, b: Self::Domain) -> Self::Domain;
}

impl<L: Language> EGraph<L> {
    /// Run a lattice analysis to a fixpoint (bounded), one fact per e-class.
    pub fn run_analysis_bounded<A: Analysis<L>>(
        &self,
        a: &A,
        ctx: A::Ctx<'_>,
        max_iters: usize,
    ) -> HashMap<Id, A::Domain> {
        let mut m: HashMap<Id, A::Domain> =
            self.classes.keys().map(|&id| (id, a.bottom())).collect();
        for iter in 0..max_iters {
            let mut updates: Vec<(Id, A::Domain)> = Vec::new();
            for (&id, nodes) in &self.classes {
                let get = |c: Id| m.get(&self.find(c)).cloned().unwrap_or_else(|| a.bottom());
                let mut d = a.bottom();
                for n in nodes {
                    d = a.merge(d, a.make(n, &get, ctx));
                }
                if m.get(&id) != Some(&d) {
                    updates.push((id, d));
                }
            }
            if updates.is_empty() {
                break;
            }
            if iter + 1 == max_iters {
                tracing::debug!(
                    "run_analysis: did not converge after {max_iters} iterations; \
                     returning partial (under-approximate) result"
                );
                for (id, d) in updates {
                    m.insert(id, d);
                }
                break;
            }
            for (id, d) in updates {
                m.insert(id, d);
            }
        }
        m
    }

    /// Run a lattice analysis to a fixpoint with the default iteration bound.
    pub fn run_analysis<A: Analysis<L>>(
        &self,
        a: &A,
        ctx: A::Ctx<'_>,
    ) -> HashMap<Id, A::Domain> {
        self.run_analysis_bounded(a, ctx, MAX_ANALYSIS_ITERS)
    }
}
```

Add `const MAX_ANALYSIS_ITERS: usize = …;` to `core.rs` by moving it from `egraph.rs` (find its value with `grep -n "MAX_ANALYSIS_ITERS" src/transform/src/eqsat/egraph.rs`), or `pub(crate)` it and import. Keep the same numeric value.

Note vs. the old driver: the `arity` closure built inside the old `run_analysis_bounded` (`let arity = |c| self.arity(c);`) is gone — `arity` is now supplied by the caller as `ctx`. Everything else is verbatim.

- [ ] **Step 2: Remove the old trait and driver from the relational side**

- In `analysis.rs`, delete the old `pub trait Analysis { … }` definition (:36–58); add `use crate::eqsat::core::{Id, Language, Analysis};` and `use crate::eqsat::egraph::RelLang;`.
- In `egraph.rs`, delete the old `run_analysis_bounded` (:1511) and `run_analysis` (:1554) method bodies (now in core).

- [ ] **Step 3: Migrate the five analyses to `Analysis<RelLang>`**

For each of `NonNeg` (:71), `Monotonic` (:115), `Keys` (:170), `Equivalences` (:273), `ConstantColumns` (:562) in `analysis.rs`:
- Change `impl Analysis for X` → `impl Analysis<RelLang> for X`.
- Add `type Ctx<'a> = &'a dyn Fn(Id) -> usize;` to each impl.
- Change the `make` signature from `fn make(&self, node: &ENode, get: &dyn Fn(Id) -> Self::Domain, arity: &dyn Fn(Id) -> usize)` to `fn make(&self, node: &ENode, get: &dyn Fn(Id) -> Self::Domain, ctx: Self::Ctx<'_>)`.
- At the top of each `make` body add `let arity = ctx;` so the existing body (which calls `arity(…)`) is otherwise unchanged. (Analyses that ignore arity simply never use the binding — silence with `let _arity = ctx;` if the compiler warns.)

- [ ] **Step 4: Update saturate's analysis call sites to pass the arity ctx**

In `egraph.rs` `saturate` (around :1342–:1378), before the analysis calls build the arity provider once and pass it to each call:

```rust
let arity = |c: Id| self.arity(c);
let arity_ctx: &dyn Fn(Id) -> usize = &arity;
```

Then:
- `self.run_analysis_bounded(&Equivalences { … }, MAX_EQUIVALENCES_ANALYSIS_ITERS)` → `self.run_analysis_bounded(&Equivalences { … }, arity_ctx, MAX_EQUIVALENCES_ANALYSIS_ITERS)`
- `self.run_analysis(&NonNeg { … })` → `self.run_analysis(&NonNeg { … }, arity_ctx)`
- `self.run_analysis(&Keys { … })` → `self.run_analysis(&Keys { … }, arity_ctx)`
- `self.run_analysis(&Monotonic { … })` → `self.run_analysis(&Monotonic { … }, arity_ctx)`

Also update the two test call sites (`run_analysis(&cc)` at :2431 and :2451): build a local `let arity = |c: Id| self.arity(c);` (or, in those tests, `egraph.arity`) and pass `&arity as &dyn Fn(Id) -> usize`.

(Borrow note: `arity` borrows `self` immutably and `run_analysis` borrows `self` immutably — two shared borrows, fine.)

- [ ] **Step 5: Build and run the eqsat tests (behavior-neutral check)**

Run: `bin/cargo-test -p mz-transform eqsat`
Expected: PASS, no golden changes, no `REWRITE=1`.

- [ ] **Step 6: Full transform sqllogictest behavior-neutral check**

Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/*.slt`
Expected: zero failures, zero golden diffs.

- [ ] **Step 7: Commit**

```bash
git add src/transform/src/eqsat/core.rs src/transform/src/eqsat/analysis.rs src/transform/src/eqsat/egraph.rs
git commit -m "eqsat: move Analysis framework into the generic core (SP1 task 3)"
```

---

## Final verification (after Task 3, before the whole-branch review)

- [ ] **Broad behavior-neutral corpus run.** With the cockroach container up (`COCKROACH_URL`/`METADATA_BACKEND_URL` set), run a representative slt sweep and confirm zero golden diffs:

  Run: `bin/sqllogictest --optimized -- test/sqllogictest/transform/*.slt test/sqllogictest/*.slt`
  Expected: failures are only pre-existing/unrelated (compare against a flag-free baseline if in doubt); no diffs attributable to this refactor. `git status` clean of golden changes.

- [ ] **Confirm the deferred surface is untouched.** `git diff --stat main_empty..HEAD` should show changes only in `eqsat/core.rs`, `eqsat/egraph.rs`, `eqsat/analysis.rs`, `eqsat.rs` (+ the design/plan/ledger docs). No changes to `saturate`'s logic beyond call-site argument threading, none to `dsl.rs`/`matcher.rs`/`build.rs`/`rules.rs`/`cost.rs`/`extract.rs`/`scalar/`.

---

## Self-Review notes (author)

- **Spec coverage:** SP1 success criteria → Task 1 (generic core + toy "second language" test), Task 2 (relational sole instance, behavior-neutral, `available` relocation, type-alias minimal churn), Task 3 (`Analysis<L>` framework + driver in core). "Deferred" list in the spec ⇒ the Global Constraints "Do NOT touch" list + the final deferred-surface check.
- **GAT open question (spec):** resolved here by putting `Ctx<'a>` on `Analysis` (not `Language`) — more flexible (per-analysis context) and the relational `Ctx = &dyn Fn(Id)->usize` keeps each `make` body unchanged via `let arity = ctx;`. This is within the spec's flagged flexibility; if the `Copy` bound on `Ctx` fights an analysis later, fall back to passing `&ctx`.
- **Type consistency:** `Id`, `Language::{Node,Sym,GraphData,children,map_children,symbol}`, `EGraph::{new,find,add,union,rebuild,canon,index}`, `Analysis::{Domain,Ctx,bottom,make,merge}`, and the two driver signatures are used identically across tasks.
- **Behavior-neutrality is the through-line:** Tasks 2 and 3 each end on the existing test suite + slt smoke with no golden diffs; the toy-language tests (Task 1) are the only new tests.
