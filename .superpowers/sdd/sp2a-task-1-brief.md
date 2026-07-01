## Task 1: Generic core — analysis hooks + accessors

**Files:**
- Modify: `src/transform/src/eqsat/core.rs`
- Test: `src/transform/src/eqsat/core.rs` (its `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `Language`, `EGraph<L>`, `Id` from `core.rs`.
- Produces:
  - `Language::on_add(data: &mut Self::GraphData, id: Id, node: &Self::Node, get: &dyn Fn(Id) -> Id)` — default no-op.
  - `Language::on_union(data: &mut Self::GraphData, winner: Id, loser: Id)` — default no-op.
  - `EGraph<L>::nodes(&self, id: Id) -> Vec<L::Node>` (`pub(crate)`), `class_ids(&self) -> Vec<Id>` (`pub(crate)`), `node_count(&self) -> usize` (`pub(crate)`), `data(&self) -> &L::GraphData` (`pub(crate)`), `data_mut(&mut self) -> &mut L::GraphData` (`pub(crate)`).

- [ ] **Step 1: Write the failing test**

Add to `core.rs`'s `mod tests` a toy language whose `GraphData` is maintained by the hooks, proving `on_add` reads children via `get` and `on_union` runs in the right direction, and that `EGraph::data()`/`nodes()` expose them. Reuse the existing `Arith`/`ArithSym` types.

```rust
    // A toy analysis: each class carries the integer value of its (single) node,
    // maintained incrementally by the hooks. Exercises on_add (reading children
    // via `get`) and on_union (folding loser into winner).
    #[derive(Default)]
    struct SumData {
        vals: HashMap<Id, i64>,
    }

    struct SumLang;

    impl Language for SumLang {
        type Node = Arith;
        type Sym = ArithSym;
        type GraphData = SumData;

        fn children(node: &Arith) -> Vec<Id> {
            ArithLang::children(node)
        }
        fn map_children(node: &Arith, f: impl Fn(Id) -> Id) -> Arith {
            ArithLang::map_children(node, f)
        }
        fn symbol(node: &Arith) -> ArithSym {
            ArithLang::symbol(node)
        }
        fn on_add(data: &mut SumData, id: Id, node: &Arith, get: &dyn Fn(Id) -> Id) {
            let v = match node {
                Arith::Num(x) => *x,
                Arith::Add(a, b) => data.vals[&get(*a)] + data.vals[&get(*b)],
                Arith::Mul(a, b) => data.vals[&get(*a)] * data.vals[&get(*b)],
            };
            data.vals.insert(id, v);
        }
        fn on_union(data: &mut SumData, winner: Id, loser: Id) {
            // Loser folds into winner: drop the loser's entry, keep the winner's.
            let lo = data.vals.remove(&loser);
            if data.vals.get(&winner).is_none() {
                if let Some(lo) = lo {
                    data.vals.insert(winner, lo);
                }
            }
        }
    }

    #[mz_ore::test]
    fn on_add_reads_children_through_get() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(2));
        let b = eg.add(Arith::Num(3));
        let s = eg.add(Arith::Add(a, b));
        assert_eq!(eg.data().vals[&s], 5, "on_add must combine child analyses");
        assert_eq!(eg.nodes(s).len(), 1, "nodes() exposes the class contents");
    }

    #[mz_ore::test]
    fn on_union_folds_loser_into_winner() {
        let mut eg = EGraph::<SumLang>::new();
        let a = eg.add(Arith::Num(7));
        let b = eg.add(Arith::Num(9));
        // union(a, b): winner = find(a), loser = find(b). Loser's entry is dropped.
        eg.union(a, b);
        let w = eg.find(a);
        assert!(eg.data().vals.contains_key(&w), "winner keeps an entry");
        assert_eq!(eg.data().vals.len(), 1, "loser entry was removed on union");
    }

    #[mz_ore::test]
    fn default_hooks_are_noop_for_unit_graphdata() {
        // ArithLang has GraphData = () and does not override the hooks; add/union
        // must still work (defaults are no-ops).
        let mut eg = EGraph::<ArithLang>::new();
        let a = eg.add(Arith::Num(1));
        let b = eg.add(Arith::Num(2));
        eg.union(a, b);
        eg.rebuild();
        assert_eq!(eg.find(a), eg.find(b));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bin/cargo-test -p mz-transform eqsat::core 2>&1 | tail -30`
Expected: compile errors — `no method named data` / `no method named nodes` on `EGraph`, and `no method on_add/on_union in trait Language` / missing items, because the hooks and accessors don't exist yet.

- [ ] **Step 3: Add the hook methods to the `Language` trait**

In `core.rs`, add the two default methods to `trait Language` (after `symbol`):

```rust
    /// Hook fired when `add` creates a NEW e-class `id` for `node` (not on a
    /// hash-cons hit). `get` resolves a child id to its canonical root. Lets a
    /// language maintain `GraphData`-resident per-class analysis incrementally.
    /// Default: no-op (the relational engine uses the batch `Analysis` driver).
    fn on_add(
        _data: &mut Self::GraphData,
        _id: Id,
        _node: &Self::Node,
        _get: &dyn Fn(Id) -> Id,
    ) {
    }

    /// Hook fired when `union` folds `loser`'s class into `winner`'s. Lets a
    /// language merge its `GraphData`-resident analysis. Default: no-op.
    fn on_union(_data: &mut Self::GraphData, _winner: Id, _loser: Id) {}
```

- [ ] **Step 4: Add a `find_in` free helper and route `find` through it**

In `core.rs`, above `impl<L: Language> EGraph<L>`, add:

```rust
/// Follow union-find parent pointers in `uf` from `id` to its root. Non-
/// compressing: reads only `uf`, so it can run while another field of the
/// e-graph is mutably borrowed (the `on_add` hook closure).
fn find_in(uf: &[Id], mut id: Id) -> Id {
    while uf[id] != id {
        id = uf[id];
    }
    id
}
```

Replace the body of `find` to reuse it:

```rust
    /// The canonical id of `id`.
    pub fn find(&self, id: Id) -> Id {
        find_in(&self.uf, id)
    }
```

- [ ] **Step 5: Fire `on_add` from `add` and `on_union` from `union`**

Replace `add`'s tail so the hook fires for genuinely new classes, before the memo insert (so `node` is still owned):

```rust
    /// Add an e-node, returning its (canonical) e-class. Hash-conses.
    pub fn add(&mut self, node: L::Node) -> Id {
        let node = self.canon(&node);
        if let Some(&id) = self.memo.get(&node) {
            return self.find(id);
        }
        let id = self.new_class();
        self.classes.get_mut(&id).unwrap().insert(node.clone());
        // Maintain language analysis for the new class. Borrow `uf` and `data`
        // as disjoint fields so the `get` closure and `&mut data` don't alias.
        let uf = &self.uf;
        L::on_add(&mut self.data, id, &node, &|c| find_in(uf, c));
        self.memo.insert(node, id);
        id
    }
```

Replace `union`'s tail so the hook fires after the fold, with winner = `ra`, loser = `rb`:

```rust
    /// Union the classes of `a` and `b`; returns whether they were distinct.
    pub fn union(&mut self, a: Id, b: Id) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        self.uf[rb] = ra;
        let nodes = self.classes.remove(&rb).unwrap_or_default();
        self.classes.entry(ra).or_default().extend(nodes);
        L::on_union(&mut self.data, ra, rb);
        true
    }
```

- [ ] **Step 6: Add the `pub(crate)` accessors**

In `impl<L: Language> EGraph<L>` (e.g. just after `index`), add:

```rust
    /// The set of e-nodes in `id`'s canonical class. Empty if `id` is unknown.
    pub(crate) fn nodes(&self, id: Id) -> Vec<L::Node> {
        let rep = self.find(id);
        self.classes
            .get(&rep)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The canonical ids of all live e-classes.
    pub(crate) fn class_ids(&self) -> Vec<Id> {
        self.classes.keys().copied().collect()
    }

    /// Total number of e-nodes across all classes.
    pub(crate) fn node_count(&self) -> usize {
        self.classes.values().map(|ns| ns.len()).sum()
    }

    /// The language-owned auxiliary state.
    pub(crate) fn data(&self) -> &L::GraphData {
        &self.data
    }

    /// Mutable access to the language-owned auxiliary state.
    pub(crate) fn data_mut(&mut self) -> &mut L::GraphData {
        &mut self.data
    }
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `bin/cargo-test -p mz-transform eqsat::core 2>&1 | tail -30`
Expected: PASS, including `on_add_reads_children_through_get`, `on_union_folds_loser_into_winner`, `default_hooks_are_noop_for_unit_graphdata`, and the pre-existing `add_hashconses_identical_nodes`, `congruence_collapses_after_union`, `index_buckets_by_symbol`.

- [ ] **Step 8: Run the full eqsat suite to confirm the relational engine is unperturbed**

Run: `bin/cargo-test -p mz-transform eqsat 2>&1 | tail -30`
Expected: all eqsat tests pass (relational 226 + the existing scalar tests, which still use the old `ScalarEGraph` struct at this point). The hook additions are no-ops for `RelLang` and the still-present scalar struct.

- [ ] **Step 9: Commit**

```bash
git add src/transform/src/eqsat/core.rs
git commit -m "eqsat/core: add on_add/on_union analysis hooks + class accessors

Default-no-op hooks let a Language maintain GraphData-resident per-class
analysis incrementally; the core fires them from add/union. Relational
keeps the defaults. Adds pub(crate) nodes/class_ids/node_count/data/
data_mut accessors for the upcoming scalar saturate driver.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01QNZHc5J4bdG29HzPbaCu7H"
```

---

