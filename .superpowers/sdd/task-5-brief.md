## Task 5: `ColoredView` — `MatchGraph`/`ApplyGraph` over `ColoredEGraph` at a color

**Files:**
- Create: `src/transform/src/eqsat/colored/view.rs`
- Modify: `src/transform/src/eqsat/colored.rs` (`mod view;`)
- Test: `colored/view.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredEGraph::{find,union,canon,visible_nodes,colored_class_members,add_colored}` (`colored.rs`/submodules); `MatchGraph`/`ApplyGraph` (Task 1).
- Produces: `ColoredView<'a,'b>` with `fn new(ceg: &'a mut ColoredEGraph<'b, CombinedLang>, color: ColorId, base: &'b EGraph) -> Self`; a `ColoredIndex = HashMap<Sym, Vec<(Id, ENode)>>` built from `visible_nodes(color)` under `canon`; impls of both traits.

```rust
pub(crate) struct ColoredView<'a, 'b> {
    pub ceg: &'a mut ColoredEGraph<'b, CombinedLang>,
    pub color: ColorId,
    pub base: &'b EGraph,
    /// EScalar for colored-delta scalar ids created during colored saturation
    /// (base ids resolve via base.data()). Populated on intern_scalar.
    pub delta_escalar: HashMap<Id, EScalar>,
    /// Per-color sym index over canon(color, ·) of visible_nodes(color).
    pub index: HashMap<Sym, Vec<(Id, ENode)>>,
}
```

Key implementation points:
- **`index` build:** for each `(id, node)` in `ceg.visible_nodes(color)`, if `CNode::Rel(e)`, push `(ceg.find(color, id), canon-resolved e)` under `e.sym()`. Build once per driver round (Task 6 rebuilds after each `close`).
- **`rel_class_ids`:** canonical-under-color reps of base rel classes plus colored rel delta classes — derive from the `index` values' ids (dedup).
- **`rel_class_nodes(id)`:** `index` entries whose canonical id equals `ceg.find(color, id)`.
- **`arity(id)`:** `base.try_arity(rep)` for a base member of the colored class (arity is congruence-invariant); fall back through `colored_class_members`.
- **`escalar(id)`:** `self.delta_escalar.get(&id).cloned().unwrap_or_else(|| base.data().escalar(id).clone())`.
- **color-exact conds:** reuse the existing free fns, supplying a resolver `|id| self.escalar(id)` for payload reads and `self.arity` for arities; structural conds (`not_rel_empty` etc.) read the colored class (a class is empty iff its color is unsat — Task 6/derive supplies that, else delegate to a base member).
- **analysis-gated conds:** return `false` (never reached for tagged rules; the build-time assertion guarantees it).
- **`ApplyGraph::add_rel`:** `self.ceg.add_colored(self.color, CNode::Rel(node))`.
- **`ApplyGraph::intern_scalar`:** lower the EScalar's expr to an `SNode` (via `crate::eqsat::scalar::lower::lower_into` against a colored sink — or hash-cons the scalar tree through `add_colored` for each `CNode::Scalar`), obtain a delta id, insert `delta_escalar.insert(id, e.clone())`, return id. (If `lower_into` requires `&mut EGraph`, add a small colored lowering helper that walks the expr and `add_colored`s each scalar node; the EScalar is already in hand for the cache.)

- [ ] **Step 1: Write the failing test** (`colored/view.rs`)

```rust
#[mz_ore::test]
fn colored_view_reads_canonicalize_under_color() {
    let mut eg = EGraph::new();
    let leaf = eg.add(CNode::Rel(ENode::Constant { card: 1, arity: 2, col_types: None }));
    let c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    let c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
    let _ = leaf;
    eg.rebuild();
    let mut ceg = ColoredEGraph::new(&eg);
    let color = ceg.new_color(None);
    ceg.union(color, eg.find(c0), eg.find(c1)); // #0 ≅_c #1
    let view = ColoredView::new(&mut ceg, color, &eg);
    // Under the color, #0 and #1 canonicalize to the same id.
    assert_eq!(view.ceg.find(color, eg.find(c0)), view.ceg.find(color, eg.find(c1)));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform colored_view_reads_canonicalize_under_color`
Expected: FAIL — `ColoredView` not found.

- [ ] **Step 3: Implement `ColoredView` + `ColoredIndex` + both trait impls** as above.

- [ ] **Step 4: Run to verify it passes**

Run: `bin/cargo-test -p mz-transform colored_view`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/view.rs src/transform/src/eqsat/colored.rs
git commit -m "SP4d P2: ColoredView MatchGraph/ApplyGraph over a color"
```

