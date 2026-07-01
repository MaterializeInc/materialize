## Task 7: Hierarchical color forest from the fact lattice

**Files:**
- Modify: `src/transform/src/eqsat/colored_derive.rs` (`build_colored_layer` ~124; add forest derivation)
- Modify: `src/transform/src/eqsat/colored.rs` (`new_color(parent)` already supports a parent; add `all_colors_parent_first()` / expose color order)
- Test: `colored_derive.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `DerivedScopes { scopes: Vec<ScopeEqualities>, class_scope, empty_classes }` (colored_derive.rs:87).
- Produces: `build_colored_layer` builds a forest: colors = distinct equality-sets; `parent(C)` = a maximal proper subset (deterministic tie-break: most equalities, then canonical `Vec<(Id,Id)>` order); each child color created with `new_color(Some(parent))` and applied only its **delta** equalities.

Derivation (replace the flat loop at build_colored_layer:126-133):
```rust
// scopes[i].unions are sorted-deduped equality-sets (Id,Id).
// 1. Order scopes by ascending |unions| so parents precede children.
let mut order: Vec<usize> = (0..scopes.scopes.len()).collect();
order.sort_by_key(|&i| (scopes.scopes[i].unions.len(), scopes.scopes[i].unions.clone()));
// 2. For each scope, find parent = the already-placed scope whose unions ⊂ this,
//    maximal by |unions|, tie-broken by the canonical order above.
let mut color_ids = vec![None; scopes.scopes.len()];
for &i in &order {
    let mine: BTreeSet<(Id,Id)> = scopes.scopes[i].unions.iter().copied().collect();
    let mut parent: Option<usize> = None;
    for &j in &order {
        if j == i || color_ids[j].is_none() { continue; }
        let theirs: &Vec<(Id,Id)> = &scopes.scopes[j].unions;
        if theirs.len() < mine.len() && theirs.iter().all(|e| mine.contains(e)) {
            // proper subset; keep the maximal (order is ascending, so last wins)
            parent = Some(j);
        }
    }
    let pcolor = parent.map(|j| color_ids[j].unwrap());
    let cid = ceg.new_color(pcolor);
    color_ids[i] = Some(cid);
    // Apply only the delta equalities (those not in the parent chain).
    let parent_eqs: BTreeSet<(Id,Id)> = match parent {
        Some(j) => scopes.scopes[j].unions.iter().copied().collect(),
        None => BTreeSet::new(),
    };
    for &(a, b) in &scopes.scopes[i].unions {
        if !parent_eqs.contains(&(a, b)) {
            ceg.union(color_ids[i].unwrap(), base.find(a), base.find(b));
        }
    }
}
```
(`color_of`/`empty_classes` mapping unchanged, using `color_ids[idx].unwrap()`.)

- [ ] **Step 1: Write the failing test** — three nested scopes ∅⊂{e1}⊂{e1,e2} form a chain:

```rust
#[mz_ore::test]
fn color_forest_is_inclusion_ordered() {
    // Construct DerivedScopes with scopes [{e1,e2}, {e1}] (out of order) and
    // assert the built layer makes {e1,e2}'s color a child of {e1}'s color.
    // ... build base eg with scalar ids a,b,c so e1=(a,b), e2=(a,c) ...
    let layer = build_colored_layer(&eg, scopes);
    let child = layer.color_for_unions(&[e1, e2]);
    let parent = layer.color_for_unions(&[e1]);
    assert_eq!(layer.ceg.parent_of(child), Some(parent));
}
```
(Add small test-only accessors `color_for_unions` / `parent_of` if needed, or assert via `chain_top_down(child)` containing `parent`.)

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform color_forest_is_inclusion_ordered`
Expected: FAIL.

- [ ] **Step 3: Implement** the forest derivation + color order accessor.

- [ ] **Step 4: Run to verify it passes; full suite byte-identical**

Run: `bin/cargo-test -p mz-transform color_forest_is_inclusion_ordered && bin/cargo-test -p mz-transform`
Expected: PASS; same count; zero golden diffs (forest not yet feeding extraction).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored_derive.rs src/transform/src/eqsat/colored.rs
git commit -m "SP4d P3: hierarchical color forest from the fact lattice"
```

