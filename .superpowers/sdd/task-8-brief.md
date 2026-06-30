## Task 8: `remove`-in-`close`

**Files:**
- Modify: `src/transform/src/eqsat/colored/congruence.rs` (`close` ~85)
- Modify: `src/transform/src/eqsat/colored/union_find.rs` (already has `remove`)
- Test: `colored/congruence.rs` `#[cfg(test)]`

**Interfaces:**
- Consumes: `ColoredUnionFind::remove` (union_find.rs:69), `chain_top_down`.
- Produces: `close` drops, from a child color's delta, any edge an ancestor color already provides (so the child's delta stays minimal).

In `close`, after applying equalities and reaching the congruence fixpoint, for each colored-class edge in this color's delta, if the same pair is already merged in the parent chain (`chain_top_down(color)` minus `color`), call `self.colors[color].uf.remove(id)` for the redundant local rep. The class membership (via the layered `find`) is unchanged because the parent still provides the edge; only the duplicate child-local edge is removed.

- [ ] **Step 1: Write the failing test** — a child edge subsumed by the parent is removed but membership is preserved:

```rust
#[mz_ore::test]
fn remove_in_close_prunes_redundant_child_edge() {
    let mut eg = EGraph::new();
    let a = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
    let b = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
    eg.rebuild();
    let mut ceg = ColoredEGraph::new(&eg);
    let parent = ceg.new_color(None);
    ceg.close(parent, &[(eg.find(a), eg.find(b))]);
    let child = ceg.new_color(Some(parent));
    // Re-assert the same edge in the child; close should remove it as redundant.
    ceg.close(child, &[(eg.find(a), eg.find(b))]);
    // Membership preserved (inherited from parent) ...
    assert_eq!(ceg.find(child, eg.find(a)), ceg.find(child, eg.find(b)));
    // ... but the child's local delta no longer stores the edge.
    assert!(ceg.color_delta_is_empty(child)); // small test accessor
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform remove_in_close_prunes_redundant_child_edge`
Expected: FAIL.

- [ ] **Step 3: Implement** the redundancy check + `remove` call in `close`; add `color_delta_is_empty` test accessor.

- [ ] **Step 4: Run to verify it passes; full colored suite green**

Run: `bin/cargo-test -p mz-transform colored::`
Expected: PASS (the SP3b oracle/congruence tests must still agree).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/congruence.rs src/transform/src/eqsat/colored/union_find.rs
git commit -m "SP4d P3: remove-in-close prunes ancestor-provided child edges"
```

---

# Phase P4 — incremental dirty-tracking congruence

