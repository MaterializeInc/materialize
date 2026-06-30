## Task 9: Dirty-set incremental `close`

**Files:**
- Modify: `src/transform/src/eqsat/colored/congruence.rs` (`close` ~85)
- Test: `colored/congruence.rs` `#[cfg(test)]`

**Interfaces:**
- Produces: `close` re-examines only nodes whose canonical children changed since the last batch, to a fixpoint; a `#[cfg(debug_assertions)]` check that the incremental fixpoint equals the full-pass fixpoint.

Maintain a `dirty: VecDeque<Id>` seeded with the nodes touched by the applied equalities (the classes whose reps changed). Process a node by recomputing `canon(color, node)` and merging congruent nodes; when a merge changes a rep, enqueue the parents of the affected classes. Keep the existing full-pass loop behind `#[cfg(any(test, debug_assertions))]` and assert the two reach the same partition.

- [ ] **Step 1: Write the failing test** — incremental == full-pass on a seeded congruence:

```rust
#[mz_ore::test]
fn incremental_close_equals_full_pass() {
    // Build a base with f(x) and f(y) nodes; union x≅y in a color; closing must
    // merge f(x)≅f(y). Run close (incremental) and assert the resulting
    // partition equals a from-scratch full-pass close on a clone.
    // (Use the existing oracle helper in colored/oracle.rs to compare partitions.)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bin/cargo-test -p mz-transform incremental_close_equals_full_pass`
Expected: FAIL (until the incremental path + accessor exist) — or PASS trivially if full-pass is still the only path; in that case first introduce the incremental path so the assertion is meaningful.

- [ ] **Step 3: Implement** the dirty-set incremental closure + the debug-assertion parity check.

- [ ] **Step 4: Run to verify it passes; full colored + oracle suite**

Run: `bin/cargo-test -p mz-transform colored::`
Expected: PASS.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p mz-transform --all-targets -- -D warnings
git add src/transform/src/eqsat/colored/congruence.rs
git commit -m "SP4d P4: incremental dirty-tracking congruence in close"
```

---

# Phase P5 — extraction integration + wiring + goldens

