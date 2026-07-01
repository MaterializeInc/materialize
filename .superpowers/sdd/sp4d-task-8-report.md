# SP4d Task 8 Report: `remove`-in-`close`

## Summary

`ColoredEGraph::close` now prunes redundant local edges from a child color's
delta union-find after the congruence fixpoint. Any edge already provided by
an ancestor color is removed, keeping each color's delta minimal.

## Pruning Logic in `close` (congruence.rs)

Two passes run after `recanonicalize_delta_nodes`:

### Phase 1 — remove "dead-code" entries

A tracked id `x` in color `c`'s local UF is _dead code_ if
`find_in_ancestors(c, x) != x`.

**Why this is the right condition:** `find(c, y)` applies the ancestor chain
first (producing `anc_y`), then applies `c`'s UF to `anc_y`. If the ancestor
chain already maps `x` to a different node `anc_x ≠ x`, then `c`'s UF entry
for `x` is never reached in the layered find path (the UF receives `anc_x` as
input, not `x`). The entry is unreachable and can be safely removed.

`ColoredUnionFind::remove` reattaches children of `x` to `x`'s parent's root,
so other members of the same local class remain merged.

### Phase 2 — remove solo roots

After phase 1, a root `r` (self-loop `r → r`) with no remaining non-root
members pointing to it is a _solo root_. Its presence is equivalent to
having no entry: `find_local_c(r) == r` in both cases. It is removed.

Solo roots arise when all non-root members of a class were removed in phase 1
(all were dead code). For example: parent merges `a ≅ b` (a is parent's root,
b → a). Child (created before parent closes) merges `a ≅ b` locally. Phase 1
removes `b` (ancestor moves b to a ≠ b). Phase 2 removes `a` (no remaining
non-root member, and removing a solo root doesn't change any find result).

## Helper: `find_in_ancestors`

Added to `congruence.rs` as a private `&self` method on `ColoredEGraph`:

```rust
fn find_in_ancestors(&self, c: ColorId, x: Id) -> Id {
    let mut r = if x < self.base.uf_len() { self.base.find(x) } else { x };
    for cid in self.chain_top_down(c) {
        if cid == c { break; }
        r = self.colors[cid.0].uf.find_local_ref(r);
    }
    r
}
```

Uses `find_local_ref` (non-compressing, added to `ColoredUnionFind`) to avoid
borrow conflicts: both `c`'s UF and the ancestor UFs can be read `&self` while
collecting `tracked_ids` as an owned `Vec<Id>`.

## Membership-Invariance Argument

**Phase 1:** For each removed `x` (where `anc_x ≠ x`):
- Before: `find(c, x) = find_local_c(anc_x)`.
- After: `find(c', x) = find_local_c'(anc_x)`.
- `x`'s entry only affects paths that look up `x` directly in `c`'s UF. The
  layered find supplies `anc_x` (not `x`) to `c`'s UF, so `x`'s entry is
  never on any live path. Removing it leaves `find_local_c(anc_x)` unchanged. ✓
- `remove` reattaches `x`'s children to the root, preserving their membership. ✓

**Phase 2:** For each removed solo root `r`:
- `find_local_c(r) = r` (self-loop). After removal: `find_local_c'(r) = r`
  (no entry → own rep). Same result. ✓
- No non-root in `c`'s UF points to `r` (solo), so no other node's
  `find_local_c` path goes through `r`. ✓

## New Methods

### `union_find.rs`
- `find_local_ref(&self, id) -> Id` — non-compressing find (avoids `&mut`
  borrow conflicts in the pruning check)
- `tracked_ids(&self) -> impl Iterator<Item = Id>` — keys of the parent map
- `#[cfg(test)] local_delta_is_empty(&self) -> bool` — test accessor

### `colored.rs`
- `#[cfg(test)] color_delta_is_empty(&self, c) -> bool` — delegates to
  `self.colors[c.0].uf.local_delta_is_empty()`

## New Tests (`congruence.rs`)

### `remove_in_close_prunes_redundant_child_edge`
- Creates a parent and child color; adds `a ≅ b` to child's UF directly
  (via `ceg.union`) _before_ the parent is closed.
- Closes the parent with `a ≅ b` — making the child's local edge redundant.
- Calls `close(child, &[])` — the pruning pass removes the redundant edge.
- Asserts: (1) `find(child, a) == find(child, b)` (membership preserved), and
  (2) `color_delta_is_empty(child) == true` (delta was pruned).

**TDD RED:** Before the pruning implementation, the test panicked at the
`color_delta_is_empty` assertion (child delta was non-empty). FAIL. ✓
**TDD GREEN:** After implementation, both assertions pass. ✓

### `close_keeps_non_redundant_child_edge`
- Parent provides no equalities. Child closes with `a ≅ b`.
- Asserts membership holds AND `color_delta_is_empty(child) == false`
  (the non-redundant edge is kept).

## Oracle-Test Status

All oracle differential tests pass:
- `close_matches_oracle_fixed` ✓
- `close_matches_oracle_random` ✓
- `close_matches_oracle_hierarchy` ✓

The oracle independently computes colored partitions and agrees with the
layered-find mechanism after pruning. This confirms membership invariance.

## Gate Results

| Check | Result |
|-------|--------|
| `bin/cargo-test -p mz-transform colored` | 54/54 ✓ |
| `bin/cargo-test -p mz-transform` | 357/357 ✓ |
| `cargo clippy -p mz-transform --all-targets -- -D warnings` | Clean ✓ |

## Deviations from Brief

- The brief's example test used `EGraph::new()` (CombinedLang) with
  `EScalar::plain(MirScalarExpr::column(0))`. The implemented test uses
  `EGraph::<ToyLang>::new()` with `ToyNode::Leaf`, which is consistent with
  the existing `congruence.rs` test infrastructure and tests the same behavior.
- The brief anticipated the test would fail because the child's UF has an entry
  before pruning. The direct-`union` approach (adding the edge before the parent
  closes) was needed to create a genuinely redundant entry; asserting the same
  equality through `close(child, [(a, b)])` with the parent already closed is a
  no-op (the layered `find` prevents adding a redundant entry).
- The pruning condition was refined from the brief's `find_in_ancestors(x) ==
  find_in_ancestors(local_root(x))` to the two-phase `anc_x != x` + solo-root
  approach, to avoid incorrectly marking roots as redundant (a root's trivial
  self-comparison would always match).

## Fix (T8b)

### Fix 1 (Important) — 3-id removal test

Added `remove_in_close_3id_preserves_3way_class` to `congruence.rs` tests.

**What it tests:** Three leaves `a`, `b`, `d` in a two-level color hierarchy.
Before the parent closes, the child's local UF is seeded with both `a ≅ b`
(which the parent will also provide — redundant) and `d ≅ a` (child-only).
Child's UF starts with 3 tracked ids.

After closing the parent (`a ≅ b`) and then the child (no new equalities):
- Phase-1 pruning removes `b` because `find_in_ancestors(child, b) = a ≠ b`.
- `d ≅ a` is child-only and survives; child's UF shrinks from 3 → 2 tracked ids.

**Why this triggers removal:** `b` in child's UF is unreachable via the layered
find (the ancestor chain maps `b → a` before consulting child's UF), so it is
dead code. The previous oracle-hierarchy test never triggered this path because
equalities were applied only through `close` (which canonicalizes through the
layered find first, so a redundant entry is never inserted).

**Partition checked via oracle:** The test calls `oracle_close` with the union
of all asserted equalities (`(a,b)` and `(d,a)`) and then calls `same_partition`
on `[a, b, d]` — a differential check confirming the 3-way class was not split
by the prune. A new `#[cfg(test)] color_delta_tracked_count` helper on
`ColoredEGraph` (delegating to `uf.tracked_ids().count()`) lets the test assert
the delta shrank from 3 → 2, confirming removal actually happened.

**Test would fail on over-removal:** If pruning mistakenly removed `d` (or `a`
after `b` is removed), `find(child, d)` would lose its child-local merge and the
`same_partition` call would disagree with the oracle.

### Fix 2 (Minor) — recanonicalize after pruning

Moved the `recanonicalize_delta_nodes(c)` call from BEFORE the prune block to
AFTER it in `ColoredEGraph::close`.

**Why it's safe:** The prune logic reads only the union-find entries (via
`find_in_ancestors` / `find_local_ref`) — it never inspects delta-node keys.
Reordering does not change which ids are pruned.

**Why it matters:** Pruning can reassign the representative of a live class
(when a dead root had a live child, `remove` promotes that child as the new
root). If `recanonicalize_delta_nodes` ran before the prune, delta-node keys
would be keyed against a stale representative. Running it after the prune
ensures delta-node keys reflect the final partition. This is a no-op today
(no colored-conclusion delta nodes until Task 10+) but is the correct invariant
for future tasks.

### Gate Results (T8b)

| Check | Result |
|-------|--------|
| `bin/cargo-test -p mz-transform colored` | 55/55 ✓ (new test passes) |
| `bin/cargo-test -p mz-transform` | 358/358 ✓ |
| `cargo clippy -p mz-transform --all-targets -- -D warnings` | Clean ✓ |
