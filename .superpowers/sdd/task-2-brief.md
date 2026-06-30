### Task 2: Differential emitter (`eqsat/join_commit.rs`)

**Files:**
- Create: `src/transform/src/eqsat/join_commit.rs`
- Modify: `src/transform/src/eqsat.rs` (register the module)
- Modify: `src/transform/src/join_implementation.rs` (make three helpers
  `pub(crate)`: `implement_arrangements:944`, `permute_order:1099`,
  `install_lifted_mfp:1048`)

**Interfaces:**
- Consumes: `JoinOrder`/`JoinStep` from Task 1; the JI helpers
  `implement_arrangements(inputs: &mut [MirRelationExpr], available:
  &[Vec<Vec<MirScalarExpr>>], needed: impl Iterator<Item=&'a (usize,
  Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>) -> (MapFilterProject,
  Vec<Option<Vec<usize>>>)`, `permute_order(order: &mut Vec<(usize,
  Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>, lifted_projections:
  &[Option<Vec<usize>>])`, `install_lifted_mfp(new_join: &mut MirRelationExpr,
  mfp: MapFilterProject)`.
- Produces: `pub(crate) fn commit_differential(join: MirRelationExpr, order:
  JoinOrder, available: &[Vec<Vec<MirScalarExpr>>]) -> Option<MirRelationExpr>`.
  Consumed by Task 3's routing.

- [ ] **Step 1: Make the three JI helpers `pub(crate)`**

In `src/transform/src/join_implementation.rs`:
- Line 944: `fn implement_arrangements<'a>(` → `pub(crate) fn implement_arrangements<'a>(`
- Line 1048: `fn install_lifted_mfp(` → `pub(crate) fn install_lifted_mfp(`
- Line 1099: `fn permute_order(` → `pub(crate) fn permute_order(`

- [ ] **Step 2: Register the module**

In `src/transform/src/eqsat.rs`, next to `pub mod raise;` (line 39), add:

```rust
pub(crate) mod join_commit;
```

- [ ] **Step 3: Write the failing test**

Create `src/transform/src/eqsat/join_commit.rs` with only the test first (so it
fails to compile on the missing `commit_differential`), then add the impl in
Step 5. Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{Id, JoinImplementation, LocalId, MirRelationExpr};
    use mz_repr::{ColumnType, RelationType, ScalarType};

    use crate::eqsat::cost::{JoinOrder, JoinStep};

    fn get(local: u64, arity: usize) -> MirRelationExpr {
        let typ = RelationType::new(
            (0..arity)
                .map(|_| ColumnType {
                    scalar_type: ScalarType::Int32,
                    nullable: true,
                })
                .collect(),
        );
        MirRelationExpr::Get {
            id: Id::Local(LocalId::new(local)),
            typ,
            access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
        }
    }

    fn step(input: usize, key_cols: &[usize]) -> JoinStep {
        JoinStep {
            input,
            key_cols: key_cols.iter().copied().collect(),
        }
    }

    #[mz_ore::test]
    fn commit_differential_builds_expected_shape() {
        // 3-input join, no available arrangements.
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs,
            vec![
                vec![mz_expr::MirScalarExpr::column(0), mz_expr::MirScalarExpr::column(2)],
                vec![mz_expr::MirScalarExpr::column(2), mz_expr::MirScalarExpr::column(4)],
            ],
        );
        // Order: start at input 1 (key on local col 0), then input 0 (key local
        // col 0), then input 2 (key local col 0).
        let order = JoinOrder {
            steps: vec![step(1, &[0]), step(0, &[0]), step(2, &[0])],
        };
        let available = vec![Vec::new(); 3];

        let out = commit_differential(join, order, &available)
            .expect("commit must succeed on a 3-input join");

        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join, got {out:?}");
        };
        match implementation {
            JoinImplementation::Differential((start, start_key, _), rest) => {
                assert_eq!(*start, 1, "start input");
                assert_eq!(start_key.as_ref().map(|k| k.len()), Some(1), "start key len");
                assert_eq!(rest.len(), 2, "two remaining inputs");
                assert_eq!(rest[0].0, 0);
                assert_eq!(rest[1].0, 2);
            }
            other => panic!("expected Differential, got {other:?}"),
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `bin/cargo-test -p mz-transform commit_differential`
Expected: FAIL — `commit_differential` not found (does not compile).

- [ ] **Step 5: Write the emitter**

Prepend to `src/transform/src/eqsat/join_commit.rs` (above the `#[cfg(test)]`
module):

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Cost-model-native join commit for eqsat extraction.
//!
//! Turns a left-deep [`JoinOrder`] chosen by the cost model into a committed
//! `MirRelationExpr::Join` with `JoinImplementation::Differential`, reusing
//! `JoinImplementation`'s mechanical lowering helpers
//! (`implement_arrangements` / `permute_order` / `install_lifted_mfp`). It does
//! not call JI's `optimize_orders` planner, `differential::plan`, or
//! `canonicalize_equivalences`: the order and keys come entirely from the cost
//! model, and the equivalences are left as extraction spelled them.

use mz_expr::{JoinImplementation, JoinInputCharacteristics, MirRelationExpr, MirScalarExpr};

use crate::eqsat::cost::JoinOrder;

/// Commit `join` (a bare, `Unimplemented` `Join`) to a `Differential` plan that
/// follows `order`. `available` gives each input's existing arrangement keys.
/// Returns `None` (caller keeps the bare join) if `join` is not a `Join` or the
/// order is empty.
pub(crate) fn commit_differential(
    mut join: MirRelationExpr,
    order: JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
) -> Option<MirRelationExpr> {
    if order.steps.is_empty() {
        return None;
    }
    // Build the (input, local_key, characteristics) order; element 0 is the
    // start. Characteristics are EXPLAIN-only and not produced cost-model-side.
    let mut order_tuples: Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)> = order
        .steps
        .iter()
        .map(|s| {
            let key = s.key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
            (s.input, key, None)
        })
        .collect();

    let MirRelationExpr::Join {
        inputs,
        implementation,
        ..
    } = &mut join
    else {
        return None;
    };

    let (start, mut start_key, start_characteristics) = order_tuples[0].clone();

    // Mechanical lowering: wrap inputs in ArrangeBy / lift MFPs for reuse.
    let (lifted_mfp, lifted_projections) =
        crate::join_implementation::implement_arrangements(inputs, available, order_tuples.iter());

    // Compensate keys for any projections lifted by `implement_arrangements`.
    if let Some(proj) = &lifted_projections[start] {
        start_key.iter_mut().for_each(|k| k.permute(proj));
    }
    crate::join_implementation::permute_order(&mut order_tuples, &lifted_projections);

    // The start arrangement is recorded separately; drop it from the remainder.
    order_tuples.remove(0);

    *implementation = JoinImplementation::Differential(
        (start, Some(start_key), start_characteristics),
        order_tuples,
    );

    crate::join_implementation::install_lifted_mfp(&mut join, lifted_mfp);
    Some(join)
}
```

> Note: the `let ... = &mut join else` borrow ends after the last use of
> `inputs`/`implementation` (NLL), so `install_lifted_mfp(&mut join, ..)` and
> `Some(join)` compile — this is exactly how `differential::plan`
> (`join_implementation.rs:799-927`) is structured.

- [ ] **Step 6: Run the test to verify it passes**

Run: `bin/cargo-test -p mz-transform commit_differential`
Expected: PASS.

- [ ] **Step 7: Verify JI still compiles and its tests pass**

Run: `bin/cargo-test -p mz-transform join_implementation`
Expected: PASS (visibility change is behavior-neutral).

- [ ] **Step 8: Commit**

```bash
git add src/transform/src/eqsat/join_commit.rs src/transform/src/eqsat.rs src/transform/src/join_implementation.rs
git commit -m "eqsat: add cost-model-native Differential emitter (join_commit)"
```

---

