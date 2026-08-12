// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integer-encoded kernels for the RBAC decision procedure.
//!
//! Bounded model checking cannot reach our state as it is represented in the catalog. A
//! nondeterministic role graph is a `BTreeMap<RoleId, BTreeSet<RoleId>>`, and inserting a single
//! nondeterministic element into a `BTreeSet` is expensive enough to be a performance test in
//! Kani's own suite. The kernels here take an integer encoding of the same state instead, where a
//! closure is a fixpoint over machine words, so a verifier can reach a bound worth having.
//!
//! The division of labour that makes this useful: a verifier checks the kernel, and `proptest`
//! checks that the encoding agrees with the collection-shaped implementation the system actually
//! runs. Neither half is meaningful without the other.
//!
//! NOTE: [`MAX_ROLES`] caps the encoding at one word. That is a bound on what can be *verified*,
//! not on what Materialize supports. A deployment may hold more roles than fit, so
//! [`encode_membership`] reports when it cannot represent a graph rather than truncating it. See
//! `doc/developer/design/20260812_rbac_formal_methods.md` for how this is meant to grow.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use mz_repr::role_id::RoleId;

/// The largest number of roles the single-word encoding can represent.
pub const MAX_ROLES: usize = 64;

/// A role membership graph as adjacency bitmasks.
///
/// `edges[i]` has bit `j` set when role `i` is a direct member of role `j`, using the index
/// assignment in `roles`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleGraph {
    /// Index to role, so a closure bitmask can be read back out.
    pub roles: Vec<RoleId>,
    /// Adjacency bitmask per role, parallel to `roles`.
    pub edges: Vec<u64>,
}

impl RoleGraph {
    /// Returns the index assigned to `role`, if it is in the graph.
    pub fn index_of(&self, role: &RoleId) -> Option<usize> {
        self.roles.iter().position(|candidate| candidate == role)
    }

    /// Expands a closure bitmask back into the roles it denotes.
    pub fn decode(&self, mut mask: u64) -> BTreeSet<RoleId> {
        let mut roles = BTreeSet::new();
        while mask != 0 {
            let index = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            // A mask produced by `membership_closure` only has bits for roles in the graph, but
            // callers can pass an arbitrary mask, so ignore bits that name nothing.
            if let Some(role) = self.roles.get(index) {
                roles.insert(*role);
            }
        }
        roles
    }
}

/// Encodes a direct-membership map as a [`RoleGraph`].
///
/// Returns `None` when the graph needs more than [`MAX_ROLES`] roles, which includes roles that
/// appear only as the target of a membership edge. Callers must treat `None` as "cannot analyse
/// this graph", never as an empty graph.
pub fn encode_membership(membership: &BTreeMap<RoleId, BTreeSet<RoleId>>) -> Option<RoleGraph> {
    // Sorted, because it comes out of a `BTreeSet`. `index_of` binary searches it.
    let roles: Vec<RoleId> = membership
        .iter()
        .flat_map(|(role, parents)| std::iter::once(role).chain(parents))
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if roles.len() > MAX_ROLES {
        return None;
    }

    let index_of = |role: &RoleId| {
        roles
            .binary_search(role)
            .expect("every role reachable from `membership` was collected above")
    };
    let mut edges = vec![0u64; roles.len()];
    for (role, parents) in membership {
        let mut mask = 0u64;
        for parent in parents {
            mask |= 1u64 << index_of(parent);
        }
        edges[index_of(role)] = mask;
    }

    Some(RoleGraph { roles, edges })
}

/// Computes the reflexive-transitive closure of `role` over `edges`.
///
/// Returns a bitmask of every role reachable from `role`, including `role` itself. Bits naming a
/// role outside `edges` are ignored, so the result never depends on how a caller padded its
/// encoding.
///
/// Total on all inputs. In particular it terminates on cyclic graphs: `reached` grows on every
/// iteration that continues, and it is bounded by the number of roles, so the loop runs at most
/// `edges.len()` times. The catalog rejects cyclic grants in `Op::GrantRole`, but a closure
/// algorithm should not depend on an invariant enforced somewhere else.
///
/// This is deliberately a fixpoint over machine words rather than a worklist over sets. See the
/// module documentation for why.
pub fn membership_closure(edges: &[u64], role: usize) -> u64 {
    // A caller can hand over more edge words than the encoding addresses. Ignoring the tail keeps
    // the shift below in range, which is what makes this total rather than a panic.
    let len = analysable_len(edges);
    if role >= len {
        return 0;
    }
    let valid = valid_mask(len);

    let mut reached = 1u64 << role;
    loop {
        let mut next = reached;
        let mut remaining = reached;
        while remaining != 0 {
            let index = remaining.trailing_zeros() as usize;
            remaining &= remaining - 1;
            next |= edges[index] & valid;
        }
        if next == reached {
            return reached;
        }
        reached = next;
    }
}

/// How many of `edges`' words the single-word encoding can address.
fn analysable_len(edges: &[u64]) -> usize {
    std::cmp::min(edges.len(), MAX_ROLES)
}

/// A mask with the low `len` bits set, saturating at all 64.
///
/// `len` must already be capped to [`MAX_ROLES`], which [`analysable_len`] does.
fn valid_mask(len: usize) -> u64 {
    if len >= MAX_ROLES {
        u64::MAX
    } else {
        (1u64 << len) - 1
    }
}

/// The closure computed directly over sets, mirroring `CatalogState::collect_role_membership`.
///
/// Exists so the encoded kernel has something to be checked against. Kept structurally close to
/// the catalog's version on purpose, including the worklist shape, so that a divergence between
/// the two is a divergence in the algorithm rather than in the transcription.
pub fn membership_closure_reference(
    membership: &BTreeMap<RoleId, BTreeSet<RoleId>>,
    role: &RoleId,
) -> BTreeSet<RoleId> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from(vec![*role]);
    // Not a `while let`: Kani's loop contracts, which are what would give an unbounded result
    // about this loop, do not support `while let`.
    while !queue.is_empty() {
        let current = queue.pop_front().expect("queue is not empty");
        if !reached.contains(&current) {
            reached.insert(current);
            if let Some(parents) = membership.get(&current) {
                queue.extend(parents.iter().copied());
            }
        }
    }
    reached
}

#[cfg(test)]
mod tests {
    use mz_ore::cast::CastFrom;
    use proptest::prelude::*;

    use super::*;

    fn any_role_id() -> impl Strategy<Value = RoleId> {
        prop_oneof![
            (0..6u64).prop_map(RoleId::User),
            (0..2u64).prop_map(RoleId::System),
            Just(RoleId::Public),
        ]
    }

    /// Unconstrained membership maps, including cyclic and self-referential ones.
    fn any_membership() -> impl Strategy<Value = BTreeMap<RoleId, BTreeSet<RoleId>>> {
        proptest::collection::btree_map(
            any_role_id(),
            proptest::collection::btree_set(any_role_id(), 0..4),
            0..8,
        )
    }

    proptest! {
        /// P4: the encoded kernel computes the same closure as the set-based reference, for any
        /// graph, including cyclic ones.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn proptest_closure_matches_reference(membership in any_membership()) {
            let graph = encode_membership(&membership).expect("bounded by the strategy");
            for role in &graph.roles {
                let index = graph.index_of(role).expect("role came from the graph");
                let encoded = graph.decode(membership_closure(&graph.edges, index));
                let reference = membership_closure_reference(&membership, role);
                prop_assert_eq!(
                    &encoded,
                    &reference,
                    "closure of {:?} disagrees",
                    role,
                );
            }
        }

        /// The closure is reflexive and idempotent, and every role it reaches has a closure
        /// contained in it. Together these are what make it a closure rather than a walk of
        /// bounded depth, which is the property a cycle would break.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn proptest_closure_is_transitively_closed(membership in any_membership()) {
            let graph = encode_membership(&membership).expect("bounded by the strategy");
            for index in 0..graph.roles.len() {
                let closure = membership_closure(&graph.edges, index);
                prop_assert!(closure & (1 << index) != 0, "closure is not reflexive");

                let mut remaining = closure;
                while remaining != 0 {
                    let reached = remaining.trailing_zeros() as usize;
                    remaining &= remaining - 1;
                    let inner = membership_closure(&graph.edges, reached);
                    prop_assert_eq!(
                        inner & !closure,
                        0,
                        "closure of reached role {} escapes the closure of {}",
                        reached,
                        index,
                    );
                }
            }
        }

        /// Totality: no input panics, and no bit outside the addressable graph is ever returned.
        /// Arbitrary edge words stand in for a malformed or padded encoding. The role index and
        /// the edge count both range past [`MAX_ROLES`], because that is where the shift in
        /// `membership_closure` would overflow if it were not capped.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn proptest_closure_is_total(
            edges in proptest::collection::vec(any::<u64>(), 0..70),
            role in 0..80usize,
        ) {
            let closure = membership_closure(&edges, role);
            let len = analysable_len(&edges);
            prop_assert_eq!(closure & !valid_mask(len), 0);
            if role >= len {
                prop_assert_eq!(closure, 0);
            }
        }
    }

    #[mz_ore::test]
    fn test_closure_terminates_on_cycles() {
        let a = RoleId::User(1);
        let b = RoleId::User(2);
        let membership = BTreeMap::from([(a, BTreeSet::from([b])), (b, BTreeSet::from([a]))]);
        let graph = encode_membership(&membership).expect("two roles");
        let index = graph.index_of(&a).expect("a is in the graph");
        assert_eq!(
            graph.decode(membership_closure(&graph.edges, index)),
            BTreeSet::from([a, b])
        );
    }

    #[mz_ore::test]
    fn test_encoding_refuses_oversized_graphs() {
        let membership: BTreeMap<_, _> = (0..(u64::cast_from(MAX_ROLES) + 1))
            .map(|id| (RoleId::User(id), BTreeSet::new()))
            .collect();
        assert_eq!(encode_membership(&membership), None);
    }
}
