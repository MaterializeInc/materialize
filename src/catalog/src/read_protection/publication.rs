// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Proposals constrained by maintained and incarnation-owned read requirements.

use crate::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use differential_dataflow::lattice::Lattice;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::CollectionFrontiers;
use std::collections::{BTreeMap, BTreeSet};
use timely::progress::Antichain;

#[derive(Debug, Default)]
pub struct PublicationCandidates {
    pub requirements: Vec<MaintainedReadRequirement>,
    pub bounds: Vec<CollectionCompactionBound>,
    // Timestamp distance from the resulting bound to finite policy permission,
    // not wall-clock age. Empty policy frontiers with finite bounds count separately.
    pub max_policy_lag_ts: u64,
    pub unbounded_policy_lag: usize,
}

/// Computes changed records from committed protection and captured collection progress.
/// Compute proposals contain only live governed catalog indexes, using actual readability
/// for indexes without a published bound.
pub fn publication_candidates(
    requirements: &imbl::OrdMap<GlobalId, MaintainedReadRequirement>,
    bounds: &imbl::OrdMap<GlobalId, Antichain<Timestamp>>,
    frontiers: &[CollectionFrontiers],
    compaction_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    compute_proposals: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    owns_durable_progress: impl Fn(GlobalId) -> bool,
    committed_input_limit: impl Fn(GlobalId, &BTreeSet<GlobalId>) -> Option<Timestamp>,
) -> PublicationCandidates {
    let mut candidates = PublicationCandidates::default();
    let mut input_limits: BTreeMap<GlobalId, Timestamp> = BTreeMap::new();
    let mut advancing = BTreeSet::new();

    for output in frontiers {
        let Some(requirement) = requirements.get(&output.id) else {
            continue;
        };
        if !owns_durable_progress(requirement.id) {
            continue;
        }
        let mut frontier: Antichain<_> = requirement.frontier.into_iter().collect();
        // An upper at a refresh timestamp does not complete that refresh.
        // Its predecessor retains the pending input snapshot, including at MIN.
        let predecessor = output
            .write_frontier
            .iter()
            .map(|t| t.saturating_sub(1))
            .collect();
        frontier.join_assign(&predecessor);
        let frontier = frontier.into_option();
        if frontier == requirement.frontier {
            continue;
        }
        advancing.insert(requirement.id);
        candidates.requirements.push(MaintainedReadRequirement {
            frontier,
            ..requirement.clone()
        });
        if let Some(frontier) = frontier {
            for input in &requirement.inputs {
                input_limits
                    .entry(*input)
                    .and_modify(|limit| *limit = (*limit).min(frontier))
                    .or_insert(frontier);
            }
        }
    }

    let storage_proposals = frontiers.iter().filter_map(|collection| {
        compaction_frontiers.get(&collection.id).map(|proposal| {
            (
                collection.id,
                proposal,
                Some(&collection.implied_capability),
            )
        })
    });
    let compute_proposals = compute_proposals
        .iter()
        .map(|(&id, proposal)| (id, proposal, None));
    for (id, proposal, policy) in storage_proposals.chain(compute_proposals) {
        let old_bound = bounds.get(&id);
        if old_bound.is_none() && policy.is_some() {
            continue;
        }
        let mut bound = proposal.clone();
        // Controller proposals exclude only catalog permission. Early creation and
        // execution holds remain authoritative alongside durable input requirements.
        bound.extend(committed_input_limit(id, &advancing));
        bound.extend(input_limits.get(&id).copied());
        if let Some(old_bound) = old_bound {
            bound.join_assign(old_bound);
        }
        if let Some(bound_ts) = bound.as_option()
            && let Some(policy) = policy
        {
            match policy.as_option() {
                Some(policy_ts) => {
                    candidates.max_policy_lag_ts = candidates
                        .max_policy_lag_ts
                        .max(u64::from(policy_ts.saturating_sub(*bound_ts)));
                }
                None => candidates.unbounded_policy_lag += 1,
            }
        }
        if Some(&bound) != old_bound {
            candidates.bounds.push(CollectionCompactionBound {
                id,
                frontier: bound.into_option(),
            });
        }
    }
    candidates
}
