// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Publishes durable recovery requirements and compaction permission together.

use std::collections::BTreeMap;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use mz_catalog::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use mz_catalog::memory::objects::CatalogItem;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::storage_collections::CollectionFrontiers;
use timely::progress::Antichain;

use crate::AdapterError;
use crate::catalog::Op;
use crate::coord::Coordinator;

impl Coordinator {
    /// Publishes recovery progress and compatible bounds in enabled, writable environments.
    pub(super) async fn publish_read_protection(&mut self) -> Result<(), AdapterError> {
        if self.controller.read_only() || !self.catalog().state().catalog_read_protection_enabled()
        {
            return Ok(());
        }

        let start = Instant::now();
        let frontiers = self
            .controller
            .storage_collections
            .active_collection_frontiers();
        // Coordinator handlers serialize new creation holds with this publication.
        let compaction_frontiers = self.controller.storage_collections.compaction_frontiers();
        let candidates = publication_candidates(
            self.catalog().state().maintained_read_requirements(),
            &self.catalog().state().storage_metadata().compaction_bounds,
            &frontiers,
            &compaction_frontiers,
            |id| {
                let Some(entry) = self.catalog().try_get_entry_by_global_id(&id) else {
                    return false;
                };
                let CatalogItem::MaterializedView(mv) = entry.item() else {
                    return false;
                };
                // A pending replacement observes the target's shared upper, not its own
                // progress. Retired writers' requirements are completed by replacement.
                mv.replacement_target.is_none() && mv.global_id_writes() == id
            },
        );
        let requirement_updates = candidates.requirements.len();
        let bound_updates = candidates.bounds.len();
        if requirement_updates == 0 && bound_updates == 0 {
            return Ok(());
        }

        let ops =
            candidates
                .requirements
                .into_iter()
                .map(|requirement| Op::SetMaintainedReadRequirement {
                    id: requirement.id,
                    inputs: requirement.inputs,
                    frontier: requirement.frontier,
                })
                .chain(candidates.bounds.into_iter().map(|bound| {
                    Op::SetCollectionCompactionBound {
                        id: bound.id,
                        frontier: bound.frontier,
                    }
                }))
                .collect();
        self.catalog_transact(None, ops).await?;

        tracing::info!(
            installed_collections = frontiers.len(),
            requirement_updates,
            bound_updates,
            duration_seconds = start.elapsed().as_secs_f64(),
            max_policy_lag_ts = candidates.max_policy_lag_ts,
            unbounded_policy_lag = candidates.unbounded_policy_lag,
            "published catalog read protection"
        );
        Ok(())
    }
}

#[derive(Debug, Default)]
struct PublicationCandidates {
    requirements: Vec<MaintainedReadRequirement>,
    bounds: Vec<CollectionCompactionBound>,
    // Timestamp distance from the resulting bound to finite policy permission,
    // not wall-clock age. Empty policy frontiers with finite bounds count separately.
    max_policy_lag_ts: u64,
    unbounded_policy_lag: usize,
}

/// Computes changed records from committed protection and installed storage frontiers.
fn publication_candidates(
    requirements: &BTreeMap<GlobalId, MaintainedReadRequirement>,
    bounds: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    frontiers: &[CollectionFrontiers],
    compaction_frontiers: &BTreeMap<GlobalId, Antichain<Timestamp>>,
    is_active_mv_writer: impl Fn(GlobalId) -> bool,
) -> PublicationCandidates {
    let mut candidates = PublicationCandidates::default();
    let installed: BTreeMap<_, _> = frontiers.iter().map(|f| (f.id, f)).collect();
    let mut input_limits: BTreeMap<GlobalId, Timestamp> = BTreeMap::new();

    for requirement in requirements.values() {
        let mut frontier: Antichain<_> = requirement.frontier.into_iter().collect();
        if is_active_mv_writer(requirement.id)
            && let Some(output) = installed.get(&requirement.id)
        {
            // An upper at a refresh timestamp does not complete that refresh.
            // Its predecessor preserves the pending input snapshot. At MIN,
            // saturating subtraction leaves the committed requirement intact.
            let predecessor = output
                .write_frontier
                .iter()
                .map(|t| t.saturating_sub(1))
                .collect();
            frontier.join_assign(&predecessor);
        }
        let frontier = frontier.into_option();
        if frontier != requirement.frontier {
            candidates.requirements.push(MaintainedReadRequirement {
                frontier,
                ..requirement.clone()
            });
        }

        // Every remaining requirement limits its exact committed input versions,
        // including consumers that are uninstalled or ineligible to advance.
        if let Some(frontier) = frontier {
            for input in &requirement.inputs {
                input_limits
                    .entry(*input)
                    .and_modify(|limit| *limit = (*limit).min(frontier))
                    .or_insert(frontier);
            }
        }
    }

    for collection in frontiers {
        let Some(old_bound) = bounds.get(&collection.id) else {
            continue;
        };
        let Some(mut bound) = compaction_frontiers.get(&collection.id).cloned() else {
            continue;
        };
        // Keep early creation holds authoritative during optimization. The storage
        // proposal excludes only its permission cap, not those read requirements.
        bound.extend(input_limits.get(&collection.id).copied());
        bound.join_assign(old_bound);
        if let Some(bound_ts) = bound.as_option() {
            match collection.implied_capability.as_option() {
                Some(policy_ts) => {
                    candidates.max_policy_lag_ts = candidates
                        .max_policy_lag_ts
                        .max(u64::from(policy_ts.saturating_sub(*bound_ts)));
                }
                None => candidates.unbounded_policy_lag += 1,
            }
        }
        if &bound != old_bound {
            candidates.bounds.push(CollectionCompactionBound {
                id: collection.id,
                frontier: bound.into_option(),
            });
        }
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frontier(t: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(Timestamp::from(t))
    }

    fn policy_frontiers(
        frontiers: &[CollectionFrontiers],
    ) -> BTreeMap<GlobalId, Antichain<Timestamp>> {
        frontiers
            .iter()
            .map(|f| (f.id, f.implied_capability.clone()))
            .collect()
    }

    fn collection(id: GlobalId, upper: Option<u64>, policy: Option<u64>) -> CollectionFrontiers {
        CollectionFrontiers {
            id,
            write_frontier: upper.map(Timestamp::from).into_iter().collect(),
            implied_capability: policy.map(Timestamp::from).into_iter().collect(),
            read_capabilities: frontier(0),
        }
    }

    fn requirement(id: GlobalId, inputs: &[GlobalId], t: u64) -> MaintainedReadRequirement {
        MaintainedReadRequirement {
            id,
            inputs: inputs.iter().copied().collect(),
            frontier: Some(Timestamp::from(t)),
        }
    }

    #[mz_ore::test]
    fn durable_progress_preserves_pending_refresh_and_completion() {
        let input = GlobalId::User(1);
        let output = GlobalId::User(2);
        let requirement = requirement(output, &[input], 10);
        let requirements = BTreeMap::from([(output, requirement.clone())]);
        let bounds = BTreeMap::from([(input, frontier(10))]);

        for (upper, expected) in [
            (Some(0), Some(10)),
            (Some(10), Some(10)),
            (Some(11), Some(10)),
            (Some(12), Some(11)),
            (None, None),
        ] {
            let frontiers = [
                collection(input, None, None),
                collection(output, upper, Some(0)),
            ];
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                |_| true,
            );
            let expected = expected.map(Timestamp::from);
            if expected == requirement.frontier {
                assert!(candidates.requirements.is_empty());
                assert!(candidates.bounds.is_empty());
            } else {
                assert_eq!(
                    candidates.requirements,
                    vec![MaintainedReadRequirement {
                        frontier: expected,
                        ..requirement.clone()
                    }]
                );
                assert_eq!(
                    candidates.bounds,
                    vec![CollectionCompactionBound {
                        id: input,
                        frontier: expected,
                    }]
                );
            }
        }

        let completed = BTreeMap::from([(
            output,
            MaintainedReadRequirement {
                frontier: None,
                ..requirement
            },
        )]);
        let candidates = publication_candidates(
            &completed,
            &bounds,
            &[collection(output, Some(20), Some(0))],
            &BTreeMap::new(),
            |_| true,
        );
        assert!(candidates.requirements.is_empty(), "completion is final");
    }

    #[mz_ore::test]
    fn all_consumers_limit_exact_input_versions() {
        let input = GlobalId::User(1);
        let other_version = GlobalId::User(2);
        let writer = GlobalId::User(3);
        let pending = GlobalId::User(4);
        let uninstalled = GlobalId::User(5);
        let requirements = BTreeMap::from([
            (writer, requirement(writer, &[input], 10)),
            (pending, requirement(pending, &[input], 15)),
            (uninstalled, requirement(uninstalled, &[input], 18)),
        ]);
        let bounds = BTreeMap::from([(input, frontier(10)), (other_version, frontier(10))]);
        let frontiers = [
            collection(input, Some(100), Some(90)),
            collection(other_version, Some(100), Some(90)),
            collection(writer, Some(51), Some(0)),
            collection(pending, Some(100), Some(0)),
        ];
        for (requirements, limit) in [
            (requirements.clone(), 15),
            (
                requirements
                    .into_iter()
                    .filter(|(id, _)| *id != pending)
                    .collect(),
                18,
            ),
        ] {
            let candidates = publication_candidates(
                &requirements,
                &bounds,
                &frontiers,
                &policy_frontiers(&frontiers),
                |id| id != pending,
            );
            assert_eq!(
                candidates.requirements,
                vec![requirement(writer, &[input], 50)]
            );
            assert_eq!(
                candidates.bounds,
                vec![
                    CollectionCompactionBound {
                        id: input,
                        frontier: Some(Timestamp::from(limit)),
                    },
                    CollectionCompactionBound {
                        id: other_version,
                        frontier: Some(Timestamp::from(90)),
                    },
                ]
            );
            assert_eq!(candidates.max_policy_lag_ts, 90 - limit);
        }
    }

    #[mz_ore::test]
    fn local_creation_holds_limit_permission() {
        let input = GlobalId::User(1);
        let frontiers = [collection(input, Some(100), Some(90))];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &BTreeMap::from([(input, frontier(10))]),
            &frontiers,
            &BTreeMap::from([(input, frontier(20))]),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: input,
                frontier: Some(Timestamp::from(20)),
            }]
        );
    }

    #[mz_ore::test]
    fn bounds_require_installation_and_governance_and_never_regress() {
        let advancing = GlobalId::User(1);
        let regressing_policy = GlobalId::User(2);
        let uninstalled = GlobalId::User(3);
        let ungoverned = GlobalId::User(4);
        let complete = GlobalId::User(5);
        let bounds = BTreeMap::from([
            (advancing, frontier(10)),
            (regressing_policy, frontier(10)),
            (uninstalled, frontier(10)),
            (complete, Antichain::new()),
        ]);
        let frontiers = [
            collection(advancing, Some(100), Some(90)),
            collection(regressing_policy, Some(100), Some(5)),
            collection(ungoverned, Some(100), Some(90)),
            collection(complete, None, Some(90)),
        ];
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            |_| false,
        );
        assert_eq!(
            candidates.bounds,
            vec![CollectionCompactionBound {
                id: advancing,
                frontier: Some(Timestamp::from(90)),
            }]
        );
        assert_eq!(candidates.max_policy_lag_ts, 0);

        let mut bounds = bounds;
        bounds.insert(advancing, frontier(90));
        let candidates = publication_candidates(
            &BTreeMap::new(),
            &bounds,
            &frontiers,
            &policy_frontiers(&frontiers),
            |_| false,
        );
        assert!(candidates.requirements.is_empty());
        assert!(
            candidates.bounds.is_empty(),
            "unchanged publication is a no-op"
        );
    }
}
