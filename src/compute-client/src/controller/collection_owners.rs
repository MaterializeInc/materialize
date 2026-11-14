// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! First-instance lookup for persistent compute collections.

use mz_compute_types::ComputeInstanceId;
use mz_ore::collections::HashMap;
use mz_repr::GlobalId;

pub(super) struct CollectionOwners {
    // Shared system IDs can exist in multiple instances. Keep the same owner
    // that an ordered traversal of the controller's instances would find.
    first: HashMap<GlobalId, ComputeInstanceId>,
}

impl Default for CollectionOwners {
    fn default() -> Self {
        Self {
            first: HashMap::new(),
        }
    }
}

impl CollectionOwners {
    pub fn get(&self, collection: GlobalId) -> Option<ComputeInstanceId> {
        self.first.get(&collection).copied()
    }

    pub fn insert(&mut self, collection: GlobalId, instance: ComputeInstanceId) {
        if collection.is_system() || collection.is_user() {
            self.first
                .entry(collection)
                .and_modify(|owner| *owner = (*owner).min(instance))
                .or_insert(instance);
        }
    }

    // The caller has already removed the membership from its instance map.
    // Find a replacement only when the first owner was removed.
    pub fn remove(
        &mut self,
        collection: GlobalId,
        instance: ComputeInstanceId,
        remaining_owner: impl FnOnce() -> Option<ComputeInstanceId>,
    ) {
        if self.first.get(&collection) != Some(&instance) {
            return;
        }
        if let Some(owner) = remaining_owner() {
            self.first.insert(collection, owner);
        } else {
            self.first.remove(&collection);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    #[mz_ore::test]
    fn first_owner_tracks_membership_changes() {
        let mut owners = CollectionOwners::default();
        let mut instances: BTreeMap<ComputeInstanceId, BTreeSet<GlobalId>> = BTreeMap::new();
        let ids = [
            GlobalId::System(1),
            GlobalId::IntrospectionSourceIndex(1),
            GlobalId::User(1),
        ];
        // Include lower-owner insertion, a duplicate insertion, non-owner
        // removal, primary-owner removal and an overlapping move.
        for (insert, instance) in [
            (true, 3),
            (true, 1),
            (true, 2),
            (true, 1),
            (false, 2),
            (false, 1),
            (true, 2),
            (false, 3),
            (false, 2),
            (true, 1),
            (false, 1),
        ] {
            let instance = ComputeInstanceId::User(instance);
            for id in ids {
                if insert {
                    instances.entry(instance).or_default().insert(id);
                    owners.insert(id, instance);
                } else {
                    instances.get_mut(&instance).unwrap().remove(&id);
                    owners.remove(id, instance, || {
                        instances
                            .iter()
                            .find_map(|(instance, ids)| ids.contains(&id).then_some(*instance))
                    });
                }
                for id in ids {
                    let expected = instances
                        .iter()
                        .find_map(|(instance, ids)| ids.contains(&id).then_some(*instance));
                    assert_eq!(owners.get(id), expected);
                }
            }
        }
    }

    #[mz_ore::test]
    fn removing_instance_reassigns_only_its_collections() {
        let mut owners = CollectionOwners::default();
        let system = ComputeInstanceId::System(1);
        let user = ComputeInstanceId::User(1);
        let shared = GlobalId::System(1);
        let private = GlobalId::User(1);
        let other = GlobalId::User(2);
        let mut instances = BTreeMap::from([
            (system, BTreeSet::from([shared, private])),
            (user, BTreeSet::from([shared, other])),
        ]);
        for (instance, ids) in &instances {
            for id in ids {
                owners.insert(*id, *instance);
            }
        }
        for id in instances.remove(&system).unwrap() {
            owners.remove(id, system, || {
                instances
                    .iter()
                    .find_map(|(instance, ids)| ids.contains(&id).then_some(*instance))
            });
        }
        assert_eq!(owners.get(shared), Some(user));
        assert_eq!(owners.get(private), None);
        assert_eq!(owners.get(other), Some(user));
    }

    #[mz_ore::test]
    fn ignored_ids_and_non_owners_do_not_search_for_replacements() {
        let mut owners = CollectionOwners::default();
        let first = ComputeInstanceId::System(1);
        let second = ComputeInstanceId::User(1);
        for id in [GlobalId::Transient(1), GlobalId::Explain] {
            owners.insert(id, first);
            assert_eq!(owners.get(id), None);
            owners.remove(id, first, || panic!("untracked collection must not search"));
        }
        let id = GlobalId::System(1);
        owners.insert(id, second);
        owners.insert(id, first);
        assert_eq!(owners.get(id), Some(first));
        owners.remove(id, second, || panic!("non-owner removal must not search"));
        assert_eq!(owners.get(id), Some(first));
        owners.remove(id, first, || Some(second));
        assert_eq!(owners.get(id), Some(second));
        owners.remove(id, second, || None);
        assert_eq!(owners.get(id), None);
    }
}
