// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_client::controller::ComputeInstanceId;
use mz_repr::GlobalId;
use serde::Deserialize;

use crate::coord::Coordinator;
use crate::session::Session;

/// A bundle of storage and compute collection identifiers.
#[derive(Deserialize, Debug, Default, Clone)]
pub struct CollectionIdBundle {
    /// The identifiers for sources in the storage layer.
    pub storage_ids: BTreeSet<GlobalId>,
    /// The identifiers for indexes in the compute layer.
    pub compute_ids: BTreeMap<ComputeInstanceId, BTreeSet<GlobalId>>,
}

impl CollectionIdBundle {
    /// Reports whether the bundle contains any identifiers of any type.
    pub fn is_empty(&self) -> bool {
        self.storage_ids.is_empty() && self.compute_ids.values().all(|ids| ids.is_empty())
    }

    /// Returns a new bundle without the identifiers from `other`.
    pub fn difference(&self, other: &CollectionIdBundle) -> CollectionIdBundle {
        let storage_ids = &self.storage_ids - &other.storage_ids;
        let compute_ids: BTreeMap<_, _> = self
            .compute_ids
            .iter()
            .map(|(compute_instance, compute_ids)| {
                let compute_ids =
                    if let Some(other_compute_ids) = other.compute_ids.get(compute_instance) {
                        compute_ids - other_compute_ids
                    } else {
                        compute_ids.clone()
                    };
                (*compute_instance, compute_ids)
            })
            .filter(|(_, ids)| !ids.is_empty())
            .collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids,
        }
    }

    /// Extends a `CollectionIdBundle` with the contents of another `CollectionIdBundle`.
    pub fn extend(&mut self, other: &CollectionIdBundle) {
        self.storage_ids.extend(&other.storage_ids);
        for (compute_instance, ids) in &other.compute_ids {
            self.compute_ids
                .entry(*compute_instance)
                .or_default()
                .extend(ids);
        }
    }

    /// Returns an iterator over all IDs in the bundle.
    ///
    /// The IDs are iterated in an unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.storage_ids
            .iter()
            .copied()
            .chain(self.compute_ids.values().flat_map(BTreeSet::iter).copied())
    }
}

impl Coordinator {
    /// Resolves the full name from the corresponding catalog entry for each item in `id_bundle`.
    /// If an item in the bundle does not exist in the catalog, it's not included in the result.
    pub fn resolve_collection_id_bundle_names(
        &self,
        session: &Session,
        id_bundle: &CollectionIdBundle,
    ) -> Vec<String> {
        let mut names: Vec<_> = id_bundle
            .iter()
            // This could filter out an entry that has been replaced in another transaction.
            .filter_map(|id| self.catalog().try_get_entry(&id))
            .map(|item| {
                self.catalog()
                    .resolve_full_name(item.name(), Some(session.conn_id()))
                    .to_string()
            })
            .collect();
        names.sort();
        names
    }
}
