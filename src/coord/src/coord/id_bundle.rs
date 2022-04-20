// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_repr::GlobalId;

/// A bundle of storage and compute collection identifiers.
#[derive(Debug, Default, Clone)]
pub struct CollectionIdBundle {
    /// The identifiers for sources in the storage layer.
    pub storage_ids: BTreeSet<GlobalId>,
    /// The identifiers for indexes in the compute layer.
    pub compute_ids: BTreeSet<GlobalId>,
}

impl CollectionIdBundle {
    /// Reports whether the bundle contains any identifiers of any type.
    pub fn is_empty(&self) -> bool {
        self.storage_ids.is_empty() && self.compute_ids.is_empty()
    }

    /// Returns a new bundle without the identifiers from `other`.
    pub fn difference(&self, other: &CollectionIdBundle) -> CollectionIdBundle {
        CollectionIdBundle {
            storage_ids: &self.storage_ids - &other.storage_ids,
            compute_ids: &self.compute_ids - &other.compute_ids,
        }
    }

    /// Returns an iterator over all IDs in the bundle.
    ///
    /// The IDs are iterated in an unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.storage_ids
            .iter()
            .copied()
            .chain(self.compute_ids.iter().copied())
    }
}
