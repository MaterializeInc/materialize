// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_compute_client::controller::{ComputeController, ComputeInstanceId};
use mz_expr::MirScalarExpr;
use mz_repr::GlobalId;
use mz_stash::Append;
use mz_transform::IndexOracle;

use crate::catalog::{CatalogItem, CatalogState, Index};
use crate::coord::dataflow_builder::DataflowBuilder;
use crate::coord::{CollectionIdBundle, CoordTimestamp, Coordinator};

/// Answers questions about the indexes available on a particular compute
/// instance.
#[derive(Debug)]
pub struct ComputeInstanceIndexOracle<'a, T> {
    catalog: &'a CatalogState,
    compute: ComputeController<'a, T>,
}

impl<S: Append> Coordinator<S> {
    /// Creates a new index oracle for the specified compute instance.
    pub fn index_oracle(
        &self,
        instance: ComputeInstanceId,
    ) -> ComputeInstanceIndexOracle<mz_repr::Timestamp> {
        ComputeInstanceIndexOracle {
            catalog: self.catalog.state(),
            compute: self.controller.compute(instance).unwrap(),
        }
    }
}

impl<T: Copy> DataflowBuilder<'_, T> {
    /// Creates a new index oracle for the same compute instance as the dataflow
    /// builder.
    pub fn index_oracle(&self) -> ComputeInstanceIndexOracle<T> {
        ComputeInstanceIndexOracle {
            catalog: self.catalog,
            compute: self.compute,
        }
    }
}

impl<T: CoordTimestamp> ComputeInstanceIndexOracle<'_, T> {
    /// Identifies a bundle of storage and compute collection ids sufficient for
    /// building a dataflow for the identifiers in `ids` out of the indexes
    /// available in this compute instance.
    pub fn sufficient_collections<'a, I>(&self, ids: I) -> CollectionIdBundle
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        let mut id_bundle = CollectionIdBundle::default();
        let mut todo: BTreeSet<GlobalId> = ids.into_iter().cloned().collect();

        // Iteratively extract the largest element, potentially introducing lesser elements.
        while let Some(id) = todo.iter().rev().next().cloned() {
            // Extract available indexes as those that are enabled, and installed on the cluster.
            let mut available_indexes = self.indexes_on(id).map(|(id, _)| id).peekable();

            if available_indexes.peek().is_some() {
                id_bundle
                    .compute_ids
                    .entry(self.compute.instance_id())
                    .or_default()
                    .extend(available_indexes);
            } else {
                match self.catalog.get_entry(&id).item() {
                    // Unmaterialized view. Search its dependencies.
                    view @ CatalogItem::View(_) => {
                        todo.extend(view.uses());
                    }
                    CatalogItem::Source(_)
                    | CatalogItem::Table(_)
                    | CatalogItem::RecordedView(_) => {
                        // Unmaterialized source, table, or recorded view.
                        // Record that we are missing at least one index.
                        id_bundle.storage_ids.insert(id);
                    }
                    _ => {
                        // Non-indexable thing; no work to do.
                    }
                }
            }
            todo.remove(&id);
        }

        id_bundle
    }

    pub fn indexes_on(&self, id: GlobalId) -> impl Iterator<Item = (GlobalId, &Index)> {
        self.catalog
            .get_indexes_on(id, self.compute.instance_id())
            .filter(|(idx_id, _idx)| self.compute.collection(*idx_id).is_ok())
    }
}

impl<T: CoordTimestamp> IndexOracle for ComputeInstanceIndexOracle<'_, T> {
    fn indexes_on(&self, id: GlobalId) -> Box<dyn Iterator<Item = &[MirScalarExpr]> + '_> {
        Box::new(
            ComputeInstanceIndexOracle::indexes_on(self, id)
                .map(|(_idx_id, idx)| idx.keys.as_slice()),
        )
    }
}
