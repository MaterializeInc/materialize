// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Passive SQL frontier reporting for replica-owned indexes.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::memory::objects::CatalogItem;
use mz_compute_client::protocol::response::FrontiersResponse;
use mz_controller_types::ReplicaId;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::{IntrospectionType, StorageWriteOp};
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::coord::Coordinator;

#[derive(Debug, Default)]
pub(crate) struct NativeFrontiers {
    global: BTreeSet<Row>,
    replicas: BTreeSet<Row>,
}

impl Coordinator {
    pub(crate) fn update_native_frontier_introspection(&mut self) {
        let Some(client) = &self.query_client else {
            return;
        };
        let observations = client
            .connections
            .ready_replicas()
            .into_iter()
            .flat_map(|((cluster, replica), client)| {
                client
                    .frontiers()
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |(id, frontiers)| (cluster, replica, id, frontiers))
            })
            .filter_map(|(cluster, replica, id, frontiers)| {
                // Storage already reports persisted collections. Query-local and
                // dropped exports must not survive in these catalog relations.
                let entry = self.catalog().try_get_entry_by_global_id(&id)?;
                match entry.item() {
                    CatalogItem::Index(index) if index.cluster_id == cluster => {
                        Some((id, replica, frontiers))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        for (kind, updates) in self.native_frontiers.update(observations) {
            if !updates.is_empty() {
                let tx = self.controller.storage.differential_introspection_tx(kind);
                let (notify, _) = tokio::sync::oneshot::channel();
                // As with controller introspection, the storage manager owns
                // write retries. A closed channel means it has shut down.
                let _ = tx.send((StorageWriteOp::Append { updates }, notify));
            }
        }
    }
}

impl NativeFrontiers {
    /// Report only observed fields. Across connected replicas the read frontier
    /// is the meet and the write frontier the join, matching query observations.
    /// An absent observation is not an empty (completed) frontier. Rebuilding the
    /// snapshot also retracts disconnected replicas and catalog-dropped indexes.
    fn update(
        &mut self,
        observations: impl IntoIterator<Item = (GlobalId, ReplicaId, FrontiersResponse)>,
    ) -> [(IntrospectionType, Vec<(Row, Diff)>); 2] {
        type Frontiers = (Option<Antichain<Timestamp>>, Option<Antichain<Timestamp>>);
        let mut global = BTreeMap::<GlobalId, Frontiers>::new();
        let mut replicas = BTreeSet::new();
        for (id, replica, observation) in observations {
            let (since, upper) = global.entry(id).or_default();
            if let Some(read) = observation.read_frontier {
                since.get_or_insert_with(Antichain::new).extend(read);
            }
            if let Some(write) = observation.write_frontier {
                replicas.insert(Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String(&replica.to_string()),
                    frontier_datum(&write),
                ]));
                if upper
                    .as_ref()
                    .is_none_or(|old| PartialOrder::less_than(old, &write))
                {
                    *upper = Some(write);
                }
            }
        }
        let global = global
            .into_iter()
            .filter_map(|(id, (since, upper))| {
                Some(Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    frontier_datum(&since?),
                    frontier_datum(&upper?),
                ]))
            })
            .collect();
        [
            (
                IntrospectionType::Frontiers,
                replace_rows(&mut self.global, global),
            ),
            (
                IntrospectionType::ReplicaFrontiers,
                replace_rows(&mut self.replicas, replicas),
            ),
        ]
    }
}

fn frontier_datum(frontier: &Antichain<Timestamp>) -> Datum<'static> {
    frontier.as_option().map_or(Datum::Null, |ts| (*ts).into())
}

fn replace_rows(old: &mut BTreeSet<Row>, new: BTreeSet<Row>) -> Vec<(Row, Diff)> {
    let updates = old
        .difference(&new)
        .cloned()
        .map(|row| (row, Diff::MINUS_ONE))
        .chain(new.difference(old).cloned().map(|row| (row, Diff::ONE)))
        .collect();
    *old = new;
    updates
}

#[cfg(test)]
mod tests;
