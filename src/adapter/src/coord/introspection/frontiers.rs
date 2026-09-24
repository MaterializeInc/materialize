// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Passive SQL introspection for replica-owned compute exports.

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use mz_catalog::memory::objects::CatalogItem;
use mz_cluster_client::WallclockLagFn;
use mz_compute_client::protocol::response::FrontiersResponse;
use mz_controller_types::ReplicaId;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::{IntrospectionType, StorageWriteOp, WallclockLag};
use timely::PartialOrder;
use timely::progress::Antichain;

use crate::coord::Coordinator;

mod lag;

type Observation = (GlobalId, ReplicaId, bool, FrontiersResponse);

#[derive(Debug, Default)]
pub(crate) struct NativeFrontiers {
    global: BTreeSet<Row>,
    replicas: BTreeSet<Row>,
    dependencies: BTreeSet<Row>,
    wallclock_lag: lag::NativeWallclockLag,
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
                // Query-local and dropped exports must not survive in these
                // catalog relations.
                let entry = self.catalog().try_get_entry_by_global_id(&id)?;
                // Retained MV versions are storage aliases, not live compute
                // writers. Their cached observations must retire on replacement.
                let (owner, storage_sink) = match entry.item() {
                    CatalogItem::Index(index) => (index.cluster_id, false),
                    CatalogItem::MaterializedView(mv)
                        if mv.global_id_writes() == id
                            && mv.target_replica.is_none_or(|target| target == replica) =>
                    {
                        (mv.cluster_id, true)
                    }
                    // Metric sinks do not sink into a Persist collection.
                    CatalogItem::MetricSink(sink) => (sink.cluster_id, false),
                    _ => return None,
                };
                (owner == cluster).then_some((id, replica, storage_sink, frontiers))
            })
            .collect::<Vec<_>>();

        // Dependencies describe the selected physical plan, as in controller
        // introspection. They do not certify replica installation or readability.
        let dependencies = self
            .catalog()
            .entries()
            .filter_map(|entry| {
                let plan = entry.item().physical_plan()?;
                Some((entry.latest_global_id(), plan))
            })
            .flat_map(|(id, plan)| {
                plan.source_imports
                    .keys()
                    .chain(plan.index_imports.keys())
                    .map(move |input| {
                        Row::pack_slice(&[
                            Datum::String(&id.to_string()),
                            Datum::String(&input.to_string()),
                        ])
                    })
            })
            .collect();
        let mut updates = self.native_lag_updates(&observations);
        updates.extend(self.native_frontiers.update(observations));
        updates.push((
            IntrospectionType::ComputeDependencies,
            replace_rows(&mut self.native_frontiers.dependencies, dependencies),
        ));
        for (kind, updates) in updates {
            if !updates.is_empty() {
                if matches!(
                    kind,
                    IntrospectionType::WallclockLagHistory
                        | IntrospectionType::WallclockLagHistogram
                ) {
                    self.controller
                        .storage
                        .append_introspection_updates(kind, updates);
                    continue;
                }
                let tx = self.controller.storage.differential_introspection_tx(kind);
                let (notify, _) = tokio::sync::oneshot::channel();
                // As with controller introspection, the storage manager owns
                // write retries. A closed channel means it has shut down.
                let _ = tx.send((StorageWriteOp::Append { updates }, notify));
            }
        }
    }

    fn native_lag_updates(
        &mut self,
        observations: &[Observation],
    ) -> Vec<(IntrospectionType, Vec<(Row, Diff)>)> {
        let lag = WallclockLagFn::<Timestamp>::new(self.catalog().config().now.clone());
        let frontier_lag = |frontier: &Antichain<Timestamp>| {
            WallclockLag::Seconds(frontier.as_option().map_or(0, |ts| lag(*ts).as_secs()))
        };
        let storage_frontiers: BTreeMap<_, _> = observations
            .iter()
            .filter_map(|(id, _, storage_sink, _)| storage_sink.then_some(*id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|id| {
                self.controller
                    .storage_collections
                    .collection_frontiers(id)
                    .ok()
                    .map(|f| (id, f))
            })
            .collect();
        let storage_readable = |id| {
            storage_frontiers.get(&id).is_some_and(|frontiers| {
                PartialOrder::less_than(&frontiers.read_capabilities, &frontiers.write_frontier)
            })
        };
        let mut replicas = Vec::new();
        let mut hydrated = BTreeSet::new();
        let mut global = BTreeMap::<
            GlobalId,
            (
                bool,
                Option<Antichain<Timestamp>>,
                Option<Antichain<Timestamp>>,
            ),
        >::new();
        for (id, replica, storage_sink, observation) in observations {
            if observation.hydrated == Some(true) {
                hydrated.insert(*id);
            }
            let readable =
                (*storage_sink && storage_readable(*id)) || observation.hydrated == Some(true);
            if let Some(write) = &observation.write_frontier {
                let value = if readable {
                    frontier_lag(write)
                } else {
                    WallclockLag::Undefined
                };
                replicas.push(((*id, *replica), value));
            }
            let (_, since, upper) = global.entry(*id).or_insert((*storage_sink, None, None));
            if let Some(read) = &observation.read_frontier {
                since
                    .get_or_insert_with(Antichain::new)
                    .extend(read.iter().copied());
            }
            if let Some(write) = &observation.write_frontier {
                upper
                    .get_or_insert_with(|| write.clone())
                    .join_assign(write);
            }
        }
        let mut collections = global
            .into_iter()
            .filter_map(|(id, (storage_sink, since, upper))| {
                let mut upper = upper?;
                if let Some(storage) = storage_frontiers.get(&id) {
                    upper.join_assign(&storage.write_frontier);
                }
                let entry = self.catalog().get_entry_by_global_id(&id);
                let readable = if storage_sink {
                    storage_readable(id)
                } else if matches!(entry.item(), CatalogItem::MetricSink(_)) {
                    // Metric sinks have no readable arrangement. Actual output
                    // hydration establishes progress beyond their installation
                    // as_of, while a completed output has no readable times.
                    hydrated.contains(&id) && !upper.is_empty()
                } else {
                    since.is_some_and(|since| PartialOrder::less_than(&since, &upper))
                };
                let value = if readable {
                    frontier_lag(&upper)
                } else {
                    WallclockLag::Undefined
                };
                let cluster = self.catalog().get_cluster(entry.item().cluster_id()?);
                let labels = cluster
                    .config
                    .workload_class
                    .as_ref()
                    .map(|value| ("workload_class", value.clone()))
                    .into_iter()
                    .collect();
                Some((id, value, labels))
            })
            .collect::<Vec<_>>();
        let now = (self.catalog().config().now)();
        let dyncfg = self.catalog().system_config().dyncfgs().clone();
        let read_only = self.controller.read_only();
        let observed_replicas: BTreeSet<_> = replicas.iter().map(|(key, _)| *key).collect();
        let observed_collections: BTreeSet<_> = collections.iter().map(|(id, _, _)| *id).collect();
        let mut retained_collections = BTreeSet::new();
        for entry in self.catalog().entries() {
            let (cluster_id, target) = match entry.item() {
                CatalogItem::Index(index) => (index.cluster_id, None),
                CatalogItem::MaterializedView(mv) => (mv.cluster_id, mv.target_replica),
                CatalogItem::MetricSink(sink) => (sink.cluster_id, None),
                _ => continue,
            };
            let id = entry.latest_global_id();
            retained_collections.insert(id);
            let cluster = self.catalog().get_cluster(cluster_id);
            // A pending export has undefined lag, not an absent lag row. This
            // does not synthesize frontiers, hydration, or execution readiness.
            for replica in cluster.replicas() {
                if target.is_none_or(|target| target == replica.replica_id)
                    && !observed_replicas.contains(&(id, replica.replica_id))
                {
                    replicas.push(((id, replica.replica_id), WallclockLag::Undefined));
                }
            }
            if !observed_collections.contains(&id) {
                let labels = cluster
                    .config
                    .workload_class
                    .as_ref()
                    .map(|value| ("workload_class", value.clone()))
                    .into_iter()
                    .collect();
                collections.push((id, WallclockLag::Undefined, labels));
            }
        }
        self.native_frontiers.wallclock_lag.update(
            now,
            &dyncfg,
            read_only,
            replicas,
            collections,
            &retained_collections,
        )
    }
}

impl NativeFrontiers {
    /// Report only observed fields. Across connected replicas the read frontier
    /// is the meet and the write frontier the join, matching query observations.
    /// An absent observation is not an empty (completed) frontier. Rebuilding the
    /// snapshot also retracts disconnected replicas and catalog-dropped exports.
    /// Storage sinks report replica uppers here, but storage owns their global
    /// frontiers so compute must not emit duplicate global rows.
    fn update(
        &mut self,
        observations: impl IntoIterator<Item = Observation>,
    ) -> [(IntrospectionType, Vec<(Row, Diff)>); 2] {
        type Frontiers = (Option<Antichain<Timestamp>>, Option<Antichain<Timestamp>>);
        let mut global = BTreeMap::<GlobalId, Frontiers>::new();
        let mut replicas = BTreeSet::new();
        for (id, replica, storage_sink, observation) in observations {
            if let Some(write) = &observation.write_frontier {
                replicas.insert(Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String(&replica.to_string()),
                    frontier_datum(write),
                ]));
            }
            if storage_sink {
                continue;
            }
            let (since, upper) = global.entry(id).or_default();
            if let Some(read) = observation.read_frontier {
                since.get_or_insert_with(Antichain::new).extend(read);
            }
            if let Some(write) = observation.write_frontier {
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
