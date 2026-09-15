// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Committed catalog effects shared by adapter and replica enactment.
//!
//! The catalog enriches updates before removing old objects and after inserting
//! new ones. Absorption coalesces each consolidated batch into runtime effects.
//! This module performs no controller calls, planning, or durable writes.

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_client::logging::LogVariant;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_storage_types::sources::GenericSourceConnection;
use timely::progress::Antichain;

use crate::memory::objects::{
    CatalogItem, Cluster, ClusterReplica, Connection, Index, MaterializedView, MetricSink, Secret,
    Sink, Source, StateDiff, Table, View,
};

/// A committed catalog update with the context needed to derive runtime effects.
#[derive(Debug, Clone)]
pub struct ParsedStateUpdate {
    pub kind: ParsedStateUpdateKind,
    pub ts: Timestamp,
    pub diff: StateDiff,
}

/// A committed catalog update with the context needed to derive runtime effects.
#[derive(Debug, Clone)]
pub enum ParsedStateUpdateKind {
    Item {
        durable_item: crate::durable::objects::Item,
        parsed_item: CatalogItem,
        connection: Option<GenericSourceConnection>,
        parsed_full_name: String,
    },
    Cluster {
        durable_cluster: crate::durable::objects::Cluster,
        parsed_cluster: Cluster,
    },
    ClusterReplica {
        durable_cluster_replica: crate::durable::objects::ClusterReplica,
        parsed_cluster_replica: ClusterReplica,
    },
    IntrospectionSourceIndex {
        cluster_id: ClusterId,
        log: LogVariant,
        index_id: GlobalId,
    },
    /// A replica-scoped system-parameter override changed. The implication
    /// re-pushes the complete per-replica dyncfg layer from the catalog working
    /// copy, so it does not consume `durable`. We keep the row only so it shows
    /// up in the `tracing::trace!` of the parsed update.
    ReplicaSystemConfiguration {
        durable: crate::durable::objects::ReplicaSystemConfiguration,
    },
    /// An environment-wide system-parameter changed. The implication re-runs the
    /// `SystemVars` callbacks against the committed values, so it does not
    /// consume `durable`. We keep the row only for the `tracing::trace!`.
    SystemConfiguration {
        durable: crate::durable::objects::SystemConfiguration,
    },
    CollectionCompactionBound(crate::durable::objects::CollectionCompactionBound),
    WrittenPlan(crate::durable::objects::WrittenPlan),
    /// Storage lifetime can end after the SQL drop when the final reader releases it.
    StorageCollectionMetadata {
        id: GlobalId,
    },
}

/// Effects of one complete, consolidated catalog update batch.
///
/// Consumers enact the effects they own against the catalog after that batch.
/// The batch is not an acknowledgement of installation or read protection.
#[derive(Debug, Default)]
pub struct CatalogImplications {
    pub items: BTreeMap<CatalogItemId, CatalogImplication>,
    pub clusters: BTreeMap<ClusterId, CatalogImplication>,
    pub replicas: BTreeMap<(ClusterId, ReplicaId), CatalogImplication>,
    pub introspection_source_indexes: BTreeMap<ClusterId, BTreeMap<LogVariant, GlobalId>>,
    pub replica_scoped_config_changed: bool,
    pub system_config_changed: bool,
    pub compaction_bounds: BTreeMap<GlobalId, Antichain<Timestamp>>,
    pub retired_storage_metadata: BTreeSet<GlobalId>,
    pub written_plans: BTreeSet<GlobalId>,
}

impl CatalogImplications {
    /// Absorbs updates with at most one addition and one retraction per object.
    /// Written-plan changes are filtered to the consumer's expression build.
    pub fn from_updates(catalog_updates: Vec<ParsedStateUpdate>, build: &str) -> Self {
        let mut result = Self::default();
        for update in catalog_updates {
            tracing::trace!(?update, "got parsed state update");
            match &update.kind {
                ParsedStateUpdateKind::Item {
                    durable_item,
                    parsed_item: _,
                    connection: _,
                    parsed_full_name: _,
                } => {
                    let entry = result
                        .items
                        .entry(durable_item.id.clone())
                        .or_insert_with(|| CatalogImplication::None);
                    entry.absorb(update);
                }
                ParsedStateUpdateKind::Cluster {
                    durable_cluster,
                    parsed_cluster: _,
                } => {
                    let entry = result
                        .clusters
                        .entry(durable_cluster.id)
                        .or_insert_with(|| CatalogImplication::None);
                    entry.absorb(update.clone());
                }
                ParsedStateUpdateKind::ClusterReplica {
                    durable_cluster_replica,
                    parsed_cluster_replica: _,
                } => {
                    let entry = result
                        .replicas
                        .entry((
                            durable_cluster_replica.cluster_id,
                            durable_cluster_replica.replica_id,
                        ))
                        .or_insert_with(|| CatalogImplication::None);
                    entry.absorb(update.clone());
                }
                ParsedStateUpdateKind::IntrospectionSourceIndex {
                    cluster_id,
                    log,
                    index_id,
                } => {
                    if update.diff == StateDiff::Addition {
                        result
                            .introspection_source_indexes
                            .entry(*cluster_id)
                            .or_default()
                            .insert(log.clone(), *index_id);
                    }
                    // Retractions don't need handling: introspection
                    // source indexes are dropped with their cluster.
                }
                ParsedStateUpdateKind::ReplicaSystemConfiguration { durable: _ } => {
                    // Additions and retractions both re-derive the full
                    // per-replica layer from the working copy, so the diff sign
                    // does not matter here.
                    result.replica_scoped_config_changed = true;
                }
                ParsedStateUpdateKind::SystemConfiguration { durable: _ } => {
                    // Additions and retractions both refresh consumers from
                    // the committed values, including defaults after a reset.
                    result.system_config_changed = true;
                }
                ParsedStateUpdateKind::CollectionCompactionBound(bound) => {
                    if update.diff == StateDiff::Addition {
                        result
                            .compaction_bounds
                            .insert(bound.id, bound.frontier.into_iter().collect());
                    }
                    // Collection drops release installed bounds, not record retractions.
                }
                ParsedStateUpdateKind::StorageCollectionMetadata { id } => {
                    if update.diff == StateDiff::Retraction {
                        result.retired_storage_metadata.insert(*id);
                    }
                }
                ParsedStateUpdateKind::WrittenPlan(plan) => {
                    if plan.build_version == build {
                        result.written_plans.insert(plan.id);
                    }
                }
            }
        }

        result
    }
}

/// A state machine for building catalog implications from catalog updates.
///
/// Once all [ParsedStateUpdate] of a timestamp are ingested this is a command
/// that has to potentially be applied to in-memory state and/or the
/// controller(s).
#[derive(Debug, Clone)]
pub enum CatalogImplication {
    None,
    Table(CatalogImplicationKind<Table>),
    Source(CatalogImplicationKind<(Source, Option<GenericSourceConnection>)>),
    Sink(CatalogImplicationKind<Sink>),
    Index(CatalogImplicationKind<Index>),
    MetricSink(CatalogImplicationKind<MetricSink>),
    MaterializedView(CatalogImplicationKind<MaterializedView>),
    View(CatalogImplicationKind<View>),
    Secret(CatalogImplicationKind<Secret>),
    Connection(CatalogImplicationKind<Connection>),
    Cluster(CatalogImplicationKind<Cluster>),
    ClusterReplica(CatalogImplicationKind<ClusterReplica>),
}

#[derive(Debug, Clone)]
pub enum CatalogImplicationKind<T> {
    /// No operations seen yet.
    None,
    /// Item was added.
    Added(T),
    /// Item was dropped (with its name retained for error messages).
    Dropped(T, String),
    /// Item is being altered from one state to another.
    Altered { prev: T, new: T },
}

impl<T: Clone> CatalogImplicationKind<T> {
    /// Apply a state transition based on a diff. Returns an error message if
    /// the transition is invalid.
    fn transition(&mut self, item: T, name: Option<String>, diff: StateDiff) -> Result<(), String> {
        use CatalogImplicationKind::*;
        use StateDiff::*;

        let new_state = match (&*self, diff) {
            // Initial state transitions
            (None, Addition) => Added(item),
            (None, Retraction) => Dropped(item, name.unwrap_or_else(|| "<unknown>".to_string())),

            // From Added state
            (Added(existing), Retraction) => {
                // Add -> Drop means the item is being altered
                Altered {
                    prev: item,
                    new: existing.clone(),
                }
            }
            (Added(_), Addition) => {
                return Err("Cannot add an already added object".to_string());
            }

            // From Dropped state
            (Dropped(existing, _), Addition) => {
                // Drop -> Add means the item is being altered
                Altered {
                    prev: existing.clone(),
                    new: item,
                }
            }
            (Dropped(_, _), Retraction) => {
                return Err("Cannot drop an already dropped object".to_string());
            }

            // From Altered state
            (Altered { .. }, _) => {
                return Err(format!(
                    "Cannot apply {:?} to an object in Altered state",
                    diff
                ));
            }
        };

        *self = new_state;
        Ok(())
    }
}

/// Macro to generate absorb methods for each item type.
macro_rules! impl_absorb_method {
    (
        $method_name:ident,
        $variant:ident,
        $item_type:ty
    ) => {
        fn $method_name(
            &mut self,
            item: $item_type,
            parsed_full_name: Option<String>,
            diff: StateDiff,
        ) {
            let state = match self {
                CatalogImplication::$variant(state) => state,
                CatalogImplication::None => {
                    *self = CatalogImplication::$variant(CatalogImplicationKind::None);
                    match self {
                        CatalogImplication::$variant(state) => state,
                        _ => unreachable!(),
                    }
                }
                _ => {
                    panic!(
                        "Unexpected command type for {:?}: {} {:?}",
                        self,
                        stringify!($variant),
                        diff,
                    );
                }
            };

            if let Err(e) = state.transition(item, parsed_full_name, diff) {
                panic!(
                    "Invalid state transition for {}: {}",
                    stringify!($variant),
                    e
                );
            }
        }
    };
}

impl CatalogImplication {
    /// Absorbs the given catalog update into this [CatalogImplication], causing
    /// a state transition or error.
    fn absorb(&mut self, catalog_update: ParsedStateUpdate) {
        match catalog_update.kind {
            ParsedStateUpdateKind::Item {
                durable_item: _,
                parsed_item,
                connection,
                parsed_full_name,
            } => match parsed_item {
                CatalogItem::Table(table) => {
                    self.absorb_table(table, Some(parsed_full_name), catalog_update.diff)
                }
                CatalogItem::Source(source) => {
                    self.absorb_source(
                        (source, connection),
                        Some(parsed_full_name),
                        catalog_update.diff,
                    );
                }
                CatalogItem::Sink(sink) => {
                    self.absorb_sink(sink, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::Index(index) => {
                    self.absorb_index(index, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::MaterializedView(mv) => {
                    self.absorb_materialized_view(mv, Some(parsed_full_name), catalog_update.diff);
                }
                CatalogItem::View(view) => {
                    self.absorb_view(view, Some(parsed_full_name), catalog_update.diff);
                }

                CatalogItem::Secret(secret) => {
                    self.absorb_secret(secret, None, catalog_update.diff);
                }
                CatalogItem::Connection(connection) => {
                    self.absorb_connection(connection, None, catalog_update.diff);
                }
                CatalogItem::MetricSink(metric_sink) => {
                    self.absorb_metric_sink(
                        metric_sink,
                        Some(parsed_full_name),
                        catalog_update.diff,
                    );
                }
                CatalogItem::Log(_) => {}
                CatalogItem::Type(_) => {}
                CatalogItem::Func(_) => {}
            },
            ParsedStateUpdateKind::Cluster {
                durable_cluster: _,
                parsed_cluster,
            } => {
                let name = parsed_cluster.name.clone();
                self.absorb_cluster(parsed_cluster, Some(name), catalog_update.diff);
            }
            ParsedStateUpdateKind::ClusterReplica {
                durable_cluster_replica: _,
                parsed_cluster_replica,
            } => {
                let name = parsed_cluster_replica.name.clone();
                self.absorb_cluster_replica(
                    parsed_cluster_replica,
                    Some(name),
                    catalog_update.diff,
                );
            }
            ParsedStateUpdateKind::IntrospectionSourceIndex { .. } => {
                // IntrospectionSourceIndex updates are collected
                // separately in apply_catalog_implications and not
                // routed through absorb.
                unreachable!("IntrospectionSourceIndex should not be passed to absorb");
            }
            ParsedStateUpdateKind::ReplicaSystemConfiguration { .. } => {
                // ReplicaSystemConfiguration updates are collected separately in
                // apply_catalog_implications and not routed through absorb.
                unreachable!("ReplicaSystemConfiguration should not be passed to absorb");
            }
            ParsedStateUpdateKind::SystemConfiguration { .. } => {
                // SystemConfiguration updates are collected separately in
                // apply_catalog_implications and not routed through absorb.
                unreachable!("SystemConfiguration should not be passed to absorb");
            }
            ParsedStateUpdateKind::CollectionCompactionBound(_) => {
                unreachable!("CollectionCompactionBound should not be passed to absorb");
            }
            ParsedStateUpdateKind::WrittenPlan(_) => {
                unreachable!("WrittenPlan should not be passed to absorb");
            }
            ParsedStateUpdateKind::StorageCollectionMetadata { .. } => {
                unreachable!("StorageCollectionMetadata should not be passed to absorb");
            }
        }
    }

    impl_absorb_method!(absorb_table, Table, Table);
    impl_absorb_method!(
        absorb_source,
        Source,
        (Source, Option<GenericSourceConnection>)
    );
    impl_absorb_method!(absorb_sink, Sink, Sink);
    impl_absorb_method!(absorb_index, Index, Index);
    impl_absorb_method!(absorb_metric_sink, MetricSink, MetricSink);
    impl_absorb_method!(absorb_materialized_view, MaterializedView, MaterializedView);
    impl_absorb_method!(absorb_view, View, View);

    impl_absorb_method!(absorb_secret, Secret, Secret);
    impl_absorb_method!(absorb_connection, Connection, Connection);

    impl_absorb_method!(absorb_cluster, Cluster, Cluster);
    impl_absorb_method!(absorb_cluster_replica, ClusterReplica, ClusterReplica);
}

#[cfg(test)]
mod tests;
