// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities for parsing and augmenting "raw" catalog changes
//! ([StateUpdateKind]), so that we can apply derived commands to the
//! controller(s).
//!
//! See [parse_state_update] for details.

use mz_catalog::memory::objects::{DataSourceDesc, StateDiff, StateUpdateKind};
use mz_catalog::{durable, memory};
use mz_ore::instrument;
use mz_repr::Timestamp;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::sources::GenericSourceConnection;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::CatalogState;

/// An update that needs to be applied to a controller.
#[derive(Debug, Clone)]
pub struct ParsedStateUpdate {
    pub kind: ParsedStateUpdateKind,
    pub ts: Timestamp,
    pub diff: StateDiff,
}

/// An update that needs to be applied to a controller.
#[derive(Debug, Clone)]
pub enum ParsedStateUpdateKind {
    Item {
        durable_item: durable::objects::Item,
        parsed_item: memory::objects::CatalogItem,
        connection: Option<GenericSourceConnection>,
        parsed_full_name: String,
    },
    TemporaryItem {
        durable_item: memory::objects::TemporaryItem,
        parsed_item: memory::objects::CatalogItem,
        connection: Option<GenericSourceConnection>,
        parsed_full_name: String,
    },
    Cluster {
        durable_cluster: durable::objects::Cluster,
        parsed_cluster: memory::objects::Cluster,
    },
    ClusterReplica {
        durable_cluster_replica: durable::objects::ClusterReplica,
        parsed_cluster_replica: memory::objects::ClusterReplica,
    },
}

/// Generate a [ParsedStateUpdates](ParsedStateUpdate) that
/// corresponds to the given change to the catalog.
///
/// This technically doesn't "parse" the given state update but uses the given
/// in-memory [CatalogState] as a shortcut. It already contains the parsed
/// representation of the item. In theory, we could re-construct the parsed
/// items by hand if we're given all the changes that lead to a given catalog
/// state.
///
/// For changes with a positive diff, the given [CatalogState] must reflect the
/// catalog state _after_ applying the catalog change to the catalog. For
/// negative changes, the given [CatalogState] must reflect the catalog state
/// _before_ applying the changes. This is so that we can easily extract the
/// state of an object before it is removed.
///
/// Will return `None` if the given catalog change is not relevant to the
/// controller(s).
#[instrument(level = "debug")]
pub fn parse_state_update(
    catalog: &CatalogState,
    kind: StateUpdateKind,
    ts: Timestamp,
    diff: StateDiff,
) -> Option<ParsedStateUpdate> {
    // WIP: Exhaustive match?
    let kind = match kind {
        StateUpdateKind::Item(item) => Some(parse_item_update(catalog, item, diff)),
        StateUpdateKind::TemporaryItem(item) => {
            Some(parse_temporary_item_update(catalog, item, diff))
        }
        StateUpdateKind::Cluster(cluster) => Some(parse_cluster_update(catalog, cluster, diff)),
        StateUpdateKind::ClusterReplica(replica) => {
            Some(parse_cluster_replica_update(catalog, replica, diff))
        }
        _ => None,
    };

    kind.map(|kind| ParsedStateUpdate { kind, ts, diff })
}

fn parse_item_update(
    catalog: &CatalogState,
    durable_item: durable::objects::Item,
    _diff: StateDiff,
) -> ParsedStateUpdateKind {
    let entry = catalog.get_entry(&durable_item.id);

    let parsed_item = entry.item().clone();
    let parsed_full_name = catalog
        .resolve_full_name(entry.name(), entry.conn_id())
        .to_string();

    let connection = match &parsed_item {
        memory::objects::CatalogItem::Source(source) => {
            if let DataSourceDesc::Ingestion { desc, .. }
            | DataSourceDesc::OldSyntaxIngestion { desc, .. } = &source.data_source
            {
                Some(desc.connection.clone().into_inline_connection(catalog))
            } else {
                None
            }
        }
        _ => None,
    };

    ParsedStateUpdateKind::Item {
        durable_item,
        parsed_item,
        connection,
        parsed_full_name,
    }
}

fn parse_temporary_item_update(
    catalog: &CatalogState,
    durable_item: memory::objects::TemporaryItem,
    _diff: StateDiff,
) -> ParsedStateUpdateKind {
    let entry = catalog.get_entry(&durable_item.id);

    let parsed_item = entry.item().clone();
    let parsed_full_name = catalog
        .resolve_full_name(entry.name(), entry.conn_id())
        .to_string();

    let connection = match &parsed_item {
        memory::objects::CatalogItem::Source(source) => {
            if let DataSourceDesc::Ingestion { desc, .. }
            | DataSourceDesc::OldSyntaxIngestion { desc, .. } = &source.data_source
            {
                match &desc.connection {
                    GenericSourceConnection::Postgres(conn) => {
                        let inline_conn = conn.clone().into_inline_connection(catalog);
                        Some(GenericSourceConnection::Postgres(inline_conn))
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    };

    ParsedStateUpdateKind::TemporaryItem {
        durable_item,
        parsed_item,
        connection,
        parsed_full_name,
    }
}

fn parse_cluster_update(
    catalog: &CatalogState,
    durable_cluster: durable::objects::Cluster,
    _diff: StateDiff,
) -> ParsedStateUpdateKind {
    let parsed_cluster = catalog.get_cluster(durable_cluster.id);

    ParsedStateUpdateKind::Cluster {
        durable_cluster,
        parsed_cluster: parsed_cluster.clone(),
    }
}

fn parse_cluster_replica_update(
    catalog: &CatalogState,
    durable_cluster_replica: durable::objects::ClusterReplica,
    _diff: StateDiff,
) -> ParsedStateUpdateKind {
    let parsed_cluster_replica = catalog.get_cluster_replica(
        durable_cluster_replica.cluster_id,
        durable_cluster_replica.replica_id,
    );

    ParsedStateUpdateKind::ClusterReplica {
        durable_cluster_replica,
        parsed_cluster_replica: parsed_cluster_replica.clone(),
    }
}
