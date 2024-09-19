// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Side effects derived from catalog changes that can be applied to a
//! controller.

use itertools::Itertools;
use mz_catalog::memory::objects::{
    CatalogItem, Connection, ContinualTask, Index, MaterializedView, Source, StateDiff,
    StateUpdateKind, Table,
};
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_repr::{CatalogItemId, GlobalId};

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::CatalogState;

/// An update that needs to be applied to a controller.
#[derive(Debug, Clone)]
pub enum CatalogSideEffect {
    CreateTable(CatalogItemId, GlobalId, Table),
    DropTable(CatalogItemId, GlobalId),
    DropSource(CatalogItemId, GlobalId, Source),
    DropView(CatalogItemId, GlobalId),
    DropMaterializedView(CatalogItemId, GlobalId, MaterializedView),
    DropSink(CatalogItemId, GlobalId),
    DropIndex(CatalogItemId, GlobalId, Index),
    DropContinualTask(CatalogItemId, GlobalId, ContinualTask),
    DropSecret(CatalogItemId, GlobalId),
    DropConnection(CatalogItemId, GlobalId, Connection),
    DropCluster(ClusterId),
    DropClusterReplica {
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    },
}

impl CatalogState {
    /// Generate a list of [CatalogSideEffects](CatalogSideEffect) that
    /// correspond to a single update made to the durable catalog.
    #[instrument(level = "debug")]
    pub(crate) fn generate_side_effects(
        &self,
        kind: StateUpdateKind,
        diff: StateDiff,
    ) -> Vec<CatalogSideEffect> {
        // WIP: Exhaustive match?
        match kind {
            StateUpdateKind::Item(item) => self.generate_item_update(item.id, diff),
            StateUpdateKind::Cluster(cluster) => {
                self.generate_cluster_update(cluster.id.clone(), diff)
            }
            StateUpdateKind::ClusterReplica(replica) => self.generate_cluster_replica_update(
                replica.cluster_id.clone(),
                replica.replica_id.clone(),
                diff,
            ),
            _ => Vec::new(),
        }
    }

    fn generate_item_update(&self, id: CatalogItemId, diff: StateDiff) -> Vec<CatalogSideEffect> {
        let entry = self.get_entry(&id);
        let id = entry.id();
        let global_ids = entry.global_ids();

        let updates = match diff {
            StateDiff::Addition => match entry.item() {
                CatalogItem::Table(table) => {
                    // TODO: Correctly handle multiple global IDs.
                    global_ids
                        .map(|gid| CatalogSideEffect::CreateTable(id, gid, table.clone()))
                        .collect_vec()
                }
                _ => {
                    // Not handling anything but creating tables using the side
                    // effects framework yet.
                    Vec::new()
                }
            },
            StateDiff::Retraction => match entry.item() {
                CatalogItem::Table(_) => global_ids
                    .map(|gid| CatalogSideEffect::DropTable(id, gid))
                    .collect_vec(),
                CatalogItem::Source(source) => {
                    vec![CatalogSideEffect::DropSource(
                        id,
                        source.global_id(),
                        source.clone(),
                    )]
                }
                CatalogItem::View(view) => {
                    vec![CatalogSideEffect::DropView(id, view.global_id())]
                }
                CatalogItem::MaterializedView(mv) => {
                    vec![CatalogSideEffect::DropMaterializedView(
                        id,
                        mv.global_id(),
                        mv.clone(),
                    )]
                }
                CatalogItem::Sink(sink) => {
                    vec![CatalogSideEffect::DropSink(id, sink.global_id())]
                }
                CatalogItem::Index(index) => {
                    vec![CatalogSideEffect::DropIndex(
                        id,
                        index.global_id(),
                        index.clone(),
                    )]
                }
                CatalogItem::Secret(secret) => {
                    vec![CatalogSideEffect::DropSecret(id, secret.global_id.clone())]
                }
                CatalogItem::Connection(conn) => {
                    vec![CatalogSideEffect::DropConnection(
                        id,
                        conn.global_id(),
                        conn.clone(),
                    )]
                }
                CatalogItem::Type(_) => {
                    // Nothing to do!
                    Vec::new()
                }
                CatalogItem::Func(_) => {
                    // Nothing to do!
                    Vec::new()
                }
                CatalogItem::Log(_) => {
                    unreachable!("cannot drop builtin log source")
                }
                CatalogItem::ContinualTask(ct) => {
                    vec![CatalogSideEffect::DropContinualTask(
                        id,
                        ct.global_id(),
                        ct.clone(),
                    )]
                }
            },
        };

        updates
    }

    fn generate_cluster_update(
        &self,
        cluster_id: ClusterId,
        diff: StateDiff,
    ) -> Vec<CatalogSideEffect> {
        let updates = match diff {
            StateDiff::Addition => {
                // TODO: Only handling drops via the side effects framework for
                // now.
                Vec::new()
            }
            StateDiff::Retraction => {
                vec![CatalogSideEffect::DropCluster(cluster_id)]
            }
        };

        updates
    }

    fn generate_cluster_replica_update(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        diff: StateDiff,
    ) -> Vec<CatalogSideEffect> {
        let updates = match diff {
            StateDiff::Addition => {
                // TODO: Only handling drops via the side effects framework for
                // now.
                Vec::new()
            }
            StateDiff::Retraction => {
                vec![CatalogSideEffect::DropClusterReplica {
                    cluster_id,
                    replica_id,
                }]
            }
        };

        updates
    }
}
