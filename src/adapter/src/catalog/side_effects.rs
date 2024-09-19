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

use mz_catalog::memory::objects::{
    CatalogItem, Connection, Index, MaterializedView, Source, StateDiff, StateUpdateKind, Table,
};
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_repr::GlobalId;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::CatalogState;

/// An update that needs to be applied to a controller.
#[derive(Debug, Clone)]
pub enum CatalogSideEffect {
    CreateTable(GlobalId, Table),
    DropTable(GlobalId),
    DropSource(GlobalId, Source),
    DropView(GlobalId),
    DropMaterializedView(GlobalId, MaterializedView),
    DropSink(GlobalId),
    DropIndex(GlobalId, Index),
    DropSecret(GlobalId),
    DropConnection(GlobalId, Connection),
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

    fn generate_item_update(&self, id: GlobalId, diff: StateDiff) -> Vec<CatalogSideEffect> {
        let entry = self.get_entry(&id);
        let id = entry.id();

        let updates = match diff {
            StateDiff::Addition => match entry.item() {
                CatalogItem::Table(table) => {
                    vec![CatalogSideEffect::CreateTable(id, table.clone())]
                }
                _ => {
                    // Not handling anything but creating tables using the side
                    // effects framework yet.
                    Vec::new()
                }
            },
            StateDiff::Retraction => match entry.item() {
                CatalogItem::Table(_) => {
                    vec![CatalogSideEffect::DropTable(id)]
                }
                CatalogItem::Source(source) => {
                    vec![CatalogSideEffect::DropSource(id, source.clone())]
                }
                CatalogItem::View(_) => {
                    vec![CatalogSideEffect::DropView(id)]
                }
                CatalogItem::MaterializedView(mv) => {
                    vec![CatalogSideEffect::DropMaterializedView(id, mv.clone())]
                }
                CatalogItem::Sink(_) => {
                    vec![CatalogSideEffect::DropSink(id)]
                }
                CatalogItem::Index(index) => {
                    vec![CatalogSideEffect::DropIndex(id, index.clone())]
                }
                CatalogItem::Secret(_) => {
                    vec![CatalogSideEffect::DropSecret(id)]
                }
                CatalogItem::Connection(conn) => {
                    vec![CatalogSideEffect::DropConnection(id, conn.clone())]
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
