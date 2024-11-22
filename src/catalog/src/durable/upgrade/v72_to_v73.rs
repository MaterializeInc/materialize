// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::wire_compatible;
use mz_proto::wire_compatible::WireCompatible;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v72 as v72, objects_v73 as v73};

wire_compatible!(v72::ClusterValue with v73::ClusterValue);
wire_compatible!(v72::ClusterReplicaKey with v73::ClusterReplicaKey);
wire_compatible!(v72::ReplicaConfig with v73::ReplicaConfig);
wire_compatible!(v72::CommentValue with v73::CommentValue);
wire_compatible!(v72::RoleId with v73::RoleId);
wire_compatible!(v72::ReplicaId with v73::ReplicaId);
wire_compatible!(v72::DatabaseId with v73::DatabaseId);
wire_compatible!(v72::ResolvedSchema with v73::ResolvedSchema);
wire_compatible!(v72::NetworkPolicyId with v73::NetworkPolicyId);

/// Adds the introspection source index global ID variant. Included in this change is shortening
/// cluster IDs from 64 bits to 32 bits.
pub fn upgrade(
    snapshot: Vec<v72::StateUpdateKind>,
) -> Vec<MigrationAction<v72::StateUpdateKind, v73::StateUpdateKind>> {
    let mut migrations = Vec::new();

    for update in snapshot {
        match update.kind {
            // Attempting to duplicate the introspection source index ID allocation logic in this
            // file would be extremely cumbersome and error-prone. Instead, we delete all existing
            // introspection source indexes and let the builtin migration code recreate them with
            // the new correct IDs.
            Some(v72::state_update_kind::Kind::ClusterIntrospectionSourceIndex(_)) => {
                let migration = MigrationAction::Delete(update);
                migrations.push(migration);
            }
            Some(v72::state_update_kind::Kind::Comment(comment)) => {
                let new_comment = (&comment).into();
                let old = v72::StateUpdateKind {
                    kind: Some(v72::state_update_kind::Kind::Comment(comment)),
                };
                let new = v73::StateUpdateKind {
                    kind: Some(v73::state_update_kind::Kind::Comment(new_comment)),
                };
                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            Some(v72::state_update_kind::Kind::ClusterReplica(cluster_replica)) => {
                let new_cluster_replica = (&cluster_replica).into();
                let old = v72::StateUpdateKind {
                    kind: Some(v72::state_update_kind::Kind::ClusterReplica(
                        cluster_replica,
                    )),
                };
                let new = v73::StateUpdateKind {
                    kind: Some(v73::state_update_kind::Kind::ClusterReplica(
                        new_cluster_replica,
                    )),
                };
                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            Some(v72::state_update_kind::Kind::Cluster(cluster)) => {
                let new_cluster = (&cluster).into();
                let old = v72::StateUpdateKind {
                    kind: Some(v72::state_update_kind::Kind::Cluster(cluster)),
                };
                let new = v73::StateUpdateKind {
                    kind: Some(v73::state_update_kind::Kind::Cluster(new_cluster)),
                };
                let migration = MigrationAction::Update(old, new);
                migrations.push(migration);
            }
            _ => {}
        }
    }

    migrations
}

// COMMENTS

impl From<&v72::state_update_kind::Comment> for v73::state_update_kind::Comment {
    fn from(comment: &v72::state_update_kind::Comment) -> Self {
        let key = comment.key.as_ref().map(Into::into);
        let value = comment.value.as_ref().map(WireCompatible::convert);
        Self { key, value }
    }
}

impl From<&v72::CommentKey> for v73::CommentKey {
    fn from(comment_key: &v72::CommentKey) -> Self {
        let object = comment_key.object.as_ref().map(Into::into);
        let sub_component = comment_key.sub_component.as_ref().map(Into::into);
        Self {
            object,
            sub_component,
        }
    }
}

impl From<&v72::comment_key::SubComponent> for v73::comment_key::SubComponent {
    fn from(sub_component: &v72::comment_key::SubComponent) -> Self {
        match sub_component {
            v72::comment_key::SubComponent::ColumnPos(p) => {
                v73::comment_key::SubComponent::ColumnPos(*p)
            }
        }
    }
}

impl From<&v72::comment_key::Object> for v73::comment_key::Object {
    fn from(object: &v72::comment_key::Object) -> Self {
        match object {
            v72::comment_key::Object::Table(id) => v73::comment_key::Object::Table(id.into()),
            v72::comment_key::Object::View(id) => v73::comment_key::Object::View(id.into()),
            v72::comment_key::Object::MaterializedView(id) => {
                v73::comment_key::Object::MaterializedView(id.into())
            }
            v72::comment_key::Object::Source(id) => v73::comment_key::Object::Source(id.into()),
            v72::comment_key::Object::Sink(id) => v73::comment_key::Object::Sink(id.into()),
            v72::comment_key::Object::Index(id) => v73::comment_key::Object::Index(id.into()),
            v72::comment_key::Object::Func(id) => v73::comment_key::Object::Func(id.into()),
            v72::comment_key::Object::Connection(id) => {
                v73::comment_key::Object::Connection(id.into())
            }
            v72::comment_key::Object::Type(id) => v73::comment_key::Object::Type(id.into()),
            v72::comment_key::Object::Secret(id) => v73::comment_key::Object::Secret(id.into()),
            v72::comment_key::Object::ContinualTask(id) => {
                v73::comment_key::Object::ContinualTask(id.into())
            }
            v72::comment_key::Object::Role(id) => {
                v73::comment_key::Object::Role(WireCompatible::convert(id))
            }
            v72::comment_key::Object::Database(id) => {
                v73::comment_key::Object::Database(WireCompatible::convert(id))
            }
            v72::comment_key::Object::Schema(id) => {
                v73::comment_key::Object::Schema(WireCompatible::convert(id))
            }
            v72::comment_key::Object::Cluster(id) => v73::comment_key::Object::Cluster(id.into()),
            v72::comment_key::Object::ClusterReplica(id) => {
                v73::comment_key::Object::ClusterReplica(id.into())
            }
            v72::comment_key::Object::NetworkPolicy(id) => {
                v73::comment_key::Object::NetworkPolicy(WireCompatible::convert(id))
            }
        }
    }
}

impl From<&v72::ClusterReplicaId> for v73::ClusterReplicaId {
    fn from(cluster_replica_id: &v72::ClusterReplicaId) -> Self {
        let cluster_id = cluster_replica_id.cluster_id.as_ref().map(Into::into);
        let replica_id = cluster_replica_id
            .replica_id
            .as_ref()
            .map(WireCompatible::convert);
        Self {
            cluster_id,
            replica_id,
        }
    }
}

// CLUSTER REPLICAS

impl From<&v72::state_update_kind::ClusterReplica> for v73::state_update_kind::ClusterReplica {
    fn from(cluster_replica: &v72::state_update_kind::ClusterReplica) -> Self {
        let key = cluster_replica.key.as_ref().map(WireCompatible::convert);
        let value = cluster_replica.value.as_ref().map(Into::into);
        Self { key, value }
    }
}

impl From<&v72::ClusterReplicaValue> for v73::ClusterReplicaValue {
    fn from(value: &v72::ClusterReplicaValue) -> Self {
        let cluster_id = value.cluster_id.as_ref().map(Into::into);
        let name = value.name.clone();
        let config = value.config.as_ref().map(WireCompatible::convert);
        let owner_id = value.owner_id.as_ref().map(WireCompatible::convert);
        Self {
            cluster_id,
            name,
            config,
            owner_id,
        }
    }
}

// CLUSTERS

impl From<&v72::state_update_kind::Cluster> for v73::state_update_kind::Cluster {
    fn from(cluster: &v72::state_update_kind::Cluster) -> Self {
        let key = cluster.key.as_ref().map(Into::into);
        let value = cluster.value.as_ref().map(WireCompatible::convert);
        Self { key, value }
    }
}

impl From<&v72::ClusterKey> for v73::ClusterKey {
    fn from(cluster_key: &v72::ClusterKey) -> Self {
        let id = cluster_key.id.as_ref().map(Into::into);
        Self { id }
    }
}

// MISC

impl From<&v72::ClusterId> for v73::ClusterId {
    fn from(id: &v72::ClusterId) -> Self {
        let value = id.value.map(|value| match value {
            v72::cluster_id::Value::System(id) => {
                v73::cluster_id::Value::System(migrate_cluster_id(id))
            }
            v72::cluster_id::Value::User(id) => {
                v73::cluster_id::Value::User(migrate_cluster_id(id))
            }
        });
        Self { value }
    }
}

impl From<&v72::CatalogItemId> for v73::CatalogItemId {
    fn from(id: &v72::CatalogItemId) -> Self {
        let value = id.value.as_ref().map(Into::into);
        Self { value }
    }
}

impl From<&v72::catalog_item_id::Value> for v73::catalog_item_id::Value {
    fn from(value: &v72::catalog_item_id::Value) -> Self {
        match value {
            v72::catalog_item_id::Value::System(id) => v73::catalog_item_id::Value::System(*id),
            v72::catalog_item_id::Value::User(id) => v73::catalog_item_id::Value::User(*id),
            v72::catalog_item_id::Value::Transient(id) => {
                v73::catalog_item_id::Value::Transient(*id)
            }
        }
    }
}

fn migrate_cluster_id(id: u64) -> u32 {
    id.try_into().expect("cluster ID will not exceed u32")
}
