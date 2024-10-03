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
use crate::durable::upgrade::{objects_v67 as v67, objects_v68 as v68};

wire_compatible!(v67::ItemValue with v68::ItemValue);
wire_compatible!(v67::CommentValue with v68::CommentValue);
wire_compatible!(v67::RoleId with v68::RoleId);
wire_compatible!(v67::DatabaseId with v68::DatabaseId);
wire_compatible!(v67::ResolvedSchema with v68::ResolvedSchema);
wire_compatible!(v67::ClusterId with v68::ClusterId);
wire_compatible!(v67::ClusterReplicaId with v68::ClusterReplicaId);

/// In v68 we migrated Catalog items from being keyed on `GlobalId` to `CatalogItemId`.
pub fn upgrade(
    snapshot: Vec<v67::StateUpdateKind>,
) -> Vec<MigrationAction<v67::StateUpdateKind, v68::StateUpdateKind>> {
    snapshot
        .iter()
        .filter_map(|update| match &update.kind {
            Some(v67::state_update_kind::Kind::Item(old_item)) => {
                // ** MIGRATION **
                let new_item = v68::state_update_kind::Item::from(old_item.clone());

                let old_item = v67::StateUpdateKind {
                    kind: Some(v67::state_update_kind::Kind::Item(old_item.clone())),
                };
                let new_item = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::Item(new_item)),
                };

                Some(MigrationAction::Update(old_item, new_item))
            }
            Some(v67::state_update_kind::Kind::Comment(old_comment)) => {
                // ** MIGRATION **
                let new_comment = v68::state_update_kind::Comment::from(old_comment.clone());

                let old_comment = v67::StateUpdateKind {
                    kind: Some(v67::state_update_kind::Kind::Comment(old_comment.clone())),
                };
                let new_comment = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::Comment(new_comment.clone())),
                };

                Some(MigrationAction::Update(old_comment, new_comment))
            }
            _ => None,
        })
        .collect()
}

impl From<v67::state_update_kind::Item> for v68::state_update_kind::Item {
    fn from(value: v67::state_update_kind::Item) -> Self {
        let new_key = value.key.map(|k| v68::ItemKey {
            id: k.gid.map(v68::CatalogItemId::from),
        });
        let new_val = value.value.map(|v| v68::ItemValue::convert(&v));

        v68::state_update_kind::Item {
            key: new_key,
            value: new_val,
        }
    }
}

impl From<v67::state_update_kind::Comment> for v68::state_update_kind::Comment {
    fn from(value: v67::state_update_kind::Comment) -> Self {
        let new_key = value.key.map(|k| v68::CommentKey {
            object: k.object.map(v68::comment_key::Object::from),
            sub_component: k.sub_component.map(v68::comment_key::SubComponent::from),
        });
        let new_val = value.value.map(|v| v68::CommentValue::convert(&v));

        v68::state_update_kind::Comment {
            key: new_key,
            value: new_val,
        }
    }
}

impl From<v67::comment_key::Object> for v68::comment_key::Object {
    fn from(value: v67::comment_key::Object) -> Self {
        match value {
            v67::comment_key::Object::Table(global_id) => {
                v68::comment_key::Object::Table(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::View(global_id) => {
                v68::comment_key::Object::View(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::MaterializedView(global_id) => {
                v68::comment_key::Object::MaterializedView(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Source(global_id) => {
                v68::comment_key::Object::Source(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Sink(global_id) => {
                v68::comment_key::Object::Sink(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Index(global_id) => {
                v68::comment_key::Object::Index(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Func(global_id) => {
                v68::comment_key::Object::Func(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Connection(global_id) => {
                v68::comment_key::Object::Connection(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Type(global_id) => {
                v68::comment_key::Object::Type(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Secret(global_id) => {
                v68::comment_key::Object::Secret(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::ContinualTask(global_id) => {
                v68::comment_key::Object::ContinualTask(v68::CatalogItemId::from(global_id))
            }
            v67::comment_key::Object::Role(role_id) => {
                v68::comment_key::Object::Role(v68::RoleId::convert(&role_id))
            }
            v67::comment_key::Object::Database(database_id) => {
                v68::comment_key::Object::Database(v68::DatabaseId::convert(&database_id))
            }
            v67::comment_key::Object::Schema(resolved_schema) => {
                v68::comment_key::Object::Schema(v68::ResolvedSchema::convert(&resolved_schema))
            }
            v67::comment_key::Object::Cluster(cluster_id) => {
                v68::comment_key::Object::Cluster(v68::ClusterId::convert(&cluster_id))
            }
            v67::comment_key::Object::ClusterReplica(cluster_replica_id) => {
                v68::comment_key::Object::ClusterReplica(v68::ClusterReplicaId::convert(
                    &cluster_replica_id,
                ))
            }
        }
    }
}

impl From<v67::comment_key::SubComponent> for v68::comment_key::SubComponent {
    fn from(value: v67::comment_key::SubComponent) -> Self {
        match value {
            v67::comment_key::SubComponent::ColumnPos(x) => {
                v68::comment_key::SubComponent::ColumnPos(x)
            }
        }
    }
}

impl From<v67::GlobalId> for v68::CatalogItemId {
    fn from(id: v67::GlobalId) -> Self {
        let value = match id.value {
            Some(v67::global_id::Value::System(x)) => Some(v68::catalog_item_id::Value::System(x)),
            Some(v67::global_id::Value::User(x)) => Some(v68::catalog_item_id::Value::User(x)),
            Some(v67::global_id::Value::Transient(x)) => {
                Some(v68::catalog_item_id::Value::Transient(x))
            }
            None => None,
            Some(v67::global_id::Value::Explain(_)) => unreachable!("shouldn't persist Explain"),
        };
        v68::CatalogItemId { value }
    }
}
