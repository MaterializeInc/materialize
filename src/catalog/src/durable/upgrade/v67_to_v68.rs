// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::traits::{UpgradeFrom, UpgradeInto};
use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::{JsonCompatible, json_compatible};
use crate::durable::upgrade::{objects_v67 as v67, objects_v68 as v68};

json_compatible!(v67::GlobalId with v68::GlobalId);
json_compatible!(v67::CatalogItem with v68::CatalogItem);
json_compatible!(v67::SchemaId with v68::SchemaId);
json_compatible!(v67::CommentValue with v68::CommentValue);
json_compatible!(v67::RoleId with v68::RoleId);
json_compatible!(v67::MzAclItem with v68::MzAclItem);
json_compatible!(v67::DatabaseId with v68::DatabaseId);
json_compatible!(v67::ResolvedSchema with v68::ResolvedSchema);
json_compatible!(v67::ClusterId with v68::ClusterId);
json_compatible!(v67::ClusterReplicaId with v68::ClusterReplicaId);
json_compatible!(v67::SourceReferencesValue with v68::SourceReferencesValue);
json_compatible!(v67::GidMappingKey with v68::GidMappingKey);
json_compatible!(v67::ClusterIntrospectionSourceIndexKey with v68::ClusterIntrospectionSourceIndexKey);

/// In v68 we switched catalog items to be keyed on a `CatalogItemId`, this required a few changes:
///
/// * `ItemKey` switched from containing a single `GlobalId` to a `CatalogItemId`.
/// * `ItemValue` added `global_id: GlobalId` and `extra_versions: BTreeMap<Version, GlobalId>` fields.
/// * `CommentKey` switched from using `GlobalId` to `CatalogItemId`.
/// * `SourceReferencesKey` switched from `GlobalId` to `CatalogItemId`
/// * `GidMappingValue` switched from using raw `uint64` for an id to newtype
///   `SystemCatalogItemId` and `SystemGlobalId` wrappers.
///
/// All switches from `GlobalId` to `CatalogItemId` we re-use the inner value of the ID.
pub fn upgrade(
    snapshot: Vec<v67::StateUpdateKind>,
) -> Vec<MigrationAction<v67::StateUpdateKind, v68::StateUpdateKind>> {
    snapshot
        .iter()
        .filter_map(|update| match &update.kind {
            Some(v67::state_update_kind::Kind::Item(old_item)) => {
                // ** MIGRATION **
                let new_item = v68::state_update_kind::Item::upgrade_from(old_item.clone());

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
                let new_comment =
                    v68::state_update_kind::Comment::upgrade_from(old_comment.clone());

                let old_comment = v67::StateUpdateKind {
                    kind: Some(v67::state_update_kind::Kind::Comment(old_comment.clone())),
                };
                let new_comment = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::Comment(new_comment.clone())),
                };

                Some(MigrationAction::Update(old_comment, new_comment))
            }
            Some(v67::state_update_kind::Kind::SourceReferences(old_reference)) => {
                // ** MIGRATION **
                let new_reference =
                    v68::state_update_kind::SourceReferences::upgrade_from(old_reference.clone());

                let old_reference = v67::StateUpdateKind {
                    kind: Some(v67::state_update_kind::Kind::SourceReferences(
                        old_reference.clone(),
                    )),
                };
                let new_reference = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::SourceReferences(
                        new_reference,
                    )),
                };

                Some(MigrationAction::Update(old_reference, new_reference))
            }
            Some(v67::state_update_kind::Kind::GidMapping(old_mapping)) => {
                let new_mapping = v68::state_update_kind::GidMapping {
                    key: old_mapping.key.as_ref().map(v68::GidMappingKey::convert),
                    // ** MIGRATION **
                    value: old_mapping
                        .value
                        .as_ref()
                        .map(|old| v68::GidMappingValue::upgrade_from(old.clone())),
                };

                let old_mapping = v67::StateUpdateKind {
                    kind: Some(v67::state_update_kind::Kind::GidMapping(
                        old_mapping.clone(),
                    )),
                };
                let new_mapping = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::GidMapping(new_mapping)),
                };

                Some(MigrationAction::Update(old_mapping, new_mapping))
            }
            Some(v67::state_update_kind::Kind::ClusterIntrospectionSourceIndex(old_index)) => {
                let new_index = v68::state_update_kind::ClusterIntrospectionSourceIndex {
                    key: old_index
                        .key
                        .as_ref()
                        .map(v68::ClusterIntrospectionSourceIndexKey::convert),
                    value: old_index.value.as_ref().map(|old| {
                        v68::ClusterIntrospectionSourceIndexValue::upgrade_from(old.clone())
                    }),
                };

                let old_index = v67::StateUpdateKind {
                    kind: Some(
                        v67::state_update_kind::Kind::ClusterIntrospectionSourceIndex(
                            old_index.clone(),
                        ),
                    ),
                };
                let new_index = v68::StateUpdateKind {
                    kind: Some(
                        v68::state_update_kind::Kind::ClusterIntrospectionSourceIndex(new_index),
                    ),
                };

                Some(MigrationAction::Update(old_index, new_index))
            }
            _ => None,
        })
        .collect()
}

impl UpgradeFrom<v67::state_update_kind::Item> for v68::state_update_kind::Item {
    fn upgrade_from(value: v67::state_update_kind::Item) -> Self {
        let new_key = value.key.map(|k| v68::ItemKey {
            gid: k.gid.map(v68::CatalogItemId::upgrade_from),
        });
        let new_val = value.value.map(|val| v68::ItemValue {
            global_id: value
                .key
                .as_ref()
                .and_then(|k| k.gid)
                .map(|gid| v68::GlobalId::convert(&gid)),
            schema_id: val.schema_id.map(|id| v68::SchemaId::convert(&id)),
            definition: val.definition.map(|def| v68::CatalogItem::convert(&def)),
            name: val.name,
            owner_id: val.owner_id.map(|id| v68::RoleId::convert(&id)),
            privileges: val
                .privileges
                .into_iter()
                .map(|item| v68::MzAclItem::convert(&item))
                .collect(),
            oid: val.oid,
            // Nothing supports extra versions yet, but with ALTER TABLE we will.
            extra_versions: Vec::new(),
        });

        v68::state_update_kind::Item {
            key: new_key,
            value: new_val,
        }
    }
}

impl UpgradeFrom<v67::state_update_kind::Comment> for v68::state_update_kind::Comment {
    fn upgrade_from(value: v67::state_update_kind::Comment) -> Self {
        let new_key = value.key.map(|k| v68::CommentKey {
            object: k.object.map(v68::comment_key::Object::upgrade_from),
            sub_component: k
                .sub_component
                .map(v68::comment_key::SubComponent::upgrade_from),
        });
        let new_val = value.value.map(|v| v68::CommentValue::convert(&v));

        v68::state_update_kind::Comment {
            key: new_key,
            value: new_val,
        }
    }
}

impl UpgradeFrom<v67::comment_key::Object> for v68::comment_key::Object {
    fn upgrade_from(value: v67::comment_key::Object) -> Self {
        match value {
            v67::comment_key::Object::Table(global_id) => {
                v68::comment_key::Object::Table(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::View(global_id) => {
                v68::comment_key::Object::View(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::MaterializedView(global_id) => {
                v68::comment_key::Object::MaterializedView(v68::CatalogItemId::upgrade_from(
                    global_id,
                ))
            }
            v67::comment_key::Object::Source(global_id) => {
                v68::comment_key::Object::Source(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Sink(global_id) => {
                v68::comment_key::Object::Sink(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Index(global_id) => {
                v68::comment_key::Object::Index(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Func(global_id) => {
                v68::comment_key::Object::Func(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Connection(global_id) => {
                v68::comment_key::Object::Connection(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Type(global_id) => {
                v68::comment_key::Object::Type(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::Secret(global_id) => {
                v68::comment_key::Object::Secret(v68::CatalogItemId::upgrade_from(global_id))
            }
            v67::comment_key::Object::ContinualTask(global_id) => {
                v68::comment_key::Object::ContinualTask(v68::CatalogItemId::upgrade_from(global_id))
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

impl UpgradeFrom<v67::comment_key::SubComponent> for v68::comment_key::SubComponent {
    fn upgrade_from(value: v67::comment_key::SubComponent) -> Self {
        match value {
            v67::comment_key::SubComponent::ColumnPos(x) => {
                v68::comment_key::SubComponent::ColumnPos(x)
            }
        }
    }
}

impl UpgradeFrom<v67::GlobalId> for v68::CatalogItemId {
    fn upgrade_from(id: v67::GlobalId) -> Self {
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

impl UpgradeFrom<v67::state_update_kind::SourceReferences>
    for v68::state_update_kind::SourceReferences
{
    fn upgrade_from(old: v67::state_update_kind::SourceReferences) -> Self {
        v68::state_update_kind::SourceReferences {
            key: old.key.map(|old| old.upgrade_into()),
            value: old.value.map(|old| JsonCompatible::convert(&old)),
        }
    }
}

impl UpgradeFrom<v67::SourceReferencesKey> for v68::SourceReferencesKey {
    fn upgrade_from(value: v67::SourceReferencesKey) -> Self {
        let source = match value.source {
            Some(gid) => Some(gid.upgrade_into()),
            None => None,
        };
        v68::SourceReferencesKey { source }
    }
}

impl UpgradeFrom<v67::GidMappingValue> for v68::GidMappingValue {
    fn upgrade_from(value: v67::GidMappingValue) -> Self {
        v68::GidMappingValue {
            id: value.id,
            global_id: Some(v68::SystemGlobalId { value: value.id }),
            fingerprint: value.fingerprint,
        }
    }
}

impl UpgradeFrom<v67::ClusterIntrospectionSourceIndexValue>
    for v68::ClusterIntrospectionSourceIndexValue
{
    fn upgrade_from(value: v67::ClusterIntrospectionSourceIndexValue) -> Self {
        v68::ClusterIntrospectionSourceIndexValue {
            index_id: value.index_id,
            global_id: Some(v68::SystemGlobalId {
                value: value.index_id,
            }),
            oid: value.oid,
        }
    }
}
