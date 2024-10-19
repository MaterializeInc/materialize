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
use crate::durable::upgrade::{objects_v68 as v68, objects_v69 as v69};

wire_compatible!(v68::ItemKey with v69::ItemKey);
wire_compatible!(v68::CatalogItem with v69::CatalogItem);
wire_compatible!(v68::SchemaId with v69::SchemaId);
wire_compatible!(v68::RoleId with v69::RoleId);
wire_compatible!(v68::MzAclItem with v69::MzAclItem);
wire_compatible!(v68::SourceReferencesValue with v69::SourceReferencesValue);
wire_compatible!(v68::GidMappingKey with v69::GidMappingKey);

/// In v69 we migrated a few things:
///
/// * `SourceReferencesKey` to use `CatalogItemId`.
/// * `GidMappingValue` to include both a `CatalogItemId` and `GlobalId`.
///
pub fn upgrade(
    snapshot: Vec<v68::StateUpdateKind>,
) -> Vec<MigrationAction<v68::StateUpdateKind, v69::StateUpdateKind>> {
    snapshot
        .into_iter()
        .filter_map(|update| match update.kind {
            Some(v68::state_update_kind::Kind::Item(old_item)) => {
                // ** MIGRATION **
                let new_item = v69::state_update_kind::Item::from(old_item.clone());

                let old_item = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::Item(old_item)),
                };
                let new_item = v69::StateUpdateKind {
                    kind: Some(v69::state_update_kind::Kind::Item(new_item)),
                };

                Some(MigrationAction::Update(old_item, new_item))
            }
            Some(v68::state_update_kind::Kind::SourceReferences(old_reference)) => {
                // ** MIGRATION **
                let new_reference =
                    v69::state_update_kind::SourceReferences::from(old_reference.clone());

                let old_reference = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::SourceReferences(
                        old_reference,
                    )),
                };
                let new_reference = v69::StateUpdateKind {
                    kind: Some(v69::state_update_kind::Kind::SourceReferences(
                        new_reference,
                    )),
                };

                Some(MigrationAction::Update(old_reference, new_reference))
            }
            Some(v68::state_update_kind::Kind::GidMapping(old_mapping)) => {
                let new_mapping = v69::state_update_kind::GidMapping {
                    key: old_mapping
                        .key
                        .as_ref()
                        .map(v69::GidMappingKey::convert),
                    // ** MIGRATION **
                    value: old_mapping
                        .value
                        .as_ref()
                        .map(|old| v69::GidMappingValue::from(old.clone())),
                };

                let old_mapping = v68::StateUpdateKind {
                    kind: Some(v68::state_update_kind::Kind::GidMapping(old_mapping)),
                };
                let new_mapping = v69::StateUpdateKind {
                    kind: Some(v69::state_update_kind::Kind::GidMapping(new_mapping)),
                };

                Some(MigrationAction::Update(old_mapping, new_mapping))
            }
            _ => None,
        })
        .collect()
}

impl From<v68::state_update_kind::Item> for v69::state_update_kind::Item {
    fn from(item: v68::state_update_kind::Item) -> v69::state_update_kind::Item {
        // Initially the `GlobalId` for an object will exactly match the `CatalogItemId`.
        let global_id = item
            .key
            .as_ref()
            .and_then(|key| key.id)
            .map(|id| match id.value {
                Some(v68::catalog_item_id::Value::User(x)) => Some(v69::global_id::Value::User(x)),
                Some(v68::catalog_item_id::Value::System(x)) => {
                    Some(v69::global_id::Value::System(x))
                }
                Some(v68::catalog_item_id::Value::Transient(x)) => {
                    Some(v69::global_id::Value::Transient(x))
                }
                None => None,
            })
            .map(|value| v69::GlobalId { value });

        let key = item.key.map(|key| v69::ItemKey::convert(&key));
        let value = match item.value {
            Some(value) => Some(v69::ItemValue {
                global_id,
                schema_id: value.schema_id.map(|id| v69::SchemaId::convert(&id)),
                definition: value.definition.map(|def| v69::CatalogItem::convert(&def)),
                name: value.name,
                owner_id: value.owner_id.map(|id| v69::RoleId::convert(&id)),
                privileges: value
                    .privileges
                    .into_iter()
                    .map(|item| v69::MzAclItem::convert(&item))
                    .collect(),
                oid: value.oid,
                versions: Vec::new(),
            }),
            None => None,
        };

        v69::state_update_kind::Item { key, value }
    }
}

impl From<v68::state_update_kind::SourceReferences> for v69::state_update_kind::SourceReferences {
    fn from(old: v68::state_update_kind::SourceReferences) -> Self {
        v69::state_update_kind::SourceReferences {
            key: old.key.map(|old| old.into()),
            value: old.value.map(|old| WireCompatible::convert(&old)),
        }
    }
}

impl From<v68::SourceReferencesKey> for v69::SourceReferencesKey {
    fn from(value: v68::SourceReferencesKey) -> Self {
        let source = match value.source {
            Some(gid) => Some(gid.into()),
            None => None,
        };
        v69::SourceReferencesKey { source }
    }
}

impl From<v68::GlobalId> for v69::CatalogItemId {
    fn from(gid: v68::GlobalId) -> Self {
        let value = match gid.value {
            Some(v68::global_id::Value::User(x)) => Some(v69::catalog_item_id::Value::User(x)),
            Some(v68::global_id::Value::System(x)) => Some(v69::catalog_item_id::Value::System(x)),
            Some(v68::global_id::Value::Transient(x)) => {
                Some(v69::catalog_item_id::Value::Transient(x))
            }
            Some(v68::global_id::Value::Explain(_)) => {
                unreachable!("found GlobalId::Explain in the Catalog")
            }
            None => None,
        };
        v69::CatalogItemId { value }
    }
}

impl From<v68::GidMappingValue> for v69::GidMappingValue {
    fn from(value: v68::GidMappingValue) -> Self {
        v69::GidMappingValue {
            catalog_id: Some(v69::SystemCatalogItemId { value: value.id }),
            global_id: Some(v69::SystemGlobalId { value: value.id }),
            fingerprint: value.fingerprint,
        }
    }
}
