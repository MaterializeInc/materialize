// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Admission for native warm handover, before changing durable authority.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog_protos::objects as proto;
use mz_controller_types::ClusterId;
use mz_repr::CatalogItemId;
use mz_sql_parser::ast::{RawClusterName, Statement};
use serde::Deserialize;
use uuid::Uuid;

use super::objects::serialization::ProtoType;
use super::objects::state_update::StateUpdateKindJson;
use super::{CatalogError, DurableCatalogError};

// Admission precedes migrations. Decode only the fields this policy needs, not
// complete current-version records whose unrelated required fields can evolve.
// These fields must decode from every supported protected catalog version.
// An incompatible encoding change makes promotion across that version fail closed.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum PromotionRecord {
    Cluster {
        key: proto::ClusterKey,
        value: Cluster,
    },
    Item {
        key: proto::ItemKey,
        value: Item,
    },
}

#[derive(Deserialize)]
struct Cluster {
    name: String,
    config: ClusterConfig,
}

#[derive(Deserialize)]
struct ClusterConfig {
    variant: ClusterVariant,
}

#[derive(Deserialize)]
enum ClusterVariant {
    Unmanaged,
    Managed(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
struct Item {
    definition: proto::CatalogItem,
}

/// Collects cleanup authority from the consolidated snapshot admitted by the
/// generation CAS. Only this stable field is decoded before catalog migrations.
pub(super) fn ephemeral_owners<'a>(
    snapshot: impl IntoIterator<Item = &'a StateUpdateKindJson>,
) -> Result<BTreeSet<Uuid>, CatalogError> {
    #[derive(Deserialize)]
    struct Record {
        value: Owner,
    }
    #[derive(Deserialize)]
    struct Owner {
        ephemeral_owner_session: Option<Uuid>,
    }

    let mut owners = BTreeSet::new();
    for update in snapshot {
        if update.kind() == "Item" {
            let record = update.try_to_serde::<Record>().map_err(|error| {
                DurableCatalogError::NotWritable(format!(
                    "cannot capture promotion cleanup owners: {error}"
                ))
            })?;
            owners.extend(record.value.ephemeral_owner_session);
        }
    }
    Ok(owners)
}

/// Validates a consolidated catalog snapshot. Explicit unmanaged replica
/// declarations remain valid targets, but managed pins cannot cross warm handover.
pub(super) fn validate_native_promotion<'a>(
    snapshot: impl IntoIterator<Item = &'a StateUpdateKindJson>,
) -> Result<(), CatalogError> {
    let mut clusters = BTreeMap::<ClusterId, Cluster>::new();
    let mut items = Vec::<(CatalogItemId, String)>::new();
    for update in snapshot {
        if !matches!(update.kind(), "Cluster" | "Item") {
            continue;
        }
        match update.try_to_serde::<PromotionRecord>().map_err(|error| {
            DurableCatalogError::NotWritable(format!("cannot validate warm handover: {error}"))
        })? {
            PromotionRecord::Cluster { key, value } => {
                clusters.insert(key.id.into_rust()?, value);
            }
            PromotionRecord::Item { key, value } => {
                let proto::CatalogItem::V1(definition) = value.definition;
                items.push((key.gid.into_rust()?, definition.create_sql));
            }
        }
    }
    for (id, create_sql) in items {
        let statements = mz_sql_parser::parser::parse_statements(&create_sql).map_err(|e| {
            DurableCatalogError::NotWritable(
                format!("cannot validate warm handover for {id}: {e}",),
            )
        })?;
        for statement in statements {
            let Statement::CreateMaterializedView(mv) = statement.ast else {
                continue;
            };
            if mv.in_cluster_replica.is_none() {
                continue;
            }
            let cluster = match mv.in_cluster {
                Some(RawClusterName::Resolved(id)) => {
                    id.parse().ok().and_then(|id| clusters.get(&id))
                }
                Some(RawClusterName::Unresolved(_)) | None => None,
            }
            .ok_or_else(|| {
                DurableCatalogError::NotWritable(format!(
                    "cannot resolve replica-targeted materialized view {} during warm handover",
                    id
                ))
            })?;
            if matches!(cluster.config.variant, ClusterVariant::Managed(_)) {
                return Err(DurableCatalogError::NotWritable(format!(
                    "native warm handover does not support replica-targeted materialized view {} on managed cluster {}",
                    id, cluster.name
                )).into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn cleanup_uses_only_owner_field_before_migration() {
        let owner = Uuid::new_v4();
        let item = |value| {
            StateUpdateKindJson::from_serde(serde_json::json!({
                "kind": "Item", "value": value
            }))
        };
        let temporary = item(serde_json::json!({"ephemeral_owner_session": owner}));
        let permanent = item(serde_json::json!({}));
        assert_eq!(
            ephemeral_owners([&temporary, &permanent]).expect("decode sparse owner records"),
            BTreeSet::from([owner]),
        );
        let malformed = item(serde_json::json!({"ephemeral_owner_session": "not a UUID"}));
        assert!(ephemeral_owners([&malformed]).is_err());
    }

    #[mz_ore::test]
    fn admission_uses_only_policy_fields_before_migration() {
        let cluster = |variant| {
            StateUpdateKindJson::from_serde(serde_json::json!({
                "kind": "Cluster", "key": {"id": {"User": 1}},
                "value": {"name": "c", "config": {"variant": variant}}
            }))
        };
        let item = StateUpdateKindJson::from_serde(serde_json::json!({
            "kind": "Item", "key": {"gid": {"User": 1}},
            "value": {"definition": {"V1": {"create_sql":
                "CREATE MATERIALIZED VIEW materialize.public.mv IN CLUSTER [u1] REPLICA r1 AS SELECT 1"
            }}}
        }));
        // Full current Cluster/Item records have additional required fields.
        // Admission must not make their migration a prerequisite for fencing.
        let unmanaged = cluster(serde_json::json!("Unmanaged"));
        assert!(validate_native_promotion([&unmanaged, &item]).is_ok());
        let managed = cluster(serde_json::json!({"Managed": {}}));
        let error = validate_native_promotion([&managed, &item])
            .expect_err("managed replica pins must reject warm handover");
        assert!(
            error
                .to_string()
                .contains("replica-targeted materialized view")
        );
    }
}
