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

/// Selects realization metadata and protection owned by fenced deployments.
/// Commit these exact rows with the fence derived from the same consolidated
/// snapshot. Shared identities and warmed or future deployments remain untouched.
pub(super) fn retired_realizations<'a>(
    snapshot: impl Iterator<Item = &'a StateUpdateKindJson> + Clone,
    generation: u64,
) -> Result<Vec<StateUpdateKindJson>, CatalogError> {
    #[derive(Deserialize)]
    struct Key<K> {
        key: K,
    }
    #[derive(Deserialize)]
    struct Value<V> {
        value: V,
    }
    #[derive(Deserialize)]
    struct ClientGeneration {
        // The durable encoding omits generation zero.
        #[serde(default)]
        deployment_generation: u64,
    }
    #[derive(Deserialize)]
    struct Deployment {
        deployment_generation: u64,
    }
    #[derive(Deserialize)]
    struct Membership {
        cluster_id: proto::ClusterId,
    }
    #[derive(Deserialize)]
    struct Requirement {
        incarnation: u64,
    }
    #[derive(Deserialize)]
    struct Plan {
        replica_owner: Option<Deployment>,
    }
    #[derive(Deserialize)]
    struct Comment {
        object: proto::CommentObject,
    }

    fn decode<T: serde::de::DeserializeOwned>(
        row: &StateUpdateKindJson,
    ) -> Result<T, CatalogError> {
        row.try_to_serde().map_err(|error| {
            CatalogError::from(DurableCatalogError::NotWritable(format!(
                "cannot retire deployment realizations: {error}"
            )))
        })
    }

    let mut retired_clients = BTreeSet::new();
    let mut retired_identities = BTreeSet::new();
    let mut surviving_replicas = BTreeSet::new();
    let mut rows = Vec::new();
    for row in snapshot.clone() {
        let retire = match row.kind() {
            "ClientIncarnation" => {
                let key = decode::<Key<proto::ClientIncarnationKey>>(row)?.key;
                let value = decode::<Value<ClientGeneration>>(row)?.value;
                if value.deployment_generation < generation {
                    retired_clients.insert(key.id);
                    true
                } else {
                    false
                }
            }
            "ClusterReplica" => {
                let key = decode::<Key<proto::ClusterReplicaKey>>(row)?.key;
                let value = decode::<Value<Membership>>(row)?.value;
                if key.deployment_generation < generation {
                    retired_identities.insert(proto::ClusterReplicaId {
                        cluster_id: value.cluster_id,
                        replica_id: key.id,
                    });
                    true
                } else {
                    surviving_replicas.insert(key.id);
                    false
                }
            }
            "ClusterReplicaDeclaration" => {
                let key = decode::<Key<proto::ClusterReplicaDeclarationKey>>(row)?.key;
                surviving_replicas.insert(key.id);
                false
            }
            "ClusterRuntime" => {
                decode::<Key<Deployment>>(row)?.key.deployment_generation < generation
            }
            _ => false,
        };
        if retire {
            rows.push(row.clone());
        }
    }
    for row in snapshot {
        let retire = match row.kind() {
            "ClientReadRequirement" => {
                retired_clients.contains(&decode::<Key<Requirement>>(row)?.key.incarnation)
            }
            "ReplicaSystemConfiguration" => {
                decode::<Key<Deployment>>(row)?.key.deployment_generation < generation
            }
            "WrittenPlan" => decode::<Value<Plan>>(row)?
                .value
                .replica_owner
                .is_some_and(|owner| owner.deployment_generation < generation),
            "Comment" => match decode::<Key<Comment>>(row)?.key.object {
                proto::CommentObject::ClusterReplica(id) => {
                    retired_identities.contains(&id) && !surviving_replicas.contains(&id.replica_id)
                }
                _ => false,
            },
            _ => false,
        };
        if retire {
            rows.push(row.clone());
        }
    }
    Ok(rows)
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
    use serde_json::json;

    fn retirement_row(
        kind: &str,
        key: serde_json::Value,
        value: serde_json::Value,
    ) -> StateUpdateKindJson {
        StateUpdateKindJson::from_serde(json!({
            "kind": kind, "key": key, "value": value,
            "unknown_non_policy_field": {"preserve": [1, 2, 3]}
        }))
    }

    #[mz_ore::test]
    fn retirement_preserves_other_owners_and_exact_raw_rows() {
        let mut snapshot = Vec::new();
        let mut expected = BTreeSet::new();
        for generation in [0, 2, 3] {
            let owner = json!({"replica_id": {"User": 1}, "deployment_generation": generation});
            let client_value = if generation == 0 {
                // Zero is omitted by the durable ClientIncarnation encoding.
                json!({"extra": true})
            } else {
                json!({"deployment_generation": generation, "extra": true})
            };
            let mut rows = vec![
                retirement_row("ClientIncarnation", json!({"id": generation}), client_value),
                retirement_row(
                    "ClientReadRequirement",
                    json!({"incarnation": generation}),
                    json!({"frontier": "opaque pre-migration value"}),
                ),
                retirement_row(
                    "ClusterReplica",
                    json!({"id": {"User": 1}, "deployment_generation": generation}),
                    json!({"cluster_id": {"User": 1}, "config": "opaque config"}),
                ),
                retirement_row(
                    "ClusterRuntime",
                    json!({"cluster_id": {"User": 1}, "deployment_generation": generation}),
                    json!({"realized_config": "opaque runtime"}),
                ),
                retirement_row(
                    "ReplicaSystemConfiguration",
                    owner.clone(),
                    json!({"value": {"opaque": true}}),
                ),
            ];
            for build in ["old-build", "new-build"] {
                rows.push(retirement_row(
                    "WrittenPlan",
                    json!({"id": {"Transient": generation}, "build_version": build}),
                    json!({"replica_owner": owner, "plan": "opaque plan",
                        "deployment_generation": 99, "extra": true}),
                ));
            }
            if generation < 2 {
                expected.extend(rows.iter().cloned());
            }
            snapshot.extend(rows);
        }
        // Unowned SQL selections, maintained protection, and historical records
        // have no realization lifetime, even if they mention an old generation.
        for value in [
            json!({}),
            json!({"replica_owner": null, "deployment_generation": 0}),
        ] {
            snapshot.push(retirement_row(
                "WrittenPlan",
                json!({"build_version": "old"}),
                value,
            ));
        }
        for kind in [
            "MaintainedReadRequirement",
            "AuditLog",
            "StorageUsage",
            "Cluster",
            "DeploymentAdmission",
        ] {
            snapshot.push(retirement_row(
                kind,
                json!({}),
                json!({"deployment_generation": 0}),
            ));
        }
        snapshot.push(retirement_row(
            "ClientReadRequirement",
            json!({"incarnation": 99}),
            json!({}),
        ));
        // Qualified metadata still belongs to the retired deployment even if
        // its membership has already been removed.
        for owner in [
            json!({"replica_id": {"User": 99}, "deployment_generation": 0}),
            json!({"replica_id": {"User": 1}, "deployment_generation": 1}),
        ] {
            let config = retirement_row("ReplicaSystemConfiguration", owner.clone(), json!({}));
            let plan = retirement_row("WrittenPlan", json!({}), json!({"replica_owner": owner}));
            expected.extend([config.clone(), plan.clone()]);
            snapshot.extend([config, plan]);
        }
        // Dependents need not follow their owners in the consolidated snapshot.
        snapshot.reverse();
        let retired = retired_realizations(snapshot.iter(), 2).expect("decode ownership only");
        assert_eq!(retired.len(), expected.len());
        assert_eq!(retired.into_iter().collect::<BTreeSet<_>>(), expected);
    }

    #[mz_ore::test]
    fn retirement_comments_follow_last_membership_and_declaration() {
        let mut snapshot = Vec::new();
        let mut expected = BTreeSet::new();
        // 1 loses its last memberships, 2 has a declaration, 3 is warmed,
        // 4 has a future membership, and 5 has only orphaned comments.
        for replica in 1..=5 {
            for generation in 0..=1 {
                if replica == 5 {
                    continue;
                }
                let row = retirement_row(
                    "ClusterReplica",
                    json!({"id": {"User": replica}, "deployment_generation": generation}),
                    json!({"cluster_id": {"User": 1}}),
                );
                expected.insert(row.clone());
                snapshot.push(row);
            }
            for cluster in 1..=2 {
                let row = retirement_row(
                    "Comment",
                    json!({"object": {"ClusterReplica": {
                        "cluster_id": {"User": cluster}, "replica_id": {"User": replica}
                    }}}),
                    json!({"comment": "keep exact annotation"}),
                );
                if replica == 1 && cluster == 1 {
                    expected.insert(row.clone());
                }
                snapshot.push(row);
            }
        }
        snapshot.push(retirement_row(
            "ClusterReplicaDeclaration",
            json!({"id": {"User": 2}}),
            json!({"config": "shared declaration"}),
        ));
        for (replica, generation) in [(3, 2), (4, 3)] {
            snapshot.push(retirement_row(
                "ClusterReplica",
                json!({"id": {"User": replica}, "deployment_generation": generation}),
                json!({"cluster_id": {"User": 1}}),
            ));
        }
        snapshot.push(retirement_row(
            "Comment",
            json!({"object": {"Table": {"User": 1}}}),
            json!({}),
        ));
        let retired = retired_realizations(snapshot.iter(), 2).expect("decode stable identities");
        assert_eq!(retired.len(), expected.len());
        assert_eq!(retired.into_iter().collect::<BTreeSet<_>>(), expected);
    }

    #[mz_ore::test]
    fn retirement_rejects_malformed_ownership() {
        let malformed = [
            retirement_row(
                "ClientIncarnation",
                json!({"id": 1}),
                json!({"deployment_generation": "bad"}),
            ),
            retirement_row("ClientIncarnation", json!({"id": "bad"}), json!({})),
            retirement_row(
                "ClientReadRequirement",
                json!({"incarnation": "bad"}),
                json!({}),
            ),
            retirement_row(
                "ClusterReplica",
                json!({"id": {"User": 1}}),
                json!({"cluster_id": {"User": 1}}),
            ),
            retirement_row(
                "ClusterReplica",
                json!({"id": {"User": 1}, "deployment_generation": 0}),
                json!({"cluster_id": "bad"}),
            ),
            retirement_row("ClusterReplicaDeclaration", json!({"id": "bad"}), json!({})),
            retirement_row(
                "ClusterRuntime",
                json!({"deployment_generation": null}),
                json!({}),
            ),
            retirement_row(
                "ReplicaSystemConfiguration",
                json!({"replica_id": {"User": 1}}),
                json!({}),
            ),
            retirement_row(
                "WrittenPlan",
                json!({}),
                json!({"replica_owner": {"replica_id": {"User": 1}}}),
            ),
            retirement_row("WrittenPlan", json!({}), json!({"replica_owner": "bad"})),
            retirement_row(
                "Comment",
                json!({"object": {"ClusterReplica": {"replica_id": {"User": 1}}}}),
                json!({}),
            ),
        ];
        for row in malformed {
            assert!(
                retired_realizations([&row].into_iter(), 2).is_err(),
                "{row:?}"
            );
        }
    }

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
