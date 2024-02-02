// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_stash::upgrade::{wire_compatible, MigrationAction, WireCompatible};
use mz_stash::{Transaction, TypedCollection};
use mz_stash_types::StashError;

use crate::durable::upgrade::{objects_v45 as v45, objects_v46 as v46};

wire_compatible!(v45::ClusterConfig with v46::ClusterConfig);
wire_compatible!(v45::ClusterId with v46::ClusterId);
wire_compatible!(v45::ClusterKey with v46::ClusterKey);
wire_compatible!(v45::MzAclItem with v46::MzAclItem);
wire_compatible!(v45::RoleId with v46::RoleId);

const CLUSTER_COLLECTION: TypedCollection<v45::ClusterKey, v45::ClusterValue> =
    TypedCollection::new("clusters");

/// Remove `ClusterValue`'s `linked_object_id` field.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    CLUSTER_COLLECTION
        .migrate_to::<v46::ClusterKey, v46::ClusterValue>(tx, |entries| {
            entries
                .iter()
                .map(
                    |(
                        cluster_key,
                        v45::ClusterValue {
                            name,
                            // Drop `linked_object_id` values.
                            linked_object_id: _,
                            owner_id,
                            privileges,
                            config,
                        },
                    )| {
                        MigrationAction::Update(
                            cluster_key.clone(),
                            (
                                WireCompatible::convert(cluster_key),
                                v46::ClusterValue {
                                    name: name.clone(),
                                    owner_id: owner_id.as_ref().map(WireCompatible::convert),
                                    privileges: privileges
                                        .iter()
                                        .map(WireCompatible::convert)
                                        .collect(),
                                    config: config.as_ref().map(WireCompatible::convert),
                                },
                            ),
                        )
                    },
                )
                .collect()
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_stash::Stash;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_migration() {
        const CLUSTER_COLLECTION_V45: TypedCollection<v45::ClusterKey, v45::ClusterValue> =
            TypedCollection::new("clusters");

        const CLUSTER_COLLECTION_V46: TypedCollection<v46::ClusterKey, v46::ClusterValue> =
            TypedCollection::new("clusters");

        let cluster_key_v45 = v45::ClusterKey {
            id: Some(v45::ClusterId {
                value: Some(v45::cluster_id::Value::User(Default::default())),
            }),
        };

        let cluster_value_v45 = v45::ClusterValue {
            linked_object_id: None,
            name: Default::default(),
            owner_id: Some(v45::RoleId {
                value: Some(v45::role_id::Value::Public(Default::default())),
            }),
            privileges: vec![],
            config: Some(v45::ClusterConfig {
                variant: Some(v45::cluster_config::Variant::Unmanaged(v45::Empty {})),
            }),
        };

        let cluster_key_v46 = v46::ClusterKey {
            id: Some(v46::ClusterId {
                value: Some(v46::cluster_id::Value::User(Default::default())),
            }),
        };

        let cluster_value_v46 = v46::ClusterValue {
            name: Default::default(),
            owner_id: Some(v46::RoleId {
                value: Some(v46::role_id::Value::Public(Default::default())),
            }),
            privileges: vec![],
            config: Some(v46::ClusterConfig {
                variant: Some(v46::cluster_config::Variant::Unmanaged(v46::Empty {})),
            }),
        };

        Stash::with_debug_stash(|mut stash: Stash| async move {
            CLUSTER_COLLECTION_V45
                .insert_without_overwrite(
                    &mut stash,
                    [(cluster_key_v45.clone(), cluster_value_v45.clone())],
                )
                .await
                .expect("insert success");

            // Run the migration.
            stash
                .with_transaction(|tx| {
                    Box::pin(async move {
                        upgrade(&tx).await?;
                        Ok(())
                    })
                })
                .await
                .expect("transaction failed");

            let cluster_v46: Vec<_> = CLUSTER_COLLECTION_V46
                .peek_one(&mut stash)
                .await
                .expect("read v46")
                .into_iter()
                .collect();
            assert_eq!(
                cluster_v46.as_slice(),
                [(cluster_key_v46, cluster_value_v46,)]
            );
        })
        .await
        .expect("stash failed");
    }
}
