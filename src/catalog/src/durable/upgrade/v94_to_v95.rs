// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::json_compatible::JsonCompatible;
use crate::durable::upgrade::objects_v94 as v94;
use crate::durable::upgrade::objects_v95 as v95;

crate::json_compatible!(v94::ReplicaId with v95::ReplicaId);
crate::json_compatible!(v94::ClusterReplicaValue with v95::ClusterReplicaValue);
crate::json_compatible!(v94::ReplicaSystemConfigurationValue with v95::ReplicaSystemConfigurationValue);

/// Qualifies existing replica keys with the input catalog's deployment generation.
/// All other records remain unchanged.
pub fn upgrade(
    snapshot: Vec<v94::StateUpdateKind>,
) -> Vec<MigrationAction<v94::StateUpdateKind, v95::StateUpdateKind>> {
    // Savepoints retain the stored fence token. Writable opens replace it before
    // migration, so this is the generation of the input catalog, not necessarily
    // the generation that originally created these replicas.
    let mut generations = snapshot.iter().filter_map(|update| match update {
        v94::StateUpdateKind::FenceToken(token) => Some(token.deploy_generation),
        _ => None,
    });
    let deployment_generation = generations.next().expect("catalog must have a fence token");
    assert!(
        generations.next().is_none(),
        "catalog must have one fence token"
    );

    let mut migrations = Vec::new();
    for update in snapshot {
        let new = match &update {
            v94::StateUpdateKind::ClusterReplica(replica) => {
                v95::StateUpdateKind::ClusterReplica(v95::ClusterReplica {
                    key: v95::ClusterReplicaKey {
                        id: JsonCompatible::convert(&replica.key.id),
                        deployment_generation,
                    },
                    value: JsonCompatible::convert(&replica.value),
                })
            }
            v94::StateUpdateKind::ReplicaSystemConfiguration(config) => {
                v95::StateUpdateKind::ReplicaSystemConfiguration(v95::ReplicaSystemConfiguration {
                    key: v95::ReplicaSystemConfigurationKey {
                        replica_id: JsonCompatible::convert(&config.key.replica_id),
                        deployment_generation,
                        name: config.key.name.clone(),
                    },
                    value: JsonCompatible::convert(&config.value),
                })
            }
            _ => continue,
        };
        migrations.push(MigrationAction::Update(update, new));
    }
    migrations
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn fence(deploy_generation: u64) -> v94::StateUpdateKind {
        v94::StateUpdateKind::FenceToken(v94::FenceToken {
            deploy_generation,
            epoch: 17,
        })
    }

    fn assert_key_migration(old: v94::StateUpdateKind, generation: u64) {
        // Compare the whole serialized row, including every value/config field.
        let mut expected = serde_json::to_value(&old).expect("serialize v94 row");
        expected["key"]["deployment_generation"] = generation.into();
        let expected: v95::StateUpdateKind =
            serde_json::from_value(expected).expect("decode expected v95 row");
        assert_eq!(
            upgrade(vec![old.clone(), fence(generation)]),
            vec![MigrationAction::Update(old, expected)],
        );
    }

    proptest! {
        #[mz_ore::test]
        fn qualifies_replica_preserving_id_and_value(
            replica: v94::ClusterReplica,
            generation in 1u64..=u64::MAX,
        ) {
            assert_key_migration(v94::StateUpdateKind::ClusterReplica(replica), generation);
        }

        #[mz_ore::test]
        fn qualifies_replica_configuration_preserving_id_name_and_value(
            config: v94::ReplicaSystemConfiguration,
            generation in 1u64..=u64::MAX,
        ) {
            assert_key_migration(v94::StateUpdateKind::ReplicaSystemConfiguration(config), generation);
        }

        #[mz_ore::test]
        fn leaves_other_rows_unchanged(row: v94::StateUpdateKind) {
            prop_assume!(!matches!(row,
                v94::StateUpdateKind::ClusterReplica(_)
                | v94::StateUpdateKind::ReplicaSystemConfiguration(_)
                | v94::StateUpdateKind::FenceToken(_)
            ));
            prop_assert!(upgrade(vec![fence(42), row]).is_empty());
        }
    }

    #[mz_ore::test]
    #[should_panic(expected = "catalog must have a fence token")]
    fn rejects_missing_generation() {
        upgrade(Vec::new());
    }

    #[mz_ore::test]
    #[should_panic(expected = "catalog must have one fence token")]
    fn rejects_ambiguous_generation() {
        upgrade(vec![fence(42), fence(43)]);
    }
}
