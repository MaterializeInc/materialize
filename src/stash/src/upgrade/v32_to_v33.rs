// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::wire_compatible;
use crate::upgrade::MigrationAction;
use crate::upgrade::{objects_v32, objects_v33};
use crate::{StashError, Transaction, TypedCollection};

wire_compatible!(objects_v32::RoleKey with objects_v33::RoleKey);
wire_compatible!(objects_v32::RoleValue with objects_v33::RoleValue);

const MZ_INTROSPECTION_ROLE_ID: objects_v33::RoleId = objects_v33::RoleId {
    value: Some(objects_v33::role_id::Value::System(2)),
};

/// Rename mz_introspection to mz_support
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const ROLES_COLLECTION: TypedCollection<objects_v32::RoleKey, objects_v32::RoleValue> =
        TypedCollection::new("role");

    let old_role_key = objects_v33::RoleKey {
        id: Some(MZ_INTROSPECTION_ROLE_ID),
    };

    ROLES_COLLECTION
        .migrate_compat::<objects_v33::RoleKey, objects_v33::RoleValue>(tx, |entries| {
            let mut updates = Vec::new();
            for (key, value) in entries {
                let new_value = if key == &old_role_key {
                    objects_v33::RoleValue {
                        name: "mz_support".to_string(),
                        ..value.clone()
                    }
                } else {
                    value.clone()
                };
                updates.push(MigrationAction::Update(
                    key.clone(),
                    (key.clone(), new_value),
                ));
            }
            updates
        })
        .await
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::upgrade;

    use crate::upgrade::v32_to_v33::{objects_v32, objects_v33, MZ_INTROSPECTION_ROLE_ID};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory: DebugStashFactory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        let roles_v32: TypedCollection<objects_v32::RoleKey, objects_v32::RoleValue> =
            TypedCollection::new("role");

        let system_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(1)),
        };
        let introspection_role_id_v32 = objects_v32::RoleId {
            value: Some(objects_v32::role_id::Value::System(2)),
        };

        roles_v32
            .insert_without_overwrite(
                &mut stash,
                vec![
                    (
                        objects_v32::RoleKey {
                            id: Some(system_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_system".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                    (
                        objects_v32::RoleKey {
                            id: Some(introspection_role_id_v32),
                        },
                        objects_v32::RoleValue {
                            name: "mz_introspection".to_string(),
                            attributes: Some(objects_v32::RoleAttributes { inherit: true }),
                            membership: Some(objects_v32::RoleMembership {
                                map: Default::default(),
                            }),
                        },
                    ),
                ],
            )
            .await
            .unwrap();

        // Run our migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .expect("migration failed");

        let roles_v33: TypedCollection<objects_v33::RoleKey, objects_v33::RoleValue> =
            TypedCollection::new("role");
        let roles = roles_v33.iter(&mut stash).await.unwrap();

        let system_role_id_v33 = objects_v33::RoleId {
            value: Some(objects_v33::role_id::Value::System(1)),
        };
        let mut roles = roles
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect_vec();
        roles.sort();
        assert_eq!(
            roles,
            vec![
                (
                    objects_v33::RoleKey {
                        id: Some(system_role_id_v33),
                    },
                    objects_v33::RoleValue {
                        name: "mz_system".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                ),
                (
                    objects_v33::RoleKey {
                        id: Some(MZ_INTROSPECTION_ROLE_ID),
                    },
                    objects_v33::RoleValue {
                        name: "mz_support".to_string(),
                        attributes: Some(objects_v33::RoleAttributes { inherit: true }),
                        membership: Some(objects_v33::RoleMembership {
                            map: Default::default(),
                        }),
                    },
                )
            ]
        );
    }
}
