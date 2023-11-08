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

use crate::durable::upgrade::{objects_v38 as v38, objects_v39 as v39};

wire_compatible!(v38::RoleKey with v39::RoleKey);
wire_compatible!(v38::RoleAttributes with v39::RoleAttributes);
wire_compatible!(v38::RoleMembership with v39::RoleMembership);

const ROLES_COLLECTION: TypedCollection<v38::RoleKey, v38::RoleValue> =
    TypedCollection::new("role");

/// Update all Roles to contains an empy set of RoleVars.
pub async fn upgrade(tx: &Transaction<'_>) -> Result<(), StashError> {
    ROLES_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::with_capacity(entries.len());

            for (key, value) in entries {
                let new_key: v39::RoleKey = WireCompatible::convert(key);
                let new_value = v39::RoleValue {
                    name: value.name.clone(),
                    attributes: value.attributes.as_ref().map(WireCompatible::convert),
                    membership: value.membership.as_ref().map(WireCompatible::convert),
                    //
                    // ** MIGRATION ** Adding a default RoleVars message to all types.
                    //
                    vars: Some(v39::RoleVars::default()),
                };

                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }

            updates
        })
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Stash;

    const ROLES_COLLECTION_V39: TypedCollection<v39::RoleKey, v39::RoleValue> =
        TypedCollection::new("role");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test_role_vars_added() {
        Stash::with_debug_stash(|mut stash| async move {
            ROLES_COLLECTION
                .insert_without_overwrite(
                    &mut stash,
                    vec![
                        (
                            v38::RoleKey {
                                id: Some(v38::RoleId {
                                    value: Some(v38::role_id::Value::System(1)),
                                }),
                            },
                            v38::RoleValue {
                                name: "mz_system".to_string(),
                                attributes: Some(v38::RoleAttributes { inherit: true }),
                                membership: None,
                            },
                        ),
                        (
                            v38::RoleKey {
                                id: Some(v38::RoleId {
                                    value: Some(v38::role_id::Value::User(42)),
                                }),
                            },
                            v38::RoleValue {
                                name: "parker".to_string(),
                                attributes: None,
                                membership: Some(v38::RoleMembership {
                                    map: Default::default(),
                                }),
                            },
                        ),
                        (
                            v38::RoleKey {
                                id: Some(v38::RoleId {
                                    value: Some(v38::role_id::Value::Public(Default::default())),
                                }),
                            },
                            v38::RoleValue {
                                name: "public".to_string(),
                                attributes: Some(v38::RoleAttributes { inherit: false }),
                                membership: Some(v38::RoleMembership {
                                    map: Default::default(),
                                }),
                            },
                        ),
                    ],
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
                .unwrap();

            let roles = ROLES_COLLECTION_V39
                .peek_one(&mut stash)
                .await
                .expect("read v39");
            insta::assert_debug_snapshot!(roles, @r###"
        {
            RoleKey {
                id: Some(
                    RoleId {
                        value: Some(
                            System(
                                1,
                            ),
                        ),
                    },
                ),
            }: RoleValue {
                name: "mz_system",
                attributes: Some(
                    RoleAttributes {
                        inherit: true,
                    },
                ),
                membership: None,
                vars: Some(
                    RoleVars {
                        entries: [],
                    },
                ),
            },
            RoleKey {
                id: Some(
                    RoleId {
                        value: Some(
                            User(
                                42,
                            ),
                        ),
                    },
                ),
            }: RoleValue {
                name: "parker",
                attributes: None,
                membership: Some(
                    RoleMembership {
                        map: [],
                    },
                ),
                vars: Some(
                    RoleVars {
                        entries: [],
                    },
                ),
            },
            RoleKey {
                id: Some(
                    RoleId {
                        value: Some(
                            Public(
                                Empty,
                            ),
                        ),
                    },
                ),
            }: RoleValue {
                name: "public",
                attributes: Some(
                    RoleAttributes {
                        inherit: false,
                    },
                ),
                membership: Some(
                    RoleMembership {
                        map: [],
                    },
                ),
                vars: Some(
                    RoleVars {
                        entries: [],
                    },
                ),
            },
        }
        "###);
        })
        .await
        .unwrap();
    }
}
