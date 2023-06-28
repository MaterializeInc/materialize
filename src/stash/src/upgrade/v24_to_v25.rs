// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::upgrade::MigrationAction;
use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v24 {
    include!(concat!(env!("OUT_DIR"), "/objects_v24.rs"));
}

pub mod objects_v25 {
    include!(concat!(env!("OUT_DIR"), "/objects_v25.rs"));
}

/// Remove role attributes.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const ROLES_COLLECTION: TypedCollection<objects_v24::RoleKey, objects_v24::RoleValue> =
        TypedCollection::new("role");

    ROLES_COLLECTION
        .migrate_to::<objects_v25::RoleKey, objects_v25::RoleValue>(tx, |entries| {
            entries
                .into_iter()
                .map(|(key, value)| {
                    MigrationAction::Update(key.clone(), (key.clone().into(), value.clone().into()))
                })
                .collect()
        })
        .await
}

impl From<objects_v24::RoleId> for objects_v25::RoleId {
    fn from(id: objects_v24::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v24::role_id::Value::User(id)) => {
                Some(objects_v25::role_id::Value::User(id))
            }
            Some(objects_v24::role_id::Value::System(id)) => {
                Some(objects_v25::role_id::Value::System(id))
            }
            Some(objects_v24::role_id::Value::Public(_)) => {
                Some(objects_v25::role_id::Value::Public(Default::default()))
            }
        };
        objects_v25::RoleId { value }
    }
}

impl From<objects_v24::RoleAttributes> for objects_v25::RoleAttributes {
    fn from(attributes: objects_v24::RoleAttributes) -> Self {
        objects_v25::RoleAttributes {
            inherit: attributes.inherit,
        }
    }
}

impl From<objects_v24::role_membership::Entry> for objects_v25::role_membership::Entry {
    fn from(entry: objects_v24::role_membership::Entry) -> Self {
        objects_v25::role_membership::Entry {
            key: entry.key.map(Into::into),
            value: entry.value.map(Into::into),
        }
    }
}

impl From<objects_v24::RoleMembership> for objects_v25::RoleMembership {
    fn from(attributes: objects_v24::RoleMembership) -> Self {
        objects_v25::RoleMembership {
            map: attributes
                .map
                .into_iter()
                .map(|entry| entry.into())
                .collect(),
        }
    }
}

impl From<objects_v24::RoleKey> for objects_v25::RoleKey {
    fn from(key: objects_v24::RoleKey) -> Self {
        objects_v25::RoleKey {
            id: key.id.map(Into::into),
        }
    }
}

impl From<objects_v24::RoleValue> for objects_v25::RoleValue {
    fn from(value: objects_v24::RoleValue) -> Self {
        objects_v25::RoleValue {
            name: value.name,
            attributes: value.attributes.map(Into::into),
            membership: value.membership.map(Into::into),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v24_to_v25::{objects_v24, objects_v25};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const ROLE_ID: objects_v24::RoleId = objects_v24::RoleId {
            value: Some(objects_v24::role_id::Value::User(1)),
        };
        const GROUP_ROLE_ID: objects_v24::RoleId = objects_v24::RoleId {
            value: Some(objects_v24::role_id::Value::User(2)),
        };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a system privilege.
        let roles_v24: TypedCollection<objects_v24::RoleKey, objects_v24::RoleValue> =
            TypedCollection::new("role");
        roles_v24
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v24::RoleKey { id: Some(ROLE_ID) },
                    objects_v24::RoleValue {
                        name: "Joe".to_string(),
                        attributes: Some(objects_v24::RoleAttributes {
                            inherit: true,
                            create_role: false,
                            create_db: true,
                            create_cluster: false,
                        }),
                        membership: Some(objects_v24::RoleMembership {
                            map: vec![objects_v24::role_membership::Entry {
                                key: Some(ROLE_ID),
                                value: Some(GROUP_ROLE_ID),
                            }],
                        }),
                    },
                )],
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

        // Read back the items.
        let roles: TypedCollection<objects_v25::RoleKey, objects_v25::RoleValue> =
            TypedCollection::new("role");
        let roles = roles.iter(&mut stash).await.unwrap();
        // Filter down to just the key and value to make comparisons easier.
        let roles: Vec<_> = roles
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert_eq!(
            roles,
            vec![(
                objects_v25::RoleKey {
                    id: Some(ROLE_ID.into()),
                },
                objects_v25::RoleValue {
                    name: "Joe".to_string(),
                    attributes: Some(objects_v25::RoleAttributes { inherit: true }),
                    membership: Some(objects_v25::RoleMembership {
                        map: vec![objects_v25::role_membership::Entry {
                            key: Some(ROLE_ID.into()),
                            value: Some(GROUP_ROLE_ID.into()),
                        }]
                    }),
                }
            )]
        );
    }
}
