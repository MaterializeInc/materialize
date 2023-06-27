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

pub mod objects_v23 {
    include!(concat!(env!("OUT_DIR"), "/objects_v23.rs"));
}

pub mod objects_v24 {
    include!(concat!(env!("OUT_DIR"), "/objects_v24.rs"));
}

/// Migrate the format of system privilege for easier look ups.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const SYSTEM_PRIVILEGES_COLLECTION: TypedCollection<objects_v23::SystemPrivilegesKey, ()> =
        TypedCollection::new("system_privileges");

    SYSTEM_PRIVILEGES_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = Vec::new();
            for (key, _value) in entries {
                let grantee = key
                    .privileges
                    .as_ref()
                    .map(|acl_item| acl_item.grantee.clone().map(Into::into))
                    .flatten();
                let grantor = key
                    .privileges
                    .as_ref()
                    .map(|acl_item| acl_item.grantor.clone().map(Into::into))
                    .flatten();
                let acl_mode = key
                    .privileges
                    .as_ref()
                    .map(|acl_item| acl_item.acl_mode.clone().map(Into::into))
                    .flatten();
                let new_key = objects_v24::SystemPrivilegesKey { grantee, grantor };
                let new_value = objects_v24::SystemPrivilegesValue { acl_mode };
                updates.push(MigrationAction::Update(key.clone(), (new_key, new_value)));
            }
            updates
        })
        .await
}

impl From<objects_v23::RoleId> for objects_v24::RoleId {
    fn from(id: objects_v23::RoleId) -> Self {
        let value = match id.value {
            None => None,
            Some(objects_v23::role_id::Value::User(id)) => {
                Some(objects_v24::role_id::Value::User(id))
            }
            Some(objects_v23::role_id::Value::System(id)) => {
                Some(objects_v24::role_id::Value::System(id))
            }
            Some(objects_v23::role_id::Value::Public(_)) => {
                Some(objects_v24::role_id::Value::Public(Default::default()))
            }
        };
        objects_v24::RoleId { value }
    }
}

impl From<objects_v23::AclMode> for objects_v24::AclMode {
    fn from(acl_mode: objects_v23::AclMode) -> Self {
        objects_v24::AclMode {
            bitflags: acl_mode.bitflags,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use crate::upgrade::v23_to_v24::{objects_v23, objects_v24};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        const GRANTEE: objects_v23::RoleId = objects_v23::RoleId {
            value: Some(objects_v23::role_id::Value::User(1)),
        };
        const GRANTOR: objects_v23::RoleId = objects_v23::RoleId {
            value: Some(objects_v23::role_id::Value::System(1)),
        };
        const ACL_MODE: objects_v23::AclMode = objects_v23::AclMode { bitflags: 1 << 30 };

        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        // Insert a system privilege.
        let system_privileges_v23: TypedCollection<objects_v23::SystemPrivilegesKey, ()> =
            TypedCollection::new("system_privileges");
        system_privileges_v23
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    objects_v23::SystemPrivilegesKey {
                        privileges: Some(objects_v23::MzAclItem {
                            grantee: Some(GRANTEE),
                            grantor: Some(GRANTOR),
                            acl_mode: Some(ACL_MODE),
                        }),
                    },
                    (),
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
        let system_privileges: TypedCollection<
            objects_v24::SystemPrivilegesKey,
            objects_v24::SystemPrivilegesValue,
        > = TypedCollection::new("system_privileges");
        let privileges = system_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the key and value to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, value), _timestamp, _diff)| (key, value))
            .collect();

        assert_eq!(
            privileges,
            vec![(
                objects_v24::SystemPrivilegesKey {
                    grantee: Some(GRANTEE.into()),
                    grantor: Some(GRANTOR.into()),
                },
                objects_v24::SystemPrivilegesValue {
                    acl_mode: Some(ACL_MODE.into()),
                }
            )]
        );
    }
}
