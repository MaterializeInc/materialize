// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v21 {
    include!(concat!(env!("OUT_DIR"), "/objects_v21.rs"));
}

const MZ_SYSTEM_ROLE_ID: objects_v21::RoleId = objects_v21::RoleId {
    value: Some(objects_v21::role_id::Value::System(1)),
};
const ACL_MODE_CREATE_CLUSTER: u64 = 1 << 29;
const ACL_MODE_CREATE_DB: u64 = 1 << 30;
const ACL_MODE_CREATE_ROLE: u64 = 1 << 31;
const MZ_SYSTEM_ACL_MODE: objects_v21::AclMode = objects_v21::AclMode {
    bitflags: ACL_MODE_CREATE_CLUSTER | ACL_MODE_CREATE_DB | ACL_MODE_CREATE_ROLE,
};

/// Migration that adds system privileges.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const SYSTEM_PRIVILEGES_COLLECTION: TypedCollection<objects_v21::SystemPrivilegesKey, ()> =
        TypedCollection::new("system_privileges");

    SYSTEM_PRIVILEGES_COLLECTION
        .initialize(
            tx,
            vec![(
                objects_v21::SystemPrivilegesKey {
                    privileges: Some(objects_v21::MzAclItem {
                        grantee: Some(MZ_SYSTEM_ROLE_ID),
                        grantor: Some(MZ_SYSTEM_ROLE_ID),
                        acl_mode: Some(MZ_SYSTEM_ACL_MODE),
                    }),
                },
                (),
            )],
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::upgrade;

    use super::objects_v21::{self};
    use crate::upgrade::v20_to_v21::{MZ_SYSTEM_ACL_MODE, MZ_SYSTEM_ROLE_ID};
    use crate::{DebugStashFactory, TypedCollection};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        // Connect to the Stash.
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

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
        let system_privileges: TypedCollection<objects_v21::SystemPrivilegesKey, ()> =
            TypedCollection::new("system_privileges");
        let privileges = system_privileges.iter(&mut stash).await.unwrap();
        // Filter down to just the acl item to make comparisons easier.
        let privileges: Vec<_> = privileges
            .into_iter()
            .map(|((key, _value), _timestamp, _diff)| key)
            .collect();

        assert_eq!(
            privileges,
            vec![objects_v21::SystemPrivilegesKey {
                privileges: Some(objects_v21::MzAclItem {
                    grantee: Some(MZ_SYSTEM_ROLE_ID),
                    grantor: Some(MZ_SYSTEM_ROLE_ID),
                    acl_mode: Some(MZ_SYSTEM_ACL_MODE),
                }),
            },]
        );
    }
}
