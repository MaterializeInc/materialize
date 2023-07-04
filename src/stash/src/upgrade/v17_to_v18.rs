// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{StashError, Transaction, TypedCollection};

pub mod objects_v18 {
    include!(concat!(env!("OUT_DIR"), "/objects_v18.rs"));
}

/// Migration that adds USAGE privileges on all types to the PUBLIC role in the default privileges
/// cluster.
///
/// Author - jkosh44
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const DEFAULT_PRIVILEGES_COLLECTION: TypedCollection<
        objects_v18::DefaultPrivilegesKey,
        objects_v18::DefaultPrivilegesValue,
    > = TypedCollection::new("default_privileges");
    const ACL_MODE_USAGE: objects_v18::AclMode = objects_v18::AclMode { bitflags: 1 << 8 };

    DEFAULT_PRIVILEGES_COLLECTION
        .initialize(
            tx,
            vec![(
                objects_v18::DefaultPrivilegesKey {
                    role_id: Some(objects_v18::RoleId {
                        value: Some(objects_v18::role_id::Value::Public(Default::default())),
                    }),
                    database_id: None,
                    schema_id: None,
                    object_type: objects_v18::ObjectType::Type.into(),
                    grantee: Some(objects_v18::RoleId {
                        value: Some(objects_v18::role_id::Value::Public(Default::default())),
                    }),
                },
                objects_v18::DefaultPrivilegesValue {
                    privileges: Some(ACL_MODE_USAGE),
                },
            )],
        )
        .await
}
