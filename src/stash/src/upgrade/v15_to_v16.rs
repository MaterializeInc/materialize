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

pub mod objects_v15 {
    include!(concat!(env!("OUT_DIR"), "/objects_v15.rs"));
}
pub mod objects_v16 {
    include!(concat!(env!("OUT_DIR"), "/objects_v16.rs"));
}

/// Migration that grants the mz_introspection user USAGE privileges for the mz_system cluster.
///
/// Author - jkosh44 (ported by parkmycar)
pub async fn upgrade(tx: &'_ mut Transaction<'_>) -> Result<(), StashError> {
    const CLUSTERS_COLLECTION: TypedCollection<objects_v15::ClusterKey, objects_v15::ClusterValue> =
        TypedCollection::new("compute_instance");

    const MZ_SYSTEM_CLUSTER_NAME: &str = "mz_system";
    const MZ_SYSTEM_ROLE_ID: objects_v15::RoleId = objects_v15::RoleId {
        value: Some(objects_v15::role_id::Value::System(1)),
    };
    const MZ_INTROSPECTION_ROLE_ID: objects_v15::RoleId = objects_v15::RoleId {
        value: Some(objects_v15::role_id::Value::System(2)),
    };
    const ACL_MODE_USAGE: objects_v15::AclMode = objects_v15::AclMode { bitflags: 1 << 8 };
    const MZ_INTROSPECTION_PRIVILEGE: objects_v15::MzAclItem = objects_v15::MzAclItem {
        grantee: Some(MZ_INTROSPECTION_ROLE_ID),
        grantor: Some(MZ_SYSTEM_ROLE_ID),
        acl_mode: Some(ACL_MODE_USAGE),
    };

    CLUSTERS_COLLECTION
        .migrate_to(tx, |entries| {
            let mut updates = vec![];

            // Only migrate the system cluster, if the privileges don't already have an
            // entry for the mz_introspection user.
            for (cluster_key, cluster_value) in entries {
                if cluster_value.name == MZ_SYSTEM_CLUSTER_NAME
                    && !cluster_value
                        .privileges
                        .contains(&MZ_INTROSPECTION_PRIVILEGE)
                {
                    let v15_key = cluster_key.clone();
                    let v15_val = cluster_value.clone();

                    // The migration! Here we're adding the MZ_INTROSPECTION_PRIVILEGE
                    // to the existing privileges.
                    let privileges = v15_val
                        .privileges
                        .into_iter()
                        .chain(std::iter::once(MZ_INTROSPECTION_PRIVILEGE))
                        .map(|p| p.into())
                        .collect();

                    let v16_key = objects_v16::ClusterKey {
                        id: v15_key.id.map(|id| id.into()),
                    };
                    let v16_val = objects_v16::ClusterValue {
                        name: v15_val.name,
                        linked_object_id: v15_val.linked_object_id.map(|o| o.into()),
                        owner_id: v15_val.owner_id.map(|o| o.into()),
                        privileges,
                    };

                    updates.push(MigrationAction::Update(
                        cluster_key.clone(),
                        (v16_key, v16_val),
                    ));
                }
            }

            updates
        })
        .await?;

    Ok(())
}

impl From<objects_v15::ClusterId> for objects_v16::ClusterId {
    fn from(v15: objects_v15::ClusterId) -> Self {
        let value = match v15.value {
            Some(objects_v15::cluster_id::Value::User(id)) => {
                Some(objects_v16::cluster_id::Value::User(id))
            }
            Some(objects_v15::cluster_id::Value::System(id)) => {
                Some(objects_v16::cluster_id::Value::System(id))
            }
            None => None,
        };
        objects_v16::ClusterId { value }
    }
}

impl From<objects_v15::GlobalId> for objects_v16::GlobalId {
    fn from(v15: objects_v15::GlobalId) -> Self {
        let value = match v15.value {
            Some(objects_v15::global_id::Value::User(id)) => {
                Some(objects_v16::global_id::Value::User(id))
            }
            Some(objects_v15::global_id::Value::System(id)) => {
                Some(objects_v16::global_id::Value::System(id))
            }
            Some(objects_v15::global_id::Value::Transient(id)) => {
                Some(objects_v16::global_id::Value::Transient(id))
            }
            Some(objects_v15::global_id::Value::Explain(_)) => {
                Some(objects_v16::global_id::Value::Explain(Default::default()))
            }
            None => None,
        };
        objects_v16::GlobalId { value }
    }
}

impl From<objects_v15::RoleId> for objects_v16::RoleId {
    fn from(v15: objects_v15::RoleId) -> Self {
        let value = match v15.value {
            Some(objects_v15::role_id::Value::User(id)) => {
                Some(objects_v16::role_id::Value::User(id))
            }
            Some(objects_v15::role_id::Value::System(id)) => {
                Some(objects_v16::role_id::Value::System(id))
            }
            Some(objects_v15::role_id::Value::Public(_)) => {
                Some(objects_v16::role_id::Value::Public(Default::default()))
            }
            None => None,
        };
        objects_v16::RoleId { value }
    }
}

impl From<objects_v15::MzAclItem> for objects_v16::MzAclItem {
    fn from(v15: objects_v15::MzAclItem) -> Self {
        objects_v16::MzAclItem {
            grantee: v15.grantee.map(|g| g.into()),
            grantor: v15.grantor.map(|g| g.into()),
            acl_mode: v15.acl_mode.map(|mode| objects_v16::AclMode {
                bitflags: mode.bitflags,
            }),
        }
    }
}
