// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_pgrepr::oid::NETWORK_POLICIES_DEFAULT_POLICY_OID;

use crate::durable::upgrade::objects_v70::Empty;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v69 as v69, objects_v70 as v70};

const DEFAULT_USER_NETWORK_POLICY_NAME: &str = "default";

const MZ_SYSTEM_ROLE_ID: u64 = 1;
const MZ_SUPPORT_ROLE_ID: u64 = 2;

/// In v69, we add the Network Policy resources.
pub fn upgrade(
    _snapshot: Vec<v69::StateUpdateKind>,
) -> Vec<MigrationAction<v69::StateUpdateKind, v70::StateUpdateKind>> {
    let policy = v70::state_update_kind::NetworkPolicy {
        key: Some(v70::NetworkPolicyKey {
            id: Some(v70::NetworkPolicyId {
                value: Some(v70::network_policy_id::Value::User(1)),
            }),
        }),
        value: Some(v70::NetworkPolicyValue {
            name: DEFAULT_USER_NETWORK_POLICY_NAME.to_string(),
            rules: vec![v70::NetworkPolicyRule {
                name: "open_ingress".to_string(),
                action: Some(v70::network_policy_rule::Action::Allow(Empty {})),
                direction: Some(v70::network_policy_rule::Direction::Ingress(Empty {})),
                address: "0.0.0.0/0".to_string(),
            }],
            owner_id: Some(v70::RoleId {
                value: Some(v70::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
            }),
            privileges: vec![
                v70::MzAclItem {
                    grantee: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::Public(Empty {})),
                    }),
                    grantor: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage
                    acl_mode: Some(v70::AclMode { bitflags: 256 }),
                },
                v70::MzAclItem {
                    grantee: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::System(MZ_SUPPORT_ROLE_ID)),
                    }),
                    grantor: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage
                    acl_mode: Some(v70::AclMode { bitflags: 256 }),
                },
                v70::MzAclItem {
                    grantee: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    grantor: Some(v70::RoleId {
                        value: Some(v70::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage_create
                    acl_mode: Some(v70::AclMode { bitflags: 768 }),
                },
            ],
            oid: NETWORK_POLICIES_DEFAULT_POLICY_OID,
        }),
    };

    vec![MigrationAction::Insert(v70::StateUpdateKind {
        kind: Some(v70::state_update_kind::Kind::NetworkPolicy(policy)),
    })]
}
