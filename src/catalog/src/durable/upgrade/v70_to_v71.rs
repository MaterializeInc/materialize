// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::durable::upgrade::objects_v71::Empty;

use crate::durable::upgrade::MigrationAction;
use crate::durable::upgrade::{objects_v70 as v70, objects_v71 as v71};

const DEFAULT_USER_NETWORK_POLICY_NAME: &str = "default";
const NETWORK_POLICIES_DEFAULT_POLICY_OID: u32 = 17048;

const MZ_SYSTEM_ROLE_ID: u64 = 1;
const MZ_SUPPORT_ROLE_ID: u64 = 2;

pub fn upgrade(
    _snapshot: Vec<v70::StateUpdateKind>,
) -> Vec<MigrationAction<v70::StateUpdateKind, v71::StateUpdateKind>> {
    let policy = v71::state_update_kind::NetworkPolicy {
        key: Some(v71::NetworkPolicyKey {
            id: Some(v71::NetworkPolicyId {
                value: Some(v71::network_policy_id::Value::User(1)),
            }),
        }),
        value: Some(v71::NetworkPolicyValue {
            name: DEFAULT_USER_NETWORK_POLICY_NAME.to_string(),
            rules: vec![v71::NetworkPolicyRule {
                name: "open_ingress".to_string(),
                action: Some(v71::network_policy_rule::Action::Allow(Empty {})),
                direction: Some(v71::network_policy_rule::Direction::Ingress(Empty {})),
                address: "0.0.0.0/0".to_string(),
            }],
            owner_id: Some(v71::RoleId {
                value: Some(v71::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
            }),
            privileges: vec![
                v71::MzAclItem {
                    grantee: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::Public(Empty {})),
                    }),
                    grantor: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage
                    acl_mode: Some(v71::AclMode { bitflags: 256 }),
                },
                v71::MzAclItem {
                    grantee: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::System(MZ_SUPPORT_ROLE_ID)),
                    }),
                    grantor: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage
                    acl_mode: Some(v71::AclMode { bitflags: 256 }),
                },
                v71::MzAclItem {
                    grantee: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    grantor: Some(v71::RoleId {
                        value: Some(v71::role_id::Value::System(MZ_SYSTEM_ROLE_ID)),
                    }),
                    // usage_create
                    acl_mode: Some(v71::AclMode { bitflags: 768 }),
                },
            ],
            oid: NETWORK_POLICIES_DEFAULT_POLICY_OID,
        }),
    };

    vec![MigrationAction::Insert(v71::StateUpdateKind {
        kind: Some(v71::state_update_kind::Kind::NetworkPolicy(policy)),
    })]
}
