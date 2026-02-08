// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{RustType, TryFromProtoError};

// Re-export RoleId from mz-catalog-types
pub use mz_catalog_types::RoleId;

include!(concat!(env!("OUT_DIR"), "/mz_repr.role_id.rs"));

impl RustType<ProtoRoleId> for RoleId {
    fn into_proto(&self) -> ProtoRoleId {
        use proto_role_id::Kind::*;
        ProtoRoleId {
            kind: Some(match self {
                RoleId::System(x) => System(*x),
                RoleId::Predefined(x) => Predefined(*x),
                RoleId::User(x) => User(*x),
                RoleId::Public => Public(()),
            }),
        }
    }

    fn from_proto(proto: ProtoRoleId) -> Result<Self, TryFromProtoError> {
        use proto_role_id::Kind::*;
        match proto.kind {
            Some(System(x)) => Ok(RoleId::System(x)),
            Some(Predefined(x)) => Ok(RoleId::Predefined(x)),
            Some(User(x)) => Ok(RoleId::User(x)),
            Some(Public(_)) => Ok(RoleId::Public),
            None => Err(TryFromProtoError::missing_field("ProtoRoleId::kind")),
        }
    }
}

#[mz_ore::test]
fn test_role_id_parsing() {
    let s = "s42";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::System(42), role_id);
    assert_eq!(s, role_id.to_string());

    let s = "g24";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::Predefined(24), role_id);
    assert_eq!(s, role_id.to_string());

    let s = "u666";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::User(666), role_id);
    assert_eq!(s, role_id.to_string());

    let s = "p";
    let role_id: RoleId = s.parse().unwrap();
    assert_eq!(RoleId::Public, role_id);
    assert_eq!(s, role_id.to_string());

    let s = "p23";
    mz_ore::assert_err!(s.parse::<RoleId>());

    let s = "d23";
    mz_ore::assert_err!(s.parse::<RoleId>());

    let s = "asfje90uf23i";
    mz_ore::assert_err!(s.parse::<RoleId>());

    let s = "";
    mz_ore::assert_err!(s.parse::<RoleId>());
}

#[mz_ore::test]
fn test_role_id_binary() {
    let role_id = RoleId::System(42);
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    let role_id = RoleId::Predefined(24);
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    let role_id = RoleId::User(666);
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    let role_id = RoleId::Public;
    assert_eq!(
        role_id,
        RoleId::decode_binary(&role_id.encode_binary()).unwrap()
    );

    mz_ore::assert_err!(RoleId::decode_binary(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]))
}

#[mz_ore::test]
fn test_role_id_binary_size() {
    assert_eq!(9, RoleId::binary_size());
}
