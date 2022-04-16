// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::proto::TryFromProtoError;
use crate::GlobalId;

include!(concat!(env!("OUT_DIR"), "/global_id.rs"));

impl From<&GlobalId> for ProtoGlobalId {
    fn from(x: &GlobalId) -> Self {
        ProtoGlobalId {
            kind: Some(match x {
                GlobalId::System(x) => proto_global_id::Kind::System(*x),
                GlobalId::User(x) => proto_global_id::Kind::User(*x),
                GlobalId::Transient(x) => proto_global_id::Kind::Transient(*x),
                GlobalId::Explain => proto_global_id::Kind::Explain(()),
            }),
        }
    }
}

impl TryFrom<ProtoGlobalId> for GlobalId {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoGlobalId) -> Result<Self, Self::Error> {
        match x.kind {
            Some(proto_global_id::Kind::System(x)) => Ok(GlobalId::System(x)),
            Some(proto_global_id::Kind::User(x)) => Ok(GlobalId::User(x)),
            Some(proto_global_id::Kind::Transient(x)) => Ok(GlobalId::Transient(x)),
            Some(proto_global_id::Kind::Explain(_)) => Ok(GlobalId::Explain),
            None => Err(TryFromProtoError::missing_field("ProtoGlobalId::kind")),
        }
    }
}
