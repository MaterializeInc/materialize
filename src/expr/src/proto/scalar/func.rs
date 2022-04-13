// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::scalar::func`].

use crate::scalar::func::UnmaterializableFunc;
use mz_repr::proto::TryFromProtoError;

include!(concat!(env!("OUT_DIR"), "/scalar.func.rs"));

impl From<&UnmaterializableFunc> for ProtoUnmaterializableFunc {
    fn from(func: &UnmaterializableFunc) -> Self {
        use proto_unmaterializable_func::Kind::*;
        let kind = match func {
            UnmaterializableFunc::CurrentDatabase => CurrentDatabase(()),
            UnmaterializableFunc::CurrentSchemasWithSystem => CurrentSchemasWithSystem(()),
            UnmaterializableFunc::CurrentSchemasWithoutSystem => CurrentSchemasWithoutSystem(()),
            UnmaterializableFunc::CurrentTimestamp => CurrentTimestamp(()),
            UnmaterializableFunc::CurrentUser => CurrentUser(()),
            UnmaterializableFunc::MzClusterId => MzClusterId(()),
            UnmaterializableFunc::MzLogicalTimestamp => MzLogicalTimestamp(()),
            UnmaterializableFunc::MzSessionId => MzSessionId(()),
            UnmaterializableFunc::MzUptime => MzUptime(()),
            UnmaterializableFunc::MzVersion => MzVersion(()),
            UnmaterializableFunc::PgBackendPid => PgBackendPid(()),
            UnmaterializableFunc::PgPostmasterStartTime => PgPostmasterStartTime(()),
            UnmaterializableFunc::Version => Version(()),
        };
        ProtoUnmaterializableFunc { kind: Some(kind) }
    }
}

impl TryFrom<ProtoUnmaterializableFunc> for UnmaterializableFunc {
    type Error = TryFromProtoError;

    fn try_from(func: ProtoUnmaterializableFunc) -> Result<Self, Self::Error> {
        use proto_unmaterializable_func::Kind::*;
        if let Some(kind) = func.kind {
            match kind {
                CurrentDatabase(()) => Ok(UnmaterializableFunc::CurrentDatabase),
                CurrentSchemasWithSystem(()) => Ok(UnmaterializableFunc::CurrentSchemasWithSystem),
                CurrentSchemasWithoutSystem(()) => {
                    Ok(UnmaterializableFunc::CurrentSchemasWithoutSystem)
                }
                CurrentTimestamp(()) => Ok(UnmaterializableFunc::CurrentTimestamp),
                CurrentUser(()) => Ok(UnmaterializableFunc::CurrentUser),
                MzClusterId(()) => Ok(UnmaterializableFunc::MzClusterId),
                MzLogicalTimestamp(()) => Ok(UnmaterializableFunc::MzLogicalTimestamp),
                MzSessionId(()) => Ok(UnmaterializableFunc::MzSessionId),
                MzUptime(()) => Ok(UnmaterializableFunc::MzUptime),
                MzVersion(()) => Ok(UnmaterializableFunc::MzVersion),
                PgBackendPid(()) => Ok(UnmaterializableFunc::PgBackendPid),
                PgPostmasterStartTime(()) => Ok(UnmaterializableFunc::PgPostmasterStartTime),
                Version(()) => Ok(UnmaterializableFunc::Version),
            }
        } else {
            Err(TryFromProtoError::missing_field(
                "`ProtoUnmaterializableFunc::kind`",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn unmaterializable_func_protobuf_roundtrip(expect in any::<UnmaterializableFunc>()) {
            let actual = protobuf_roundtrip::<_, ProtoUnmaterializableFunc>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
