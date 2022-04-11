// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

include!(concat!(env!("OUT_DIR"), "/scalar.rs"));

use crate::scalar::DomainLimit;
use mz_repr::proto::TryFromProtoError;

impl From<&DomainLimit> for ProtoDomainLimit {
    fn from(limit: &DomainLimit) -> Self {
        use proto_domain_limit::Kind::*;
        let kind = match limit {
            DomainLimit::None => None(()),
            DomainLimit::Inclusive(v) => Inclusive(*v),
            DomainLimit::Exclusive(v) => Exclusive(*v),
        };
        ProtoDomainLimit { kind: Some(kind) }
    }
}

impl TryFrom<ProtoDomainLimit> for DomainLimit {
    type Error = TryFromProtoError;

    fn try_from(limit: ProtoDomainLimit) -> Result<Self, Self::Error> {
        use proto_domain_limit::Kind::*;
        if let Some(kind) = limit.kind {
            match kind {
                None(()) => Ok(DomainLimit::None),
                Inclusive(v) => Ok(DomainLimit::Inclusive(v)),
                Exclusive(v) => Ok(DomainLimit::Exclusive(v)),
            }
        } else {
            Err(TryFromProtoError::missing_field("`ProtoDomainLimit::kind`"))
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
        fn domain_limit_protobuf_roundtrip(expect in any::<DomainLimit>()) {
            let actual = protobuf_roundtrip::<_, ProtoDomainLimit>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
