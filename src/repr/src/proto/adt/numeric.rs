// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::numeric`].

use crate::adt::numeric::NumericMaxScale;
use crate::proto::{ProtoRepr, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/adt.numeric.rs"));

impl From<&NumericMaxScale> for ProtoNumericMaxScale {
    fn from(max_scale: &NumericMaxScale) -> Self {
        ProtoNumericMaxScale {
            value: max_scale.0.into_proto(),
        }
    }
}

impl TryFrom<ProtoNumericMaxScale> for NumericMaxScale {
    type Error = TryFromProtoError;

    fn try_from(max_scale: ProtoNumericMaxScale) -> Result<Self, Self::Error> {
        Ok(NumericMaxScale(u8::from_proto(max_scale.value)?))
    }
}

impl From<&Option<NumericMaxScale>> for ProtoOptionalNumericMaxScale {
    fn from(max_scale: &Option<NumericMaxScale>) -> Self {
        ProtoOptionalNumericMaxScale {
            value: max_scale.as_ref().map(From::from),
        }
    }
}

impl TryFrom<ProtoOptionalNumericMaxScale> for Option<NumericMaxScale> {
    type Error = TryFromProtoError;

    fn try_from(max_scale: ProtoOptionalNumericMaxScale) -> Result<Self, Self::Error> {
        match max_scale.value {
            Some(value) => Ok(Some(value.try_into()?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn numeric_max_scale_protobuf_roundtrip(expect in any::<NumericMaxScale>()) {
            let actual = protobuf_roundtrip::<_, ProtoNumericMaxScale>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn optional_numeric_max_scale_protobuf_roundtrip(expect in any::<Option<NumericMaxScale>>()) {
            let actual = protobuf_roundtrip::<_, ProtoOptionalNumericMaxScale>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
