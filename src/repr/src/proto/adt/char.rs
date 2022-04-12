// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::char`].

use crate::adt::char::CharLength;
use crate::proto::TryFromProtoError;

include!(concat!(env!("OUT_DIR"), "/adt.char.rs"));

impl From<&CharLength> for ProtoCharLength {
    fn from(x: &CharLength) -> Self {
        ProtoCharLength { value: x.0 }
    }
}

impl TryFrom<ProtoCharLength> for CharLength {
    type Error = TryFromProtoError;

    fn try_from(repr: ProtoCharLength) -> Result<Self, Self::Error> {
        Ok(CharLength(repr.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn char_length_protobuf_roundtrip(expect in any::<CharLength>()) {
            let actual = protobuf_roundtrip::<_, ProtoCharLength>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
