// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::array`].
include!(concat!(env!("OUT_DIR"), "/strconv.rs"));

use super::{ProtoRepr, TryFromProtoError};
use crate::strconv::{ParseError, ParseErrorKind, ParseHexError};

impl From<&ParseError> for ProtoParseError {
    fn from(error: &ParseError) -> Self {
        use proto_parse_error::*;
        use Kind::*;
        let kind = match error.kind {
            ParseErrorKind::OutOfRange => OutOfRange(()),
            ParseErrorKind::InvalidInputSyntax => InvalidInputSyntax(()),
        };
        ProtoParseError {
            kind: Some(kind),
            type_name: error.type_name.clone(),
            input: error.input.clone(),
            details: error.details.clone(),
        }
    }
}

impl TryFrom<ProtoParseError> for ParseError {
    type Error = TryFromProtoError;

    fn try_from(error: ProtoParseError) -> Result<Self, Self::Error> {
        use proto_parse_error::Kind::*;

        if let Some(kind) = error.kind {
            Ok(ParseError {
                kind: match kind {
                    OutOfRange(()) => ParseErrorKind::OutOfRange,
                    InvalidInputSyntax(()) => ParseErrorKind::InvalidInputSyntax,
                },
                type_name: error.type_name,
                input: error.input,
                details: error.details,
            })
        } else {
            Err(TryFromProtoError::missing_field("`ProtoParseError::kind`"))
        }
    }
}

impl From<&ParseHexError> for ProtoParseHexError {
    fn from(error: &ParseHexError) -> Self {
        use proto_parse_hex_error::*;
        use Kind::*;
        let kind = match error {
            ParseHexError::InvalidHexDigit(v) => InvalidHexDigit(v.into_proto()),
            ParseHexError::OddLength => OddLength(()),
        };
        ProtoParseHexError { kind: Some(kind) }
    }
}

impl TryFrom<ProtoParseHexError> for ParseHexError {
    type Error = TryFromProtoError;

    fn try_from(error: ProtoParseHexError) -> Result<Self, Self::Error> {
        use proto_parse_hex_error::Kind::*;
        match error.kind {
            Some(kind) => match kind {
                InvalidHexDigit(v) => Ok(ParseHexError::InvalidHexDigit(char::from_proto(v)?)),
                OddLength(()) => Ok(ParseHexError::OddLength),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoParseHexError::kind`",
            )),
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
        fn parse_error_protobuf_roundtrip(expect in any::<ParseError>()) {
            let actual = protobuf_roundtrip::<_, ProtoParseError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn parse_hex_error_protobuf_roundtrip(expect in any::<ParseHexError>()) {
            let actual = protobuf_roundtrip::<_, ProtoParseHexError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
