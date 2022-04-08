// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

include!(concat!(env!("OUT_DIR"), "/scalar.rs"));

use crate::scalar::{DomainLimit, EvalError};
use mz_repr::proto::{ProtoRepr, TryFromProtoError, TryIntoIfSome};

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

impl From<&EvalError> for ProtoEvalError {
    fn from(error: &EvalError) -> Self {
        use proto_eval_error::*;
        use proto_incompatible_array_dimensions::*;
        use Kind::*;
        let kind = match error {
            EvalError::CharacterNotValidForEncoding(v) => CharacterNotValidForEncoding(*v),
            EvalError::CharacterTooLargeForEncoding(v) => CharacterTooLargeForEncoding(*v),
            EvalError::DateBinOutOfRange(v) => DateBinOutOfRange(v.clone()),
            EvalError::DivisionByZero => DivisionByZero(()),
            EvalError::Unsupported { feature, issue_no } => Unsupported(ProtoUnsupported {
                feature: feature.clone(),
                issue_no: issue_no.into_proto(),
            }),
            EvalError::FloatOverflow => FloatOverflow(()),
            EvalError::FloatUnderflow => FloatUnderflow(()),
            EvalError::NumericFieldOverflow => NumericFieldOverflow(()),
            EvalError::Float32OutOfRange => Float32OutOfRange(()),
            EvalError::Float64OutOfRange => Float64OutOfRange(()),
            EvalError::Int16OutOfRange => Int16OutOfRange(()),
            EvalError::Int32OutOfRange => Int32OutOfRange(()),
            EvalError::Int64OutOfRange => Int64OutOfRange(()),
            EvalError::OidOutOfRange => OidOutOfRange(()),
            EvalError::IntervalOutOfRange => IntervalOutOfRange(()),
            EvalError::TimestampOutOfRange => TimestampOutOfRange(()),
            EvalError::CharOutOfRange => CharOutOfRange(()),
            EvalError::InvalidBase64Equals => InvalidBase64Equals(()),
            EvalError::InvalidBase64Symbol(sym) => InvalidBase64Symbol(sym.into_proto()),
            EvalError::InvalidBase64EndSequence => InvalidBase64EndSequence(()),
            EvalError::InvalidTimezone(tz) => InvalidTimezone(tz.clone()),
            EvalError::InvalidTimezoneInterval => InvalidTimezoneInterval(()),
            EvalError::InvalidTimezoneConversion => InvalidTimezoneConversion(()),
            EvalError::InvalidLayer { max_layer, val } => InvalidLayer(ProtoInvalidLayer {
                max_layer: max_layer.into_proto(),
                val: *val,
            }),
            EvalError::InvalidArray(error) => InvalidArray(error.into()),
            EvalError::InvalidEncodingName(v) => InvalidEncodingName(v.clone()),
            EvalError::InvalidHashAlgorithm(v) => InvalidHashAlgorithm(v.clone()),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => InvalidByteSequence(ProtoInvalidByteSequence {
                byte_sequence: byte_sequence.clone(),
                encoding_name: encoding_name.clone(),
            }),
            EvalError::InvalidJsonbCast { from, to } => InvalidJsonbCast(ProtoInvalidJsonbCast {
                from: from.clone(),
                to: to.clone(),
            }),
            EvalError::InvalidRegex(v) => InvalidRegex(v.clone()),
            EvalError::InvalidRegexFlag(v) => InvalidRegexFlag(v.into_proto()),
            EvalError::InvalidParameterValue(v) => InvalidParameterValue(v.clone()),
            EvalError::NegSqrt => NegSqrt(()),
            EvalError::NullCharacterNotPermitted => NullCharacterNotPermitted(()),
            EvalError::UnknownUnits(v) => UnknownUnits(v.clone()),
            EvalError::UnsupportedUnits(units, typ) => UnsupportedUnits(ProtoUnsupportedUnits {
                units: units.clone(),
                typ: typ.clone(),
            }),
            EvalError::UnterminatedLikeEscapeSequence => UnterminatedLikeEscapeSequence(()),
            EvalError::Parse(error) => Parse(error.into()),
            EvalError::ParseHex(error) => ParseHex(error.into()),
            EvalError::Internal(v) => Internal(v.clone()),
            EvalError::InfinityOutOfDomain(v) => InfinityOutOfDomain(v.clone()),
            EvalError::NegativeOutOfDomain(v) => NegativeOutOfDomain(v.clone()),
            EvalError::ZeroOutOfDomain(v) => ZeroOutOfDomain(v.clone()),
            EvalError::OutOfDomain(lower, upper, id) => OutOfDomain(ProtoOutOfDomain {
                lower: Some(lower.into()),
                upper: Some(upper.into()),
                id: id.clone(),
            }),
            EvalError::ComplexOutOfRange(v) => ComplexOutOfRange(v.clone()),
            EvalError::MultipleRowsFromSubquery => MultipleRowsFromSubquery(()),
            EvalError::Undefined(v) => Undefined(v.clone()),
            EvalError::LikePatternTooLong => LikePatternTooLong(()),
            EvalError::LikeEscapeTooLong => LikeEscapeTooLong(()),
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => StringValueTooLong(ProtoStringValueTooLong {
                target_type: target_type.clone(),
                length: length.into_proto(),
            }),
            EvalError::MultidimensionalArrayRemovalNotSupported => {
                MultidimensionalArrayRemovalNotSupported(())
            }
            EvalError::IncompatibleArrayDimensions { dims } => {
                IncompatibleArrayDimensions(ProtoIncompatibleArrayDimensions {
                    dims: dims.map(|dims| ProtoDims {
                        f0: dims.0.into_proto(),
                        f1: dims.1.into_proto(),
                    }),
                })
            }
            EvalError::TypeFromOid(v) => TypeFromOid(v.clone()),
        };
        ProtoEvalError { kind: Some(kind) }
    }
}

impl TryFrom<ProtoEvalError> for EvalError {
    type Error = TryFromProtoError;

    fn try_from(error: ProtoEvalError) -> Result<Self, Self::Error> {
        use proto_eval_error::Kind::*;
        match error.kind {
            Some(kind) => match kind {
                CharacterNotValidForEncoding(v) => Ok(EvalError::CharacterNotValidForEncoding(v)),
                CharacterTooLargeForEncoding(v) => Ok(EvalError::CharacterTooLargeForEncoding(v)),
                DateBinOutOfRange(v) => Ok(EvalError::DateBinOutOfRange(v)),
                DivisionByZero(()) => Ok(EvalError::DivisionByZero),
                Unsupported(v) => Ok(EvalError::Unsupported {
                    feature: v.feature,
                    issue_no: Option::<usize>::from_proto(v.issue_no)?,
                }),
                FloatOverflow(()) => Ok(EvalError::FloatOverflow),
                FloatUnderflow(()) => Ok(EvalError::FloatUnderflow),
                NumericFieldOverflow(()) => Ok(EvalError::NumericFieldOverflow),
                Float32OutOfRange(()) => Ok(EvalError::Float32OutOfRange),
                Float64OutOfRange(()) => Ok(EvalError::Float64OutOfRange),
                Int16OutOfRange(()) => Ok(EvalError::Int16OutOfRange),
                Int32OutOfRange(()) => Ok(EvalError::Int32OutOfRange),
                Int64OutOfRange(()) => Ok(EvalError::Int64OutOfRange),
                OidOutOfRange(()) => Ok(EvalError::OidOutOfRange),
                IntervalOutOfRange(()) => Ok(EvalError::IntervalOutOfRange),
                TimestampOutOfRange(()) => Ok(EvalError::TimestampOutOfRange),
                CharOutOfRange(()) => Ok(EvalError::CharOutOfRange),
                InvalidBase64Equals(()) => Ok(EvalError::InvalidBase64Equals),
                InvalidBase64Symbol(v) => char::from_proto(v).map(EvalError::InvalidBase64Symbol),
                InvalidBase64EndSequence(()) => Ok(EvalError::InvalidBase64EndSequence),
                InvalidTimezone(v) => Ok(EvalError::InvalidTimezone(v)),
                InvalidTimezoneInterval(()) => Ok(EvalError::InvalidTimezoneInterval),
                InvalidTimezoneConversion(()) => Ok(EvalError::InvalidTimezoneConversion),
                InvalidLayer(v) => Ok(EvalError::InvalidLayer {
                    max_layer: usize::from_proto(v.max_layer)?,
                    val: v.val,
                }),
                InvalidArray(error) => Ok(EvalError::InvalidArray(error.try_into()?)),
                InvalidEncodingName(v) => Ok(EvalError::InvalidEncodingName(v)),
                InvalidHashAlgorithm(v) => Ok(EvalError::InvalidHashAlgorithm(v)),
                InvalidByteSequence(v) => Ok(EvalError::InvalidByteSequence {
                    byte_sequence: v.byte_sequence,
                    encoding_name: v.encoding_name,
                }),
                InvalidJsonbCast(v) => Ok(EvalError::InvalidJsonbCast {
                    from: v.from,
                    to: v.to,
                }),
                InvalidRegex(v) => Ok(EvalError::InvalidRegex(v)),
                InvalidRegexFlag(v) => Ok(EvalError::InvalidRegexFlag(char::from_proto(v)?)),
                InvalidParameterValue(v) => Ok(EvalError::InvalidParameterValue(v)),
                NegSqrt(()) => Ok(EvalError::NegSqrt),
                NullCharacterNotPermitted(()) => Ok(EvalError::NullCharacterNotPermitted),
                UnknownUnits(v) => Ok(EvalError::UnknownUnits(v)),
                UnsupportedUnits(v) => Ok(EvalError::UnsupportedUnits(v.units, v.typ)),
                UnterminatedLikeEscapeSequence(()) => Ok(EvalError::UnterminatedLikeEscapeSequence),
                Parse(error) => Ok(EvalError::Parse(error.try_into()?)),
                ParseHex(error) => Ok(EvalError::ParseHex(error.try_into()?)),
                Internal(v) => Ok(EvalError::Internal(v)),
                InfinityOutOfDomain(v) => Ok(EvalError::InfinityOutOfDomain(v)),
                NegativeOutOfDomain(v) => Ok(EvalError::NegativeOutOfDomain(v)),
                ZeroOutOfDomain(v) => Ok(EvalError::ZeroOutOfDomain(v)),
                OutOfDomain(v) => Ok(EvalError::OutOfDomain(
                    v.lower.try_into_if_some("`ProtoDomainLimit::lower`")?,
                    v.upper.try_into_if_some("`ProtoDomainLimit::upper`")?,
                    v.id,
                )),
                ComplexOutOfRange(v) => Ok(EvalError::ComplexOutOfRange(v)),
                MultipleRowsFromSubquery(()) => Ok(EvalError::MultipleRowsFromSubquery),
                Undefined(v) => Ok(EvalError::Undefined(v)),
                LikePatternTooLong(()) => Ok(EvalError::LikePatternTooLong),
                LikeEscapeTooLong(()) => Ok(EvalError::LikeEscapeTooLong),
                StringValueTooLong(v) => Ok(EvalError::StringValueTooLong {
                    target_type: v.target_type,
                    length: usize::from_proto(v.length)?,
                }),
                MultidimensionalArrayRemovalNotSupported(()) => {
                    Ok(EvalError::MultidimensionalArrayRemovalNotSupported)
                }
                IncompatibleArrayDimensions(v) => Ok(EvalError::IncompatibleArrayDimensions {
                    dims: v
                        .dims
                        .map::<Result<_, TryFromProtoError>, _>(|dims| {
                            let f0 = usize::from_proto(dims.f0)?;
                            let f1 = usize::from_proto(dims.f1)?;
                            Ok((f0, f1))
                        })
                        .transpose()?,
                }),
                TypeFromOid(v) => Ok(EvalError::TypeFromOid(v)),
            },
            None => Err(TryFromProtoError::missing_field("`ProtoEvalError::kind`")),
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

    proptest! {
        #[test]
        fn eval_error_protobuf_roundtrip(expect in any::<EvalError>()) {
            let actual = protobuf_roundtrip::<_, ProtoEvalError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
