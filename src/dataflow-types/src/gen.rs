// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

use expr::EvalError;
use repr::{gen::*, Row};

use crate::gen::errors::{
    proto_eval_error, proto_row_result, proto_source_error, ProtoEvalError, ProtoEvalErrorSimple,
    ProtoRowResult, ProtoSourceError,
};
use crate::{DataflowError, DecodeError, SourceError, SourceErrorDetails};

include!(concat!(env!("OUT_DIR"), "/protobuf/mod.rs"));

impl From<Result<Row, DataflowError>> for ProtoRowResult {
    fn from(x: Result<Row, DataflowError>) -> Self {
        let result = match x {
            Ok(row) => proto_row_result::Result::row((&row).into()),
            Err(DataflowError::DecodeError(DecodeError::Text(x))) => {
                proto_row_result::Result::decode(x)
            }
            Err(DataflowError::EvalError(x)) => proto_row_result::Result::eval(x.into()),
            Err(DataflowError::SourceError(x)) => proto_row_result::Result::source(x.into()),
        };
        ProtoRowResult {
            result: Some(result),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl From<EvalError> for ProtoEvalError {
    fn from(x: EvalError) -> Self {
        let variant = match x {
            EvalError::DivisionByZero => {
                proto_eval_error::Variant::simple(ProtoEvalErrorSimple::DIVISION_BY_ZERO.into())
            }
            EvalError::FeatureNotSupported(x) => {
                proto_eval_error::Variant::feature_not_supported(x)
            }
            EvalError::FloatOverflow => {
                proto_eval_error::Variant::simple(ProtoEvalErrorSimple::FLOAT_OVERFLOW.into())
            }
            EvalError::FloatUnderflow => {
                proto_eval_error::Variant::simple(ProtoEvalErrorSimple::FLOAT_UNDERFLOW.into())
            }
            EvalError::NumericFieldOverflow => proto_eval_error::Variant::simple(
                ProtoEvalErrorSimple::NUMERIC_FIELD_OVERFLOW.into(),
            ),
            EvalError::Float32OutOfRange => {
                proto_eval_error::Variant::simple(ProtoEvalErrorSimple::FLOAT32_OUT_OF_RANGE.into())
            }
            EvalError::Float64OutOfRange => {
                proto_eval_error::Variant::simple(ProtoEvalErrorSimple::FLOAT64_OUT_OF_RANGE.into())
            }
            EvalError::InvalidBase64Symbol(x) => {
                proto_eval_error::Variant::invalid_base64_symbol(x.into())
            }
            EvalError::InvalidTimezone(x) => proto_eval_error::Variant::invalid_timezone(x),
            EvalError::InvalidEncodingName(x) => {
                proto_eval_error::Variant::invalid_encoding_name(x)
            }
            _ => todo!("WIP"),
        };
        ProtoEvalError {
            variant: Some(variant),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl From<SourceError> for ProtoSourceError {
    fn from(x: SourceError) -> Self {
        let details = match x.error {
            SourceErrorDetails::Initialization(x) => proto_source_error::Details::initialization(x),
            SourceErrorDetails::FileIO(x) => proto_source_error::Details::file_io(x),
            SourceErrorDetails::Persistence(x) => proto_source_error::Details::persistence(x),
        };
        ProtoSourceError {
            source_name: x.source_name,
            details: Some(details),
            cached_size: Default::default(),
            unknown_fields: Default::default(),
        }
    }
}

impl TryFrom<ProtoRowResult> for Row {
    type Error = DataflowError;

    // WIP is it reasonable for this to map proto decoding errors to the
    // DecodeError variant? probably not. we likely need to return a Result of a
    // Result instead.
    fn try_from(x: ProtoRowResult) -> Result<Self, Self::Error> {
        match x.result {
            Some(proto_row_result::Result::row(x)) => match Row::try_from(&x) {
                Ok(x) => Ok(x),
                Err(x) => Err(DataflowError::DecodeError(DecodeError::Text(x))),
            },
            Some(proto_row_result::Result::decode(x)) => {
                Err(DataflowError::DecodeError(DecodeError::Text(x)))
            }
            Some(proto_row_result::Result::eval(x)) => match EvalError::try_from(x) {
                Ok(x) => Err(DataflowError::EvalError(x)),
                Err(x) => Err(DataflowError::DecodeError(DecodeError::Text(x))),
            },
            Some(proto_row_result::Result::source(x)) => match SourceError::try_from(x) {
                Ok(x) => Err(DataflowError::SourceError(x)),
                Err(x) => Err(DataflowError::DecodeError(DecodeError::Text(x))),
            },
            None => Err(DataflowError::DecodeError(DecodeError::Text(
                "unknown result".into(),
            ))),
        }
    }
}

impl TryFrom<ProtoEvalError> for EvalError {
    type Error = String;

    fn try_from(x: ProtoEvalError) -> Result<Self, Self::Error> {
        match x.variant {
            Some(proto_eval_error::Variant::simple(x)) => match x.enum_value() {
                Ok(x) => EvalError::try_from(x),
                Err(x) => Err(format!("unknown simple EvalError: {}", x)),
            },
            Some(proto_eval_error::Variant::feature_not_supported(x)) => {
                Ok(EvalError::FeatureNotSupported(x))
            }
            Some(_) => todo!(),
            None => Err("unknown EvalError variant".into()),
        }
    }
}

impl TryFrom<ProtoEvalErrorSimple> for EvalError {
    type Error = String;

    fn try_from(x: ProtoEvalErrorSimple) -> Result<Self, Self::Error> {
        match x {
            ProtoEvalErrorSimple::UNKNOWN => Err("unknown simple EvalError".into()),
            ProtoEvalErrorSimple::DIVISION_BY_ZERO => Ok(EvalError::DivisionByZero),
            ProtoEvalErrorSimple::FLOAT_OVERFLOW => Ok(EvalError::FloatOverflow),
            ProtoEvalErrorSimple::FLOAT_UNDERFLOW => Ok(EvalError::FloatUnderflow),
            ProtoEvalErrorSimple::NUMERIC_FIELD_OVERFLOW => Ok(EvalError::NumericFieldOverflow),
            ProtoEvalErrorSimple::FLOAT32_OUT_OF_RANGE => Ok(EvalError::Float32OutOfRange),
            ProtoEvalErrorSimple::FLOAT64_OUT_OF_RANGE => Ok(EvalError::Float64OutOfRange),
        }
    }
}

impl TryFrom<ProtoSourceError> for SourceError {
    type Error = String;

    fn try_from(x: ProtoSourceError) -> Result<Self, Self::Error> {
        let error = match x.details {
            Some(proto_source_error::Details::initialization(x)) => {
                SourceErrorDetails::Initialization(x)
            }
            Some(proto_source_error::Details::file_io(x)) => SourceErrorDetails::FileIO(x),
            Some(proto_source_error::Details::persistence(x)) => SourceErrorDetails::Persistence(x),
            None => return Err("missing SourceError details".into()),
        };
        Ok(SourceError {
            source_name: x.source_name,
            error,
        })
    }
}

#[cfg(test)]
mod tests {
    use expr::EvalError;
    use repr::{Datum, Row};

    use crate::gen::errors::ProtoRowResult;
    use crate::{DataflowError, DecodeError, SourceError, SourceErrorDetails};

    #[test]
    fn roundtrip() {
        fn testcase(x: Result<Row, DataflowError>) {
            let encoded = ProtoRowResult::from(x.clone());
            let decoded = Row::try_from(encoded.clone());
            eprintln!("{:?} -> {:?} -> {:?}", &x, &encoded, &decoded);
            assert_eq!(decoded, x);
        }

        testcase(Ok(Row::pack_slice(&[
            Datum::Int32(7),
            Datum::String("foo"),
        ])));
        testcase(Err(DataflowError::DecodeError(DecodeError::Text(
            "foo".into(),
        ))));
        testcase(Err(DataflowError::EvalError(EvalError::DivisionByZero)));
        testcase(Err(DataflowError::EvalError(
            EvalError::FeatureNotSupported("foo".into()),
        )));
        testcase(Err(DataflowError::SourceError(SourceError {
            source_name: "foo".into(),
            error: SourceErrorDetails::Initialization("bar".into()),
        })));
        testcase(Err(DataflowError::SourceError(SourceError {
            source_name: "foo".into(),
            error: SourceErrorDetails::FileIO("bar".into()),
        })));
        testcase(Err(DataflowError::SourceError(SourceError {
            source_name: "foo".into(),
            error: SourceErrorDetails::Persistence("bar".into()),
        })));
    }
}
