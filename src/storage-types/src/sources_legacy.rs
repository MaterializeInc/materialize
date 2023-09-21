// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{ProtoType, RustType, TryFromProtoError};
use prost::Message;

use crate::errors::{
    DataflowError, EnvelopeError, ProtoDataflowError, UpsertError, UpsertValueError,
};
use crate::sources::{ProtoSourceData, SourceData};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources_legacy.rs"
));

pub(crate) fn decode_source_data_with_fallback(buf: &[u8]) -> Result<SourceData, String> {
    let decoded = ProtoSourceData::decode(buf)
        .ok()
        .and_then(|proto| proto.into_rust().ok());

    match decoded {
        Some(data) => Ok(data),
        // Try to fall back to legacy encoding.
        None => {
            let proto = ProtoSourceDataLegacy::decode(buf).map_err(|err| err.to_string())?;
            proto.into_rust().map_err(|err| err.to_string())
        }
    }
}

pub(crate) fn decode_dataflow_error_with_fallback(buf: &[u8]) -> Result<DataflowError, String> {
    let decoded = ProtoDataflowError::decode(buf)
        .ok()
        .and_then(|proto| proto.into_rust().ok());

    match decoded {
        Some(data) => Ok(data),
        // Try to fall back to legacy encoding.
        None => {
            let proto = ProtoDataflowErrorLegacy::decode(buf).map_err(|err| err.to_string())?;
            proto.into_rust().map_err(|err| err.to_string())
        }
    }
}

impl RustType<ProtoSourceDataLegacy> for SourceData {
    fn into_proto(&self) -> ProtoSourceDataLegacy {
        use proto_source_data_legacy::Kind;
        ProtoSourceDataLegacy {
            kind: Some(match &**self {
                Ok(row) => Kind::Ok(row.into_proto()),
                Err(err) => Kind::Err(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceDataLegacy) -> Result<Self, TryFromProtoError> {
        use proto_source_data_legacy::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Ok(row) => Ok(SourceData(Ok(row.into_rust()?))),
                Kind::Err(err) => Ok(SourceData(Err(err.into_rust()?))),
            },
            None => Result::Err(TryFromProtoError::missing_field(
                "ProtoSourceDataLegacy::kind",
            )),
        }
    }
}

impl RustType<ProtoDataflowErrorLegacy> for DataflowError {
    fn into_proto(&self) -> ProtoDataflowErrorLegacy {
        use proto_dataflow_error_legacy::Kind::*;
        ProtoDataflowErrorLegacy {
            kind: Some(match self {
                DataflowError::DecodeError(err) => DecodeError(*err.into_proto()),
                DataflowError::EvalError(err) => EvalError(*err.into_proto()),
                DataflowError::SourceError(err) => SourceError(*err.into_proto()),
                DataflowError::EnvelopeError(err) => EnvelopeErrorV1(Box::new(*err.into_proto())),
            }),
        }
    }

    fn from_proto(proto: ProtoDataflowErrorLegacy) -> Result<Self, TryFromProtoError> {
        use proto_dataflow_error_legacy::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                DecodeError(err) => Ok(DataflowError::DecodeError(Box::new(err.into_rust()?))),
                EvalError(err) => Ok(DataflowError::EvalError(Box::new(err.into_rust()?))),
                SourceError(err) => Ok(DataflowError::SourceError(Box::new(err.into_rust()?))),
                EnvelopeErrorV1(err) => {
                    Ok(DataflowError::EnvelopeError(Box::new((*err).into_rust()?)))
                }
            },
            None => Err(TryFromProtoError::missing_field(
                "ProtoDataflowErrorLegacy::kind",
            )),
        }
    }
}

impl RustType<ProtoEnvelopeErrorV1Legacy> for EnvelopeError {
    fn into_proto(&self) -> ProtoEnvelopeErrorV1Legacy {
        use proto_envelope_error_v1_legacy::Kind;
        ProtoEnvelopeErrorV1Legacy {
            kind: Some(match self {
                EnvelopeError::Debezium(text) => Kind::Debezium(text.clone()),
                EnvelopeError::Upsert(rust) => Kind::Upsert(Box::new(rust.into_proto())),
                EnvelopeError::Flat(text) => Kind::Flat(text.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoEnvelopeErrorV1Legacy) -> Result<Self, TryFromProtoError> {
        use proto_envelope_error_v1_legacy::Kind;
        match proto.kind {
            Some(Kind::Debezium(text)) => Ok(Self::Debezium(text)),
            Some(Kind::Upsert(proto)) => {
                let rust = RustType::from_proto(*proto)?;
                Ok(Self::Upsert(rust))
            }
            Some(Kind::Flat(text)) => Ok(Self::Flat(text)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoEnvelopeErrorV1Legacy::kind",
            )),
        }
    }
}

impl RustType<ProtoUpsertErrorLegacy> for UpsertError {
    fn into_proto(&self) -> ProtoUpsertErrorLegacy {
        use proto_upsert_error_legacy::Kind;
        ProtoUpsertErrorLegacy {
            kind: Some(match self {
                UpsertError::KeyDecode(err) => Kind::KeyDecode(err.into_proto()),
                UpsertError::Value(err) => Kind::Value(Box::new(err.into_proto())),
                UpsertError::NullKey(err) => Kind::NullKey(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoUpsertErrorLegacy) -> Result<Self, TryFromProtoError> {
        use proto_upsert_error_legacy::Kind;
        match proto.kind {
            Some(Kind::KeyDecode(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::KeyDecode(rust))
            }
            Some(Kind::Value(proto)) => {
                let rust = RustType::from_proto(*proto)?;
                Ok(Self::Value(rust))
            }
            Some(Kind::NullKey(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::NullKey(rust))
            }
            None => Err(TryFromProtoError::missing_field(
                "ProtoUpsertErrorLegacy::kind",
            )),
        }
    }
}

impl RustType<ProtoUpsertValueErrorLegacy> for UpsertValueError {
    fn into_proto(&self) -> ProtoUpsertValueErrorLegacy {
        let inner = ProtoDataflowErrorLegacy {
            kind: Some(proto_dataflow_error_legacy::Kind::DecodeError(
                self.inner.into_proto(),
            )),
        };

        ProtoUpsertValueErrorLegacy {
            inner: Some(Box::new(inner)),
            for_key: Some(self.for_key.into_proto()),
        }
    }

    fn from_proto(proto: ProtoUpsertValueErrorLegacy) -> Result<Self, TryFromProtoError> {
        let inner = match proto.inner {
            Some(inner) => match inner.kind {
                Some(proto_dataflow_error_legacy::Kind::DecodeError(error)) => {
                    RustType::from_proto(error)?
                }
                _ => panic!("unexpected kind in ProtoUpsertValueErrorLegacy: {inner:?}"),
            },
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoUpsertValueErrorLegacy::inner",
                ))
            }
        };
        let for_key = match proto.for_key {
            Some(key) => RustType::from_proto(key)?,
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoUpsertValueErrorLegacy::for_key",
                ))
            }
        };
        Ok(Self { inner, for_key })
    }
}
