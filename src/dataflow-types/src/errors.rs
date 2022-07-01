// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt::Display;

use bytes::BufMut;
use prost::Message;
use serde::{Deserialize, Serialize};

use mz_expr::EvalError;
use mz_persist_types::Codec;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::GlobalId;

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.errors.rs"));

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum DecodeError {
    Text(String),
}

impl RustType<ProtoDecodeError> for DecodeError {
    fn into_proto(&self) -> ProtoDecodeError {
        use proto_decode_error::Kind::*;
        ProtoDecodeError {
            kind: Some(match self {
                DecodeError::Text(v) => Text(v.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoDecodeError) -> Result<Self, TryFromProtoError> {
        use proto_decode_error::Kind::*;
        match proto.kind {
            Some(Text(v)) => Ok(DecodeError::Text(v)),
            None => Err(TryFromProtoError::missing_field("ProtoDecodeError::kind")),
        }
    }
}

impl Codec for DecodeError {
    fn codec_name() -> String {
        "protobuf[DecodeError]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDecodeError::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Text(e) => write!(f, "Text: {}", e),
        }
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct SourceError {
    pub source_id: GlobalId,
    pub error: SourceErrorDetails,
}

impl SourceError {
    pub fn new(source_id: GlobalId, error: SourceErrorDetails) -> SourceError {
        SourceError { source_id, error }
    }
}

impl RustType<ProtoSourceError> for SourceError {
    fn into_proto(&self) -> ProtoSourceError {
        ProtoSourceError {
            source_id: Some(self.source_id.into_proto()),
            error: Some((&self.error).into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceError) -> Result<Self, TryFromProtoError> {
        Ok(SourceError {
            source_id: proto
                .source_id
                .into_rust_if_some("ProtoSourceError::source_id")?,
            error: proto.error.into_rust_if_some("ProtoSourceError::error")?,
        })
    }
}

impl Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.source_id)?;
        self.error.fmt(f)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum SourceErrorDetails {
    Initialization(String),
    FileIO(String),
    Persistence(String),
    Other(String),
}

impl RustType<ProtoSourceErrorDetails> for SourceErrorDetails {
    fn into_proto(&self) -> ProtoSourceErrorDetails {
        use proto_source_error_details::Kind;
        ProtoSourceErrorDetails {
            kind: Some(match self {
                SourceErrorDetails::Initialization(s) => Kind::Initialization(s.clone()),
                SourceErrorDetails::FileIO(s) => Kind::FileIo(s.clone()),
                SourceErrorDetails::Persistence(s) => Kind::Persistence(s.clone()),
                SourceErrorDetails::Other(s) => Kind::Other(s.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceErrorDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_error_details::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Initialization(s) => Ok(SourceErrorDetails::Initialization(s)),
                Kind::FileIo(s) => Ok(SourceErrorDetails::FileIO(s)),
                Kind::Persistence(s) => Ok(SourceErrorDetails::Persistence(s)),
                Kind::Other(s) => Ok(SourceErrorDetails::Other(s)),
            },
            None => Err(TryFromProtoError::missing_field(
                "ProtoSourceErrorDetails::kind",
            )),
        }
    }
}

impl Display for SourceErrorDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceErrorDetails::Initialization(e) => {
                write!(
                    f,
                    "failed during initialization, must be dropped and recreated: {}",
                    e
                )
            }
            SourceErrorDetails::FileIO(e) => write!(f, "file IO: {}", e),
            SourceErrorDetails::Persistence(e) => write!(f, "persistence: {}", e),
            SourceErrorDetails::Other(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum DataflowError {
    DecodeError(DecodeError),
    EvalError(EvalError),
    SourceError(SourceError),
}

impl Error for DataflowError {}

impl RustType<ProtoDataflowError> for DataflowError {
    fn into_proto(&self) -> ProtoDataflowError {
        use proto_dataflow_error::Kind::*;
        ProtoDataflowError {
            kind: Some(match self {
                DataflowError::DecodeError(err) => DecodeError(err.into_proto()),
                DataflowError::EvalError(err) => EvalError(err.into_proto()),
                DataflowError::SourceError(err) => SourceError(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDataflowError) -> Result<Self, TryFromProtoError> {
        use proto_dataflow_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                DecodeError(err) => Ok(DataflowError::DecodeError(err.into_rust()?)),
                EvalError(err) => Ok(DataflowError::EvalError(err.into_rust()?)),
                SourceError(err) => Ok(DataflowError::SourceError(err.into_rust()?)),
            },
            None => Err(TryFromProtoError::missing_field("ProtoDataflowError::kind")),
        }
    }
}

impl Codec for DataflowError {
    fn codec_name() -> String {
        "protobuf[DataflowError]".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B) {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors");
    }

    fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDataflowError::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl Display for DataflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataflowError::DecodeError(e) => write!(f, "Decode error: {}", e),
            DataflowError::EvalError(e) => write!(f, "Evaluation error: {}", e),
            DataflowError::SourceError(e) => write!(f, "Source error: {}", e),
        }
    }
}

impl From<DecodeError> for DataflowError {
    fn from(e: DecodeError) -> Self {
        Self::DecodeError(e)
    }
}

impl From<EvalError> for DataflowError {
    fn from(e: EvalError) -> Self {
        Self::EvalError(e)
    }
}
impl From<SourceError> for DataflowError {
    fn from(e: SourceError) -> Self {
        Self::SourceError(e)
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec;

    use super::DecodeError;

    #[test]
    fn test_decode_error_codec_roundtrip() -> Result<(), String> {
        let original = DecodeError::Text("ciao".to_string());
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = DecodeError::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }
}
