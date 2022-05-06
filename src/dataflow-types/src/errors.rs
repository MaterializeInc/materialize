// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use bytes::BufMut;
use mz_expr::EvalError;
use mz_persist_types::Codec;
use mz_repr::GlobalId;

use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum DecodeError {
    Text(String),
}

// We only want to support persisting DecodeError for now, and not the full DataflowError, which is
// a bit more complex.
impl Codec for DecodeError {
    fn codec_name() -> String {
        "DecodeError".into()
    }

    fn encode<B: BufMut>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        match serde_json::to_writer(buf.writer(), self) {
            Ok(ok) => ok,
            Err(e) => panic!("Encoding error, trying to encode {}: {}", self, e),
        };
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let decoded = serde_json::from_slice(buf).map_err(|e| format!("Decoding error: {}", e))?;
        Ok(decoded)
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
