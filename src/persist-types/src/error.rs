// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Codec related errors.

use std::{error, fmt};

/// A Codec related error.
#[derive(Debug, Clone)]
pub enum CodecError {
    /// An unstructured codec related error.
    String(String),
    /// The encoding version is incompatible with what we can currently decode.
    InvalidEncodingVersion(Option<usize>),
}

impl error::Error for CodecError {}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::String(e) => f.write_str(e),
            CodecError::InvalidEncodingVersion(v) => {
                f.write_str(&format!("invalid encoding version: {:?}", v))
            }
        }
    }
}

impl From<String> for CodecError {
    fn from(e: String) -> Self {
        CodecError::String(e)
    }
}

impl<'a> From<&'a str> for CodecError {
    fn from(e: &'a str) -> Self {
        CodecError::String(e.into())
    }
}

// Hack so we can debug_assert_eq against Result<(), CodecError>.
impl PartialEq for CodecError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CodecError::String(a), CodecError::String(b)) => a == b,
            (CodecError::InvalidEncodingVersion(a), CodecError::InvalidEncodingVersion(b)) => {
                a == b
            }
            _ => false,
        }
    }
}
