// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error type for fallible conversion from [`super::Value`] to [`mz_repr::Datum`].

use std::error::Error;
use std::fmt;

use mz_repr::adt::array::InvalidArrayError;
use mz_repr::adt::range::InvalidRangeError;

/// Errors that can occur when converting a [`super::Value`] into a [`mz_repr::Datum`].
#[derive(Debug)]
pub enum IntoDatumError {
    /// Invalid range (e.g. misordered bounds).
    Range(InvalidRangeError),
    /// Invalid array (e.g. wrong cardinality or too many dimensions).
    Array(InvalidArrayError),
}

impl fmt::Display for IntoDatumError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntoDatumError::Range(e) => e.fmt(f),
            IntoDatumError::Array(e) => e.fmt(f),
        }
    }
}

impl Error for IntoDatumError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            IntoDatumError::Range(e) => Some(e),
            IntoDatumError::Array(e) => Some(e),
        }
    }
}

impl From<InvalidRangeError> for IntoDatumError {
    fn from(e: InvalidRangeError) -> Self {
        IntoDatumError::Range(e)
    }
}

impl From<InvalidArrayError> for IntoDatumError {
    fn from(e: InvalidArrayError) -> Self {
        IntoDatumError::Array(e)
    }
}
