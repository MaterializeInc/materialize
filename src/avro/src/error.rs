// Copyright 2018 Flavien Raynaud.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is derived from the avro-rs project, available at
// https://github.com/flavray/avro-rs. It was incorporated
// directly into Materialize on March 3, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use crate::types::ScalarKind;
use crate::{util::TsUnit, ParseSchemaError, SchemaResolutionError};

use chrono::NaiveDateTime;
use fmt::{Debug, Display};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeError {
    CodecUtf8Error,
    MapKeyUtf8Error,
    StringUtf8Error,
    UuidUtf8Error,
    UnrecognizedCodec(String),
    BadSnappyChecksum {
        expected: u32,
        actual: u32,
    },
    ExpectedNonnegInteger(i64),
    BadTimestamp {
        unit: TsUnit,
        seconds: i64,
        fraction: u32,
    },
    BadBoolean(u8),
    BadDate(i32),
    // The distinction between "bad" and "missing",
    // for both unions and enums,
    // is that a "bad" index was not found in the writer schema,
    // meaning either the input is corrupt or there is a bug in this crate,
    // whereas a "missing" index means the value was validly written,
    // but can't be interpreted by the _reader_ schema
    BadUnionIndex {
        index: usize,
        len: usize,
    },
    MissingUnionIndex(usize),
    BadEnumIndex {
        index: usize,
        len: usize,
    },
    MissingEnumIndex {
        index: usize,
        symbol: String,
    },
    WrongUnionIndex {
        expected: usize,
        actual: usize,
    },
    UnexpectedRecord,
    UnexpectedUnion,
    UnexpectedArray,
    UnexpectedMap,
    UnexpectedEnum,
    UnexpectedScalar,
    UnexpectedDecimal,
    UnexpectedBytes,
    UnexpectedString,
    UnexpectedJson,
    UnexpectedUuid,
    UnexpectedFixed,
    UnexpectedScalarKind(ScalarKind),
    WrongHeaderMagic([u8; 4]),
    MissingAvroDotSchema,
    I32OutOfRange(i64),
    IntConversionError,
    IntDecodeOverflow,
    BadJson(serde_json::error::Category),
    BadUuid(uuid::Error),
    MismatchedBlockHeader {
        expected: [u8; 16],
        actual: [u8; 16],
    },
    DateOutOfRange(i32),
    TimestampOutOfRange(NaiveDateTime),
    Custom(String),
}

impl DecodeError {
    fn fmt_inner(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeError::UnrecognizedCodec(codec) => write!(f, "Unrecognized codec: {}", codec),
            DecodeError::BadSnappyChecksum { expected, actual } => write!(
                f,
                "Bad Snappy CRC32; expected {:x} but got {:x}",
                expected, actual
            ),
            DecodeError::ExpectedNonnegInteger(i) => {
                write!(f, "Expected non-negative integer, got {}", i)
            }
            DecodeError::BadTimestamp {
                unit,
                seconds,
                fraction,
            } => write!(f, "Invalid {} timestamp {}.{}", unit, seconds, fraction),
            DecodeError::BadBoolean(byte) => write!(f, "Invalid boolean: {:x}", byte),
            DecodeError::BadDate(since_epoch) => {
                write!(f, "Invalid num days since epoch: {}", since_epoch)
            }
            DecodeError::BadUnionIndex { index, len } => {
                write!(f, "Union index out of bounds: {} (len: {})", index, len)
            }
            DecodeError::MissingUnionIndex(index) => {
                write!(f, "Union variant not found in reader schema: {}", index)
            }
            DecodeError::BadEnumIndex { index, len } => write!(
                f,
                "Enum symbol index out of bounds: {} (len: {})",
                index, len
            ),
            DecodeError::MissingEnumIndex { index, symbol } => write!(
                f,
                "Enum symbol {} at index {} in writer schema not found in reader",
                symbol, index
            ),
            DecodeError::UnexpectedRecord => write!(f, "Unexpected record"),
            DecodeError::UnexpectedUnion => write!(f, "Unexpected union"),
            DecodeError::UnexpectedArray => write!(f, "Unexpected array"),
            DecodeError::UnexpectedMap => write!(f, "Unexpected map"),
            DecodeError::UnexpectedEnum => write!(f, "Unexpected enum"),
            DecodeError::UnexpectedScalar => write!(f, "Unexpected scalar"),
            DecodeError::UnexpectedDecimal => write!(f, "Unexpected decimal"),
            DecodeError::UnexpectedBytes => write!(f, "Unexpected bytes"),
            DecodeError::UnexpectedString => write!(f, "Unexpected string"),
            DecodeError::UnexpectedJson => write!(f, "Unexpected json"),
            DecodeError::UnexpectedUuid => write!(f, "Unexpected UUID"),
            DecodeError::UnexpectedFixed => write!(f, "Unexpected fixed"),
            DecodeError::UnexpectedScalarKind(kind) => {
                write!(f, "Scalar of unexpected kind: {:?}", kind)
            }
            DecodeError::WrongHeaderMagic(magic) => write!(f, "Wrong header magic: {:x?}", magic),
            DecodeError::MissingAvroDotSchema => write!(
                f,
                "Symbol's value as variable is void: avro.schema missing from header"
            ),
            DecodeError::I32OutOfRange(i) => write!(f, "Expected i32, got: {}", i),
            DecodeError::IntDecodeOverflow => write!(f, "Overflow when decoding integer value"),
            DecodeError::WrongUnionIndex { expected, actual } => write!(
                f,
                "Reader expected variant at index {}, got {}",
                expected, actual
            ),
            DecodeError::Custom(inner) => write!(f, "Error in decode client: {}", inner),
            DecodeError::CodecUtf8Error => write!(f, "Codec was not valid UTF-8"),
            DecodeError::MapKeyUtf8Error => write!(f, "Map key was not valid UTF-8"),
            DecodeError::StringUtf8Error => write!(f, "String was not valid UTF-8"),
            DecodeError::UuidUtf8Error => write!(f, "UUID was not valid UTF-8"),
            DecodeError::IntConversionError => write!(f, "Integer conversion failed"),
            DecodeError::BadJson(inner_kind) => write!(f, "Json decoding failed: {:?}", inner_kind),
            DecodeError::BadUuid(inner) => write!(f, "UUID decoding failed: {}", inner),
            DecodeError::MismatchedBlockHeader { expected, actual } => write!(
                f,
                "Block marker ({:x?}) does not match header marker ({:x?})",
                actual, expected
            ),
            DecodeError::DateOutOfRange(inner) => write!(f, "Date out of range: {}", inner),
            DecodeError::TimestampOutOfRange(inner) => {
                write!(f, "Timestamp out of range: {}", inner)
            }
        }
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Decoding error: ")?;
        self.fmt_inner(f)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
// TODO (btv) - some context (where in the record the error occurred) would be nice.
// We can probably get this from the schema; we would just need to pipe it through a ton of places.
pub enum Error {
    Decode(DecodeError),
    ParseSchema(ParseSchemaError), // TODO (btv) - make this a typed enum, like we did for DecodeError.
    ResolveSchema(SchemaResolutionError), // TODO (btv) - idem.
    IO(std::io::ErrorKind),        // Keeping the full error would be nicer, but that isn't `Clone`.
    Allocation { attempted: usize, allowed: usize },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::IO(e.kind())
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(_: std::num::TryFromIntError) -> Self {
        Self::Decode(DecodeError::IntConversionError)
    }
}

impl From<DecodeError> for Error {
    fn from(inner: DecodeError) -> Self {
        Self::Decode(inner)
    }
}

impl From<ParseSchemaError> for Error {
    fn from(inner: ParseSchemaError) -> Self {
        Self::ParseSchema(inner)
    }
}

impl From<SchemaResolutionError> for Error {
    fn from(inner: SchemaResolutionError) -> Self {
        Self::ResolveSchema(inner)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Decode(inner) => write!(f, "Decode error: {}", inner),
            Error::ParseSchema(inner) => write!(f, "Schema parse error: {}", inner),
            Error::IO(inner_kind) => write!(f, "IO error: {:?}", inner_kind),
            Error::Allocation { attempted, allowed } => write!(
                f,
                "Allocation error: attempt to allocate {} bytes (maximum allowed: {})",
                attempted, allowed
            ),
            Error::ResolveSchema(inner) => write!(f, "Schema resolution error: {}", inner),
        }
    }
}

impl std::error::Error for Error {}
