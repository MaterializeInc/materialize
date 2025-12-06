// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use std::error::Error;
use std::fmt::Display;

use bytes::BufMut;
use mz_expr::EvalError;
use mz_kafka_util::client::TunnelingClientContext;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_ssh_util::tunnel::SshTunnelStatus;
use proptest_derive::Arbitrary;
use prost::Message;
use rdkafka::error::KafkaError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;

include!(concat!(env!("OUT_DIR"), "/mz_storage_types.errors.rs"));

/// The underlying data was not decodable in the format we expected: eg.
/// invalid JSON or Avro data that doesn't match a schema.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DecodeError {
    pub kind: DecodeErrorKind,
    pub raw: Vec<u8>,
}

impl RustType<ProtoDecodeError> for DecodeError {
    fn into_proto(&self) -> ProtoDecodeError {
        ProtoDecodeError {
            kind: Some(RustType::into_proto(&self.kind)),
            raw: Some(self.raw.clone()),
        }
    }

    fn from_proto(proto: ProtoDecodeError) -> Result<Self, TryFromProtoError> {
        let kind = match proto.kind {
            Some(kind) => RustType::from_proto(kind)?,
            None => return Err(TryFromProtoError::missing_field("ProtoDecodeError::kind")),
        };
        let raw = proto.raw.into_rust_if_some("raw")?;
        Ok(Self { kind, raw })
    }
}

impl DecodeError {
    pub fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        self.into_proto()
            .encode(buf)
            .expect("no required fields means no initialization errors")
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        let proto = ProtoDecodeError::decode(buf).map_err(|err| err.to_string())?;
        proto.into_rust().map_err(|err| err.to_string())
    }
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // See if we can output the bytes that failed to decode as a string.
        let str_repr = std::str::from_utf8(&self.raw).ok();
        // strip any NUL characters from the str_repr to prevent an error
        // in the postgres protocol decoding
        let str_repr = str_repr.map(|s| s.replace('\0', "NULL"));
        let bytes_repr = hex::encode(&self.raw);
        match str_repr {
            Some(s) => write!(
                f,
                "{} (original text: {}, original bytes: {:x?})",
                self.kind, s, bytes_repr
            ),
            None => write!(f, "{} (original bytes: {:x?})", self.kind, bytes_repr),
        }
    }
}

#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum DecodeErrorKind {
    Text(Box<str>),
    Bytes(Box<str>),
}

impl RustType<ProtoDecodeErrorKind> for DecodeErrorKind {
    fn into_proto(&self) -> ProtoDecodeErrorKind {
        use proto_decode_error_kind::Kind::*;
        ProtoDecodeErrorKind {
            kind: Some(match self {
                DecodeErrorKind::Text(v) => Text(v.into_proto()),
                DecodeErrorKind::Bytes(v) => Bytes(v.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDecodeErrorKind) -> Result<Self, TryFromProtoError> {
        use proto_decode_error_kind::Kind::*;
        match proto.kind {
            Some(Text(v)) => Ok(DecodeErrorKind::Text(v.into())),
            Some(Bytes(v)) => Ok(DecodeErrorKind::Bytes(v.into())),
            None => Err(TryFromProtoError::missing_field("ProtoDecodeError::kind")),
        }
    }
}

impl Display for DecodeErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeErrorKind::Text(e) => f.write_str(e),
            DecodeErrorKind::Bytes(e) => f.write_str(e),
        }
    }
}

/// Errors arising during envelope processing.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EnvelopeError {
    /// An error that can be retracted by a future message using upsert logic.
    Upsert(UpsertError),
    /// Errors corresponding to `ENVELOPE NONE`. Naming this
    /// `None`, though, would have been too confusing.
    Flat(Box<str>),
}

impl RustType<ProtoEnvelopeErrorV1> for EnvelopeError {
    fn into_proto(&self) -> ProtoEnvelopeErrorV1 {
        use proto_envelope_error_v1::Kind;
        ProtoEnvelopeErrorV1 {
            kind: Some(match self {
                EnvelopeError::Upsert(rust) => Kind::Upsert(rust.into_proto()),
                EnvelopeError::Flat(text) => Kind::Flat(text.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoEnvelopeErrorV1) -> Result<Self, TryFromProtoError> {
        use proto_envelope_error_v1::Kind;
        match proto.kind {
            Some(Kind::Upsert(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::Upsert(rust))
            }
            Some(Kind::Flat(text)) => Ok(Self::Flat(text.into())),
            None => Err(TryFromProtoError::missing_field(
                "ProtoEnvelopeErrorV1::kind",
            )),
        }
    }
}

impl Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::Upsert(err) => write!(f, "Upsert: {err}"),
            EnvelopeError::Flat(err) => write!(f, "Flat: {err}"),
        }
    }
}

/// An error from a value in an upsert source. The corresponding key is included, allowing
/// us to reconstruct their entry in the upsert map upon restart.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct UpsertValueError {
    /// The underlying error.
    pub inner: DecodeError,
    /// The (good) key associated with the errored value.
    pub for_key: Row,
}

impl RustType<ProtoUpsertValueError> for UpsertValueError {
    fn into_proto(&self) -> ProtoUpsertValueError {
        ProtoUpsertValueError {
            inner: Some(self.inner.into_proto()),
            for_key: Some(self.for_key.into_proto()),
        }
    }

    fn from_proto(proto: ProtoUpsertValueError) -> Result<Self, TryFromProtoError> {
        Ok(UpsertValueError {
            inner: proto
                .inner
                .into_rust_if_some("ProtoUpsertValueError::inner")?,
            for_key: proto
                .for_key
                .into_rust_if_some("ProtoUpsertValueError::for_key")?,
        })
    }
}

impl Display for UpsertValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let UpsertValueError { inner, for_key } = self;
        write!(f, "{inner}, decoded key: {for_key:?}")?;
        Ok(())
    }
}

/// A source contained a record with a NULL key, which we don't support.
#[derive(
    Arbitrary, Ord, PartialOrd, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash,
)]
pub struct UpsertNullKeyError;

impl RustType<ProtoUpsertNullKeyError> for UpsertNullKeyError {
    fn into_proto(&self) -> ProtoUpsertNullKeyError {
        ProtoUpsertNullKeyError {}
    }

    fn from_proto(_proto: ProtoUpsertNullKeyError) -> Result<Self, TryFromProtoError> {
        Ok(UpsertNullKeyError)
    }
}

impl Display for UpsertNullKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "record with NULL key in UPSERT source; to retract this error, "
        )?;
        write!(
            f,
            "produce a record upstream with a NULL key and NULL value"
        )
    }
}

/// An error that can be retracted by a future message using upsert logic.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UpsertError {
    /// Wrapper around a key decoding error.
    /// We use this instead of emitting the underlying `DataflowError::DecodeError` because with only
    /// the underlying error, we can't distinguish between an error with the key and an error
    /// with the value.
    ///
    /// It is necessary to distinguish them because the necessary record to retract them is different.
    /// `(K, <errored V>)` is retracted by `(K, null)`, whereas `(<errored K>, anything)` is retracted by
    /// `("bytes", null)`, where "bytes" is the string that failed to correctly decode as a key.
    KeyDecode(DecodeError),
    /// Wrapper around an error related to the value.
    Value(UpsertValueError),
    NullKey(UpsertNullKeyError),
}

impl RustType<ProtoUpsertError> for UpsertError {
    fn into_proto(&self) -> ProtoUpsertError {
        use proto_upsert_error::Kind;
        ProtoUpsertError {
            kind: Some(match self {
                UpsertError::KeyDecode(err) => Kind::KeyDecode(err.into_proto()),
                UpsertError::Value(err) => Kind::Value(err.into_proto()),
                UpsertError::NullKey(err) => Kind::NullKey(err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoUpsertError) -> Result<Self, TryFromProtoError> {
        use proto_upsert_error::Kind;
        match proto.kind {
            Some(Kind::KeyDecode(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::KeyDecode(rust))
            }
            Some(Kind::Value(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::Value(rust))
            }
            Some(Kind::NullKey(proto)) => {
                let rust = RustType::from_proto(proto)?;
                Ok(Self::NullKey(rust))
            }
            None => Err(TryFromProtoError::missing_field("ProtoUpsertError::kind")),
        }
    }
}

impl Display for UpsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpsertError::KeyDecode(err) => write!(f, "Key decode: {err}"),
            UpsertError::Value(err) => write!(f, "Value error: {err}"),
            UpsertError::NullKey(err) => write!(f, "Null key: {err}"),
        }
    }
}

/// Source-wide durable errors; for example, a replication log being meaningless or corrupted.
/// This should _not_ include transient source errors, like connection issues or misconfigurations.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct SourceError {
    pub error: SourceErrorDetails,
}

impl RustType<ProtoSourceError> for SourceError {
    fn into_proto(&self) -> ProtoSourceError {
        ProtoSourceError {
            error: Some(self.error.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceError) -> Result<Self, TryFromProtoError> {
        Ok(SourceError {
            error: proto.error.into_rust_if_some("ProtoSourceError::error")?,
        })
    }
}

impl Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum SourceErrorDetails {
    Initialization(Box<str>),
    Other(Box<str>),
}

impl RustType<ProtoSourceErrorDetails> for SourceErrorDetails {
    fn into_proto(&self) -> ProtoSourceErrorDetails {
        use proto_source_error_details::Kind;
        ProtoSourceErrorDetails {
            kind: Some(match self {
                SourceErrorDetails::Initialization(s) => Kind::Initialization(s.into_proto()),
                SourceErrorDetails::Other(s) => Kind::Other(s.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceErrorDetails) -> Result<Self, TryFromProtoError> {
        use proto_source_error_details::Kind;
        match proto.kind {
            Some(kind) => match kind {
                Kind::Initialization(s) => Ok(SourceErrorDetails::Initialization(s.into())),
                Kind::DeprecatedFileIo(s) | Kind::DeprecatedPersistence(s) => {
                    warn!("Deprecated source error kind: {s}");
                    Ok(SourceErrorDetails::Other(s.into()))
                }
                Kind::Other(s) => Ok(SourceErrorDetails::Other(s.into())),
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
                    "failed during initialization, source must be dropped and recreated: {}",
                    e
                )
            }
            SourceErrorDetails::Other(e) => {
                write!(
                    f,
                    "source must be dropped and recreated due to failure: {}",
                    e
                )
            }
        }
    }
}

/// An error that's destined to be presented to the user in a differential dataflow collection.
/// For example, a divide by zero will be visible in the error collection for a particular row.
///
/// All of the variants are boxed to minimize the memory size of `DataflowError`. This type is
/// likely to appear in `Result<Row, DataflowError>`s on high-throughput code paths, so keeping its
/// size less than or equal to that of `Row` is important to ensure we are not wasting memory.
#[derive(Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum DataflowError {
    DecodeError(Box<DecodeError>),
    EvalError(Box<EvalError>),
    SourceError(Box<SourceError>),
    EnvelopeError(Box<EnvelopeError>),
}

impl Error for DataflowError {}

mod boxed_str {

    use differential_dataflow::containers::Region;
    use differential_dataflow::containers::StableRegion;

    /// Region allocation for `String` data.
    ///
    /// Content bytes are stored in stable contiguous memory locations,
    /// and then a `String` referencing them is falsified.
    #[derive(Default)]
    pub struct BoxStrStack {
        region: StableRegion<u8>,
    }

    impl Region for BoxStrStack {
        type Item = Box<str>;
        #[inline]
        fn clear(&mut self) {
            self.region.clear();
        }
        // Removing `(always)` is a 20% performance regression in
        // the `string10_copy` benchmark.
        #[inline(always)]
        unsafe fn copy(&mut self, item: &Box<str>) -> Box<str> {
            let bytes = self.region.copy_slice(item.as_bytes());
            // SAFETY: The bytes are copied from the region, and the region is stable.
            // We never drop the box.
            std::str::from_boxed_utf8_unchecked(Box::from_raw(bytes))
        }
        #[inline(always)]
        fn reserve_items<'a, I>(&mut self, items: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self::Item> + Clone,
        {
            self.region.reserve(items.map(|x| x.len()).sum());
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.region.reserve(regions.map(|r| r.region.len()).sum());
        }
        #[inline]
        fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            self.region.heap_size(callback)
        }
    }
}

mod columnation {
    use std::iter::once;

    use differential_dataflow::containers::{Columnation, Region, StableRegion};
    use mz_expr::EvalError;
    use mz_repr::Row;
    use mz_repr::adt::range::InvalidRangeError;
    use mz_repr::strconv::ParseError;

    use crate::errors::boxed_str::BoxStrStack;
    use crate::errors::{
        DataflowError, DecodeError, DecodeErrorKind, EnvelopeError, SourceError,
        SourceErrorDetails, UpsertError, UpsertValueError,
    };

    impl Columnation for DataflowError {
        type InnerRegion = DataflowErrorRegion;
    }

    /// A region to store [`DataflowError`].
    #[derive(Default)]
    pub struct DataflowErrorRegion {
        /// Stable location for [`DecodeError`] for inserting into a box.
        decode_error_region: StableRegion<DecodeError>,
        /// Stable location for [`EnvelopeError`] for inserting into a box.
        envelope_error_region: StableRegion<EnvelopeError>,
        /// Stable location for [`EvalError`] for inserting into a box.
        eval_error_region: StableRegion<EvalError>,
        /// Region for storing rows.
        row_region: <Row as Columnation>::InnerRegion,
        /// Stable location for [`SourceError`] for inserting into a box.
        source_error_region: StableRegion<SourceError>,
        /// Region for storing strings.
        string_region: BoxStrStack,
        /// Region for storing u8 vectors.
        u8_region: <Vec<u8> as Columnation>::InnerRegion,
    }

    impl DataflowErrorRegion {
        /// Copy a decode error into its region, return an owned object.
        ///
        /// This is unsafe because the returned value must not be dropped.
        unsafe fn copy_decode_error(&mut self, decode_error: &DecodeError) -> DecodeError {
            DecodeError {
                kind: match &decode_error.kind {
                    DecodeErrorKind::Text(string) => {
                        DecodeErrorKind::Text(self.string_region.copy(string))
                    }
                    DecodeErrorKind::Bytes(string) => {
                        DecodeErrorKind::Bytes(self.string_region.copy(string))
                    }
                },
                raw: self.u8_region.copy(&decode_error.raw),
            }
        }
    }

    /// Compile-time assertion that a value is `Copy`.
    fn assert_copy<T: Copy>(_: &T) {}

    impl Region for DataflowErrorRegion {
        type Item = DataflowError;

        unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item {
            // Unsafe Box::from_raw reasoning:
            // Construct a box from a provided value. This is safe because a box is
            // a pointer to a memory address, and the value is stored on the heap.
            // Note that the box must not be dropped.

            // SAFETY: When adding new enum variants, care must be taken that all types containing
            // references are region-allocated, otherwise we'll leak memory.

            // Types that are `Copy` should be asserted using `assert_copy`, or copied, to detect
            // changes that introduce pointers.

            let err = match item {
                DataflowError::DecodeError(err) => {
                    let err = self.copy_decode_error(&*err);
                    let reference = self.decode_error_region.copy_iter(once(err));
                    let boxed = unsafe { Box::from_raw(reference.as_mut_ptr()) };
                    DataflowError::DecodeError(boxed)
                }
                DataflowError::EvalError(err) => {
                    let err: &EvalError = &*err;
                    let err = match err {
                        e @ EvalError::CharacterNotValidForEncoding(x) => {
                            assert_copy(x);
                            e.clone()
                        }
                        e @ EvalError::CharacterTooLargeForEncoding(x) => {
                            assert_copy(x);
                            e.clone()
                        }
                        EvalError::DateBinOutOfRange(string) => {
                            EvalError::DateBinOutOfRange(self.string_region.copy(string))
                        }
                        e @ EvalError::DivisionByZero
                        | e @ EvalError::FloatOverflow
                        | e @ EvalError::FloatUnderflow
                        | e @ EvalError::NumericFieldOverflow
                        | e @ EvalError::MzTimestampStepOverflow
                        | e @ EvalError::TimestampCannotBeNan
                        | e @ EvalError::TimestampOutOfRange
                        | e @ EvalError::NegSqrt
                        | e @ EvalError::NegLimit
                        | e @ EvalError::NullCharacterNotPermitted
                        | e @ EvalError::KeyCannotBeNull
                        | e @ EvalError::UnterminatedLikeEscapeSequence
                        | e @ EvalError::MultipleRowsFromSubquery
                        | e @ EvalError::LikePatternTooLong
                        | e @ EvalError::LikeEscapeTooLong
                        | e @ EvalError::MultidimensionalArrayRemovalNotSupported
                        | e @ EvalError::MultiDimensionalArraySearch
                        | e @ EvalError::ArrayFillWrongArraySubscripts
                        | e @ EvalError::DateOutOfRange
                        | e @ EvalError::CharOutOfRange
                        | e @ EvalError::InvalidBase64Equals
                        | e @ EvalError::InvalidBase64EndSequence
                        | e @ EvalError::InvalidTimezoneInterval
                        | e @ EvalError::InvalidTimezoneConversion
                        | e @ EvalError::LengthTooLarge
                        | e @ EvalError::AclArrayNullElement
                        | e @ EvalError::MzAclArrayNullElement => e.clone(),
                        EvalError::Unsupported {
                            feature,
                            discussion_no,
                        } => EvalError::Unsupported {
                            feature: self.string_region.copy(feature),
                            discussion_no: *discussion_no,
                        },
                        EvalError::Float32OutOfRange(string) => {
                            EvalError::Float32OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::Float64OutOfRange(string) => {
                            EvalError::Float64OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::Int16OutOfRange(string) => {
                            EvalError::Int16OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::Int32OutOfRange(string) => {
                            EvalError::Int32OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::Int64OutOfRange(string) => {
                            EvalError::Int64OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::UInt16OutOfRange(string) => {
                            EvalError::UInt16OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::UInt32OutOfRange(string) => {
                            EvalError::UInt32OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::UInt64OutOfRange(string) => {
                            EvalError::UInt64OutOfRange(self.string_region.copy(string))
                        }
                        EvalError::MzTimestampOutOfRange(string) => {
                            EvalError::MzTimestampOutOfRange(self.string_region.copy(string))
                        }
                        EvalError::OidOutOfRange(string) => {
                            EvalError::OidOutOfRange(self.string_region.copy(string))
                        }
                        EvalError::IntervalOutOfRange(string) => {
                            EvalError::IntervalOutOfRange(self.string_region.copy(string))
                        }
                        e @ EvalError::IndexOutOfRange {
                            provided,
                            valid_end,
                        } => {
                            assert_copy(provided);
                            assert_copy(valid_end);
                            e.clone()
                        }
                        e @ EvalError::InvalidBase64Symbol(c) => {
                            assert_copy(c);
                            e.clone()
                        }
                        EvalError::InvalidTimezone(x) => {
                            EvalError::InvalidTimezone(self.string_region.copy(x))
                        }
                        e @ EvalError::InvalidLayer { max_layer, val } => {
                            assert_copy(max_layer);
                            assert_copy(val);
                            e.clone()
                        }
                        EvalError::InvalidArray(err) => EvalError::InvalidArray(*err),
                        EvalError::InvalidEncodingName(x) => {
                            EvalError::InvalidEncodingName(self.string_region.copy(x))
                        }
                        EvalError::InvalidHashAlgorithm(x) => {
                            EvalError::InvalidHashAlgorithm(self.string_region.copy(x))
                        }
                        EvalError::InvalidByteSequence {
                            byte_sequence,
                            encoding_name,
                        } => EvalError::InvalidByteSequence {
                            byte_sequence: self.string_region.copy(byte_sequence),
                            encoding_name: self.string_region.copy(encoding_name),
                        },
                        EvalError::InvalidJsonbCast { from, to } => EvalError::InvalidJsonbCast {
                            from: self.string_region.copy(from),
                            to: self.string_region.copy(to),
                        },
                        EvalError::InvalidRegex(x) => {
                            EvalError::InvalidRegex(self.string_region.copy(x))
                        }
                        e @ EvalError::InvalidRegexFlag(x) => {
                            assert_copy(x);
                            e.clone()
                        }
                        EvalError::InvalidParameterValue(x) => {
                            EvalError::InvalidParameterValue(self.string_region.copy(x))
                        }
                        EvalError::InvalidDatePart(x) => {
                            EvalError::InvalidDatePart(self.string_region.copy(x))
                        }
                        EvalError::UnknownUnits(x) => {
                            EvalError::UnknownUnits(self.string_region.copy(x))
                        }
                        EvalError::UnsupportedUnits(x, y) => EvalError::UnsupportedUnits(
                            self.string_region.copy(x),
                            self.string_region.copy(y),
                        ),
                        EvalError::Parse(ParseError {
                            kind,
                            type_name,
                            input,
                            details,
                        }) => EvalError::Parse(ParseError {
                            kind: *kind,
                            type_name: self.string_region.copy(type_name),
                            input: self.string_region.copy(input),
                            details: details
                                .as_ref()
                                .map(|details| self.string_region.copy(details)),
                        }),
                        e @ EvalError::ParseHex(x) => {
                            assert_copy(x);
                            e.clone()
                        }
                        EvalError::Internal(x) => EvalError::Internal(self.string_region.copy(x)),
                        EvalError::InfinityOutOfDomain(x) => {
                            EvalError::InfinityOutOfDomain(self.string_region.copy(x))
                        }
                        EvalError::NegativeOutOfDomain(x) => {
                            EvalError::NegativeOutOfDomain(self.string_region.copy(x))
                        }
                        EvalError::ZeroOutOfDomain(x) => {
                            EvalError::ZeroOutOfDomain(self.string_region.copy(x))
                        }
                        EvalError::OutOfDomain(x, y, z) => {
                            assert_copy(x);
                            assert_copy(y);
                            EvalError::OutOfDomain(*x, *y, self.string_region.copy(z))
                        }
                        EvalError::ComplexOutOfRange(x) => {
                            EvalError::ComplexOutOfRange(self.string_region.copy(x))
                        }
                        EvalError::Undefined(x) => EvalError::Undefined(self.string_region.copy(x)),
                        EvalError::StringValueTooLong {
                            target_type,
                            length,
                        } => EvalError::StringValueTooLong {
                            target_type: self.string_region.copy(target_type),
                            length: *length,
                        },
                        e @ EvalError::IncompatibleArrayDimensions { dims } => {
                            assert_copy(dims);
                            e.clone()
                        }
                        EvalError::TypeFromOid(x) => {
                            EvalError::TypeFromOid(self.string_region.copy(x))
                        }
                        EvalError::InvalidRange(x) => {
                            let err = match x {
                                e @ InvalidRangeError::MisorderedRangeBounds
                                | e @ InvalidRangeError::InvalidRangeBoundFlags
                                | e @ InvalidRangeError::DiscontiguousUnion
                                | e @ InvalidRangeError::DiscontiguousDifference
                                | e @ InvalidRangeError::NullRangeBoundFlags => e.clone(),
                                InvalidRangeError::CanonicalizationOverflow(string) => {
                                    InvalidRangeError::CanonicalizationOverflow(
                                        self.string_region.copy(string),
                                    )
                                }
                            };
                            EvalError::InvalidRange(err)
                        }
                        EvalError::InvalidRoleId(x) => {
                            EvalError::InvalidRoleId(self.string_region.copy(x))
                        }
                        EvalError::InvalidPrivileges(x) => {
                            EvalError::InvalidPrivileges(self.string_region.copy(x))
                        }
                        EvalError::LetRecLimitExceeded(x) => {
                            EvalError::LetRecLimitExceeded(self.string_region.copy(x))
                        }
                        EvalError::MustNotBeNull(x) => {
                            EvalError::MustNotBeNull(self.string_region.copy(x))
                        }
                        EvalError::InvalidIdentifier { ident, detail } => {
                            EvalError::InvalidIdentifier {
                                ident: self.string_region.copy(ident),
                                detail: detail
                                    .as_ref()
                                    .map(|detail| self.string_region.copy(detail)),
                            }
                        }
                        e @ EvalError::MaxArraySizeExceeded(x) => {
                            assert_copy(x);
                            e.clone()
                        }
                        EvalError::DateDiffOverflow { unit, a, b } => EvalError::DateDiffOverflow {
                            unit: self.string_region.copy(unit),
                            a: self.string_region.copy(a),
                            b: self.string_region.copy(b),
                        },
                        EvalError::IfNullError(x) => {
                            EvalError::IfNullError(self.string_region.copy(x))
                        }
                        EvalError::InvalidIanaTimezoneId(x) => {
                            EvalError::InvalidIanaTimezoneId(self.string_region.copy(x))
                        }
                        EvalError::PrettyError(x) => {
                            EvalError::PrettyError(self.string_region.copy(x))
                        }
                    };
                    let reference = self.eval_error_region.copy_iter(once(err));
                    let boxed = unsafe { Box::from_raw(reference.as_mut_ptr()) };
                    DataflowError::EvalError(boxed)
                }
                DataflowError::SourceError(err) => {
                    let err: &SourceError = &*err;
                    let err = SourceError {
                        error: match &err.error {
                            SourceErrorDetails::Initialization(string) => {
                                SourceErrorDetails::Initialization(self.string_region.copy(string))
                            }
                            SourceErrorDetails::Other(string) => {
                                SourceErrorDetails::Other(self.string_region.copy(string))
                            }
                        },
                    };
                    let reference = self.source_error_region.copy_iter(once(err));
                    let boxed = unsafe { Box::from_raw(reference.as_mut_ptr()) };
                    DataflowError::SourceError(boxed)
                }
                DataflowError::EnvelopeError(err) => {
                    let err: &EnvelopeError = &*err;
                    let err = match err {
                        EnvelopeError::Upsert(err) => {
                            let err = match err {
                                UpsertError::KeyDecode(err) => {
                                    UpsertError::KeyDecode(self.copy_decode_error(err))
                                }
                                UpsertError::Value(err) => UpsertError::Value(UpsertValueError {
                                    inner: self.copy_decode_error(&err.inner),
                                    for_key: self.row_region.copy(&err.for_key),
                                }),
                                UpsertError::NullKey(err) => UpsertError::NullKey(*err),
                            };
                            EnvelopeError::Upsert(err)
                        }
                        EnvelopeError::Flat(string) => {
                            EnvelopeError::Flat(self.string_region.copy(string))
                        }
                    };
                    let reference = self.envelope_error_region.copy_iter(once(err));
                    let boxed = unsafe { Box::from_raw(reference.as_mut_ptr()) };
                    DataflowError::EnvelopeError(boxed)
                }
            };
            // Debug-only check that we're returning an equal object.
            debug_assert_eq!(item, &err);
            err
        }

        fn clear(&mut self) {
            // De-structure `self` to make sure we're clearing all regions.
            let Self {
                decode_error_region,
                envelope_error_region,
                eval_error_region,
                row_region,
                source_error_region,
                string_region,
                u8_region,
            } = self;
            decode_error_region.clear();
            envelope_error_region.clear();
            eval_error_region.clear();
            row_region.clear();
            source_error_region.clear();
            string_region.clear();
            u8_region.clear();
        }

        fn reserve_items<'a, I>(&mut self, items: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self::Item> + Clone,
        {
            // Reserve space on all stable regions.
            self.decode_error_region.reserve(
                items
                    .clone()
                    .filter(|x| matches!(x, DataflowError::DecodeError(_)))
                    .count(),
            );
            self.envelope_error_region.reserve(
                items
                    .clone()
                    .filter(|x| matches!(x, DataflowError::EnvelopeError(_)))
                    .count(),
            );
            self.eval_error_region.reserve(
                items
                    .clone()
                    .filter(|x| matches!(x, DataflowError::EvalError(_)))
                    .count(),
            );
            self.source_error_region.reserve(
                items
                    .filter(|x| matches!(x, DataflowError::SourceError(_)))
                    .count(),
            );
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            // Reserve space on all region allocators.
            self.row_region
                .reserve_regions(regions.clone().map(|r| &r.row_region));
            self.string_region
                .reserve_regions(regions.clone().map(|r| &r.string_region));
            self.u8_region
                .reserve_regions(regions.clone().map(|r| &r.u8_region));
        }

        fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
            // De-structure `self` to make sure we're counting all regions.
            let Self {
                decode_error_region,
                envelope_error_region,
                eval_error_region,
                row_region,
                source_error_region,
                string_region,
                u8_region,
            } = &self;
            decode_error_region.heap_size(&mut callback);
            envelope_error_region.heap_size(&mut callback);
            eval_error_region.heap_size(&mut callback);
            row_region.heap_size(&mut callback);
            source_error_region.heap_size(&mut callback);
            string_region.heap_size(&mut callback);
            u8_region.heap_size(&mut callback);
        }
    }

    #[cfg(test)]
    mod tests {
        use differential_dataflow::containers::TimelyStack;
        use proptest::prelude::*;

        use super::*;

        fn columnation_roundtrip<T: Columnation>(item: &T) -> TimelyStack<T> {
            let mut container = TimelyStack::with_capacity(1);
            container.copy(item);
            container
        }

        proptest! {
            #[mz_ore::test]
            #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
            fn dataflow_error_roundtrip(expect in any::<DataflowError>()) {
                let actual = columnation_roundtrip(&expect);
                proptest::prop_assert_eq!(&expect, &actual[0])
            }
        }
    }
}

impl RustType<ProtoDataflowError> for DataflowError {
    fn into_proto(&self) -> ProtoDataflowError {
        use proto_dataflow_error::Kind::*;
        ProtoDataflowError {
            kind: Some(match self {
                DataflowError::DecodeError(err) => DecodeError(*err.into_proto()),
                DataflowError::EvalError(err) => EvalError(*err.into_proto()),
                DataflowError::SourceError(err) => SourceError(*err.into_proto()),
                DataflowError::EnvelopeError(err) => EnvelopeErrorV1(*err.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoDataflowError) -> Result<Self, TryFromProtoError> {
        use proto_dataflow_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                DecodeError(err) => Ok(DataflowError::DecodeError(Box::new(err.into_rust()?))),
                EvalError(err) => Ok(DataflowError::EvalError(Box::new(err.into_rust()?))),
                SourceError(err) => Ok(DataflowError::SourceError(Box::new(err.into_rust()?))),
                EnvelopeErrorV1(err) => {
                    Ok(DataflowError::EnvelopeError(Box::new(err.into_rust()?)))
                }
            },
            None => Err(TryFromProtoError::missing_field("ProtoDataflowError::kind")),
        }
    }
}

impl Display for DataflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataflowError::DecodeError(e) => write!(f, "Decode error: {}", e),
            DataflowError::EvalError(e) => write!(
                f,
                "{}{}",
                match **e {
                    EvalError::IfNullError(_) => "",
                    _ => "Evaluation error: ",
                },
                e
            ),
            DataflowError::SourceError(e) => write!(f, "Source error: {}", e),
            DataflowError::EnvelopeError(e) => write!(f, "Envelope error: {}", e),
        }
    }
}

impl From<DecodeError> for DataflowError {
    fn from(e: DecodeError) -> Self {
        Self::DecodeError(Box::new(e))
    }
}

impl From<EvalError> for DataflowError {
    fn from(e: EvalError) -> Self {
        Self::EvalError(Box::new(e))
    }
}

impl From<SourceError> for DataflowError {
    fn from(e: SourceError) -> Self {
        Self::SourceError(Box::new(e))
    }
}

impl From<EnvelopeError> for DataflowError {
    fn from(e: EnvelopeError) -> Self {
        Self::EnvelopeError(Box::new(e))
    }
}

/// An error returned by `KafkaConnection::create_with_context`.
#[derive(thiserror::Error, Debug)]
pub enum ContextCreationError {
    // This ends up double-printing `ssh:` in status tables, but makes for
    // a better experience during ddl.
    #[error("ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    #[error(transparent)]
    KafkaError(#[from] KafkaError),
    #[error(transparent)]
    Dns(#[from] mz_ore::netio::DnsResolutionError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// An extension trait for `Result<T, E>` that makes producing `ContextCreationError`s easier.
pub trait ContextCreationErrorExt<T> {
    /// Override the error case with an ssh error from `cx`, if there is one.
    fn check_ssh_status<C>(self, cx: &TunnelingClientContext<C>)
    -> Result<T, ContextCreationError>;
    /// Add context to the errors within the variants of `ContextCreationError`, without
    /// altering the `Ssh` variant.
    fn add_context(self, msg: &'static str) -> Result<T, ContextCreationError>;
}

impl<T, E> ContextCreationErrorExt<T> for Result<T, E>
where
    ContextCreationError: From<E>,
{
    fn check_ssh_status<C>(
        self,
        cx: &TunnelingClientContext<C>,
    ) -> Result<T, ContextCreationError> {
        self.map_err(|e| {
            if let SshTunnelStatus::Errored(e) = cx.tunnel_status() {
                ContextCreationError::Ssh(anyhow!(e))
            } else {
                ContextCreationError::from(e)
            }
        })
    }

    fn add_context(self, msg: &'static str) -> Result<T, ContextCreationError> {
        self.map_err(|e| {
            let e = ContextCreationError::from(e);
            match e {
                // We need to preserve the `Ssh` variant here, so we add the context inside of it.
                ContextCreationError::Ssh(e) => ContextCreationError::Ssh(anyhow!(e.context(msg))),
                ContextCreationError::Other(e) => {
                    ContextCreationError::Other(anyhow!(e.context(msg)))
                }
                ContextCreationError::KafkaError(e) => {
                    ContextCreationError::Other(anyhow!(anyhow!(e).context(msg)))
                }
                ContextCreationError::Io(e) => {
                    ContextCreationError::Other(anyhow!(anyhow!(e).context(msg)))
                }
                ContextCreationError::Dns(e) => {
                    ContextCreationError::Other(anyhow!(e).context(msg))
                }
            }
        })
    }
}

impl From<CsrConnectError> for ContextCreationError {
    fn from(csr: CsrConnectError) -> ContextCreationError {
        use ContextCreationError::*;

        match csr {
            CsrConnectError::Ssh(e) => Ssh(e),
            other => Other(anyhow::anyhow!(other)),
        }
    }
}

/// An error returned by `CsrConnection::connect`.
#[derive(thiserror::Error, Debug)]
pub enum CsrConnectError {
    // This ends up double-printing `ssh:` in status tables, but makes for
    // a better experience during ddl.
    #[error("ssh: {0}")]
    Ssh(#[source] anyhow::Error),
    #[error(transparent)]
    NativeTls(#[from] native_tls::Error),
    #[error(transparent)]
    Openssl(#[from] openssl::error::ErrorStack),
    #[error(transparent)]
    Dns(#[from] mz_ore::netio::DnsResolutionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Error returned in response to a reference to an unknown collection.
#[derive(Error, Debug)]
#[error("collection does not exist: {0}")]
pub struct CollectionMissing(pub GlobalId);

#[derive(Error, Debug)]
pub enum CollectionMissingOrUnreadable {
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("collection has an empty read frontier: {0}")]
    CollectionUnreadable(GlobalId),
}

impl From<CollectionMissing> for CollectionMissingOrUnreadable {
    fn from(cm: CollectionMissing) -> Self {
        CollectionMissingOrUnreadable::CollectionMissing(cm.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::DecodeErrorKind;

    use super::DecodeError;

    #[mz_ore::test]
    fn test_decode_error_codec_roundtrip() -> Result<(), String> {
        let original = DecodeError {
            kind: DecodeErrorKind::Text("ciao".into()),
            raw: b"oaic".to_vec(),
        };
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded = DecodeError::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }
}
