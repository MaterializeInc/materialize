// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

use std::borrow::Cow;
use std::char::CharTryFromError;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU64, TryFromIntError};
use std::sync::Arc;

use mz_ore::Overflowing;
use mz_ore::cast::CastFrom;
use mz_ore::num::{NonNeg, NonNegError};
use num::Signed;
use proptest::prelude::Strategy;
use prost::UnknownEnumValue;

#[cfg(feature = "chrono")]
pub mod chrono;

include!(concat!(env!("OUT_DIR"), "/mz_proto.rs"));

/// An error thrown when trying to convert from a `*.proto`-generated type
/// `Proto$T` to `$T`.
#[derive(Debug)]
pub enum TryFromProtoError {
    /// A wrapped [`TryFromIntError`] due to failed integer downcast.
    TryFromIntError(TryFromIntError),
    /// A wrapped [`NonNegError`] due to non-negative invariant being violated.
    NonNegError(NonNegError),
    /// A wrapped [`CharTryFromError`] due to failed [`char`] conversion.
    CharTryFromError(CharTryFromError),
    /// A date conversion failed
    DateConversionError(String),
    /// A regex compilation failed
    RegexError(regex::Error),
    /// A mz_repr::Row conversion failed
    RowConversionError(String),
    /// A JSON deserialization failed.
    /// TODO: Remove this when we have complete coverage for source and sink structs.
    DeserializationError(serde_json::Error),
    /// Indicates an `Option<U>` field in the `Proto$T` that should be set,
    /// but for some reason it is not. In practice this should never occur.
    MissingField(String),
    /// Indicates an unknown enum variant in `Proto$T`.
    UnknownEnumVariant(String),
    /// Indicates that the serialized ShardId value failed to deserialize, according
    /// to its custom deserialization logic.
    InvalidShardId(String),
    /// Indicates that the serialized persist state declared a codec different
    /// than the one declared in the state.
    CodecMismatch(String),
    /// Indicates that the serialized persist state being decoded was internally inconsistent.
    InvalidPersistState(String),
    /// Failed to parse a semver::Version.
    InvalidSemverVersion(String),
    /// Failed to parse a serialized URI
    InvalidUri(http::uri::InvalidUri),
    /// Failed to read back a serialized Glob
    GlobError(globset::Error),
    /// Failed to parse a serialized URL
    InvalidUrl(url::ParseError),
    /// Failed to parse bitflags.
    InvalidBitFlags(String),
    /// Failed to deserialize a LIKE/ILIKE pattern.
    LikePatternDeserializationError(String),
    /// A field represented invalid semantics.
    InvalidFieldError(String),
}

impl TryFromProtoError {
    /// Construct a new [`TryFromProtoError::MissingField`] instance.
    pub fn missing_field<T: ToString>(s: T) -> TryFromProtoError {
        TryFromProtoError::MissingField(s.to_string())
    }

    /// Construct a new [`TryFromProtoError::UnknownEnumVariant`] instance.
    pub fn unknown_enum_variant<T: ToString>(s: T) -> TryFromProtoError {
        TryFromProtoError::UnknownEnumVariant(s.to_string())
    }
}

impl From<TryFromIntError> for TryFromProtoError {
    fn from(error: TryFromIntError) -> Self {
        TryFromProtoError::TryFromIntError(error)
    }
}

impl From<NonNegError> for TryFromProtoError {
    fn from(error: NonNegError) -> Self {
        TryFromProtoError::NonNegError(error)
    }
}

impl From<CharTryFromError> for TryFromProtoError {
    fn from(error: CharTryFromError) -> Self {
        TryFromProtoError::CharTryFromError(error)
    }
}

impl From<UnknownEnumValue> for TryFromProtoError {
    fn from(UnknownEnumValue(n): UnknownEnumValue) -> Self {
        TryFromProtoError::UnknownEnumVariant(format!("value {n}"))
    }
}

// These From impls pull a bunch of deps into this crate that are otherwise
// unnecessary. Are they worth it?

impl From<regex::Error> for TryFromProtoError {
    fn from(error: regex::Error) -> Self {
        TryFromProtoError::RegexError(error)
    }
}

impl From<serde_json::Error> for TryFromProtoError {
    fn from(error: serde_json::Error) -> Self {
        TryFromProtoError::DeserializationError(error)
    }
}

impl From<http::uri::InvalidUri> for TryFromProtoError {
    fn from(error: http::uri::InvalidUri) -> Self {
        TryFromProtoError::InvalidUri(error)
    }
}

impl From<globset::Error> for TryFromProtoError {
    fn from(error: globset::Error) -> Self {
        TryFromProtoError::GlobError(error)
    }
}

impl From<url::ParseError> for TryFromProtoError {
    fn from(error: url::ParseError) -> Self {
        TryFromProtoError::InvalidUrl(error)
    }
}

impl std::fmt::Display for TryFromProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => error.fmt(f),
            NonNegError(error) => error.fmt(f),
            CharTryFromError(error) => error.fmt(f),
            DateConversionError(msg) => write!(f, "Date conversion failed: `{}`", msg),
            RegexError(error) => error.fmt(f),
            DeserializationError(error) => error.fmt(f),
            RowConversionError(msg) => write!(f, "Row packing failed: `{}`", msg),
            MissingField(field) => write!(f, "Missing value for `{}`", field),
            UnknownEnumVariant(field) => write!(f, "Unknown enum value for `{}`", field),
            InvalidShardId(value) => write!(f, "Invalid value of ShardId found: `{}`", value),
            CodecMismatch(error) => error.fmt(f),
            InvalidPersistState(error) => error.fmt(f),
            InvalidSemverVersion(error) => error.fmt(f),
            InvalidUri(error) => error.fmt(f),
            GlobError(error) => error.fmt(f),
            InvalidUrl(error) => error.fmt(f),
            InvalidBitFlags(error) => error.fmt(f),
            LikePatternDeserializationError(inner_error) => write!(
                f,
                "Protobuf deserialization failed for a LIKE/ILIKE pattern: `{}`",
                inner_error
            ),
            InvalidFieldError(error) => error.fmt(f),
        }
    }
}

/// Allow `?` operator on `Result<_, TryFromProtoError>` in contexts
/// where the error type is a `String`.
impl From<TryFromProtoError> for String {
    fn from(error: TryFromProtoError) -> Self {
        error.to_string()
    }
}

impl std::error::Error for TryFromProtoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => Some(error),
            NonNegError(error) => Some(error),
            CharTryFromError(error) => Some(error),
            RegexError(error) => Some(error),
            DeserializationError(error) => Some(error),
            DateConversionError(_) => None,
            RowConversionError(_) => None,
            MissingField(_) => None,
            UnknownEnumVariant(_) => None,
            InvalidShardId(_) => None,
            CodecMismatch(_) => None,
            InvalidPersistState(_) => None,
            InvalidSemverVersion(_) => None,
            InvalidUri(error) => Some(error),
            GlobError(error) => Some(error),
            InvalidUrl(error) => Some(error),
            InvalidBitFlags(_) => None,
            LikePatternDeserializationError(_) => None,
            InvalidFieldError(_) => None,
        }
    }
}

/// A trait that declares that `Self::Proto` is the default
/// Protobuf representation for `Self`.
pub trait ProtoRepr: Sized + RustType<Self::Proto> {
    type Proto: ::prost::Message;
}

/// A trait for representing a Rust type `Self` as a value of
/// type `Proto` for the purpose of serializing this
/// value as (part of) a Protobuf message.
///
/// To encode a value, use [`RustType::into_proto()`] (which
/// should always be an infallible conversion).
///
/// To decode a value, use the fallible [`RustType::from_proto()`].
/// Since the representation type can be "bigger" than the original,
/// decoding may fail, indicated by returning a [`TryFromProtoError`]
/// wrapped in a [`Result::Err`].
///
/// Convenience syntax for the above methods is available from the
/// matching [`ProtoType`].
pub trait RustType<Proto>: Sized {
    /// Convert a `Self` into a `Proto` value.
    fn into_proto(&self) -> Proto;

    /// A zero clone version of [`Self::into_proto`] that types can
    /// optionally implement, otherwise, the default implementation
    /// delegates to [`Self::into_proto`].
    fn into_proto_owned(self) -> Proto {
        self.into_proto()
    }

    /// Consume and convert a `Proto` back into a `Self` value.
    ///
    /// Since `Proto` can be "bigger" than the original, this
    /// may fail, indicated by returning a [`TryFromProtoError`]
    /// wrapped in a [`Result::Err`].
    fn from_proto(proto: Proto) -> Result<Self, TryFromProtoError>;
}

/// A trait that allows `Self` to be used as an entry in a
/// `Vec<Self>` representing a Rust `*Map<K, V>`.
pub trait ProtoMapEntry<K, V> {
    fn from_rust<'a>(entry: (&'a K, &'a V)) -> Self;
    fn into_rust(self) -> Result<(K, V), TryFromProtoError>;
}

macro_rules! rust_type_id(
    ($($t:ty),*) => (
        $(
            /// Identity type for $t.
            impl RustType<$t> for $t {
                #[inline]
                fn into_proto(&self) -> $t {
                    self.clone()
                }

                #[inline]
                fn from_proto(proto: $t) -> Result<Self, TryFromProtoError> {
                    Ok(proto)
                }
            }
        )+
    );
);

rust_type_id![bool, f32, f64, i32, i64, String, u32, u64, Vec<u8>];

impl RustType<u64> for Option<NonZeroU64> {
    fn into_proto(&self) -> u64 {
        match self {
            Some(d) => d.get(),
            None => 0,
        }
    }

    fn from_proto(proto: u64) -> Result<Self, TryFromProtoError> {
        Ok(NonZeroU64::new(proto)) // 0 is correctly mapped to None
    }
}

impl RustType<i64> for Overflowing<i64> {
    #[inline(always)]
    fn into_proto(&self) -> i64 {
        self.into_inner()
    }

    #[inline(always)]
    fn from_proto(proto: i64) -> Result<Self, TryFromProtoError> {
        Ok(proto.into())
    }
}

/// Blanket implementation for `BTreeMap<K, V>` where there exists `T` such
/// that `T` implements `ProtoMapEntry<K, V>`.
impl<K, V, T> RustType<Vec<T>> for BTreeMap<K, V>
where
    K: std::cmp::Eq + std::cmp::Ord,
    T: ProtoMapEntry<K, V>,
{
    fn into_proto(&self) -> Vec<T> {
        self.iter().map(T::from_rust).collect()
    }

    fn from_proto(proto: Vec<T>) -> Result<Self, TryFromProtoError> {
        proto
            .into_iter()
            .map(T::into_rust)
            .collect::<Result<BTreeMap<_, _>, _>>()
    }
}

/// Blanket implementation for `BTreeSet<R>` where `R` is a [`RustType`].
impl<R, P> RustType<Vec<P>> for BTreeSet<R>
where
    R: RustType<P> + std::cmp::Ord,
{
    fn into_proto(&self) -> Vec<P> {
        self.iter().map(R::into_proto).collect()
    }

    fn from_proto(proto: Vec<P>) -> Result<Self, TryFromProtoError> {
        proto
            .into_iter()
            .map(R::from_proto)
            .collect::<Result<BTreeSet<_>, _>>()
    }
}

/// Blanket implementation for `Vec<R>` where `R` is a [`RustType`].
impl<R, P> RustType<Vec<P>> for Vec<R>
where
    R: RustType<P>,
{
    fn into_proto(&self) -> Vec<P> {
        self.iter().map(R::into_proto).collect()
    }

    fn from_proto(proto: Vec<P>) -> Result<Self, TryFromProtoError> {
        proto.into_iter().map(R::from_proto).collect()
    }
}

/// Blanket implementation for `Box<[R]>` where `R` is a [`RustType`].
impl<R, P> RustType<Vec<P>> for Box<[R]>
where
    R: RustType<P>,
{
    fn into_proto(&self) -> Vec<P> {
        self.iter().map(R::into_proto).collect()
    }

    fn from_proto(proto: Vec<P>) -> Result<Self, TryFromProtoError> {
        proto.into_iter().map(R::from_proto).collect()
    }
}

/// Blanket implementation for `Option<R>` where `R` is a [`RustType`].
impl<R, P> RustType<Option<P>> for Option<R>
where
    R: RustType<P>,
{
    fn into_proto(&self) -> Option<P> {
        self.as_ref().map(R::into_proto)
    }

    fn from_proto(proto: Option<P>) -> Result<Self, TryFromProtoError> {
        proto.map(R::from_proto).transpose()
    }
}

/// Blanket implementation for `Box<R>` where `R` is a [`RustType`].
impl<R, P> RustType<Box<P>> for Box<R>
where
    R: RustType<P>,
{
    fn into_proto(&self) -> Box<P> {
        Box::new((**self).into_proto())
    }

    fn from_proto(proto: Box<P>) -> Result<Self, TryFromProtoError> {
        (*proto).into_rust().map(Box::new)
    }
}

impl<R, P> RustType<P> for Arc<R>
where
    R: RustType<P>,
{
    fn into_proto(&self) -> P {
        (**self).into_proto()
    }

    fn from_proto(proto: P) -> Result<Self, TryFromProtoError> {
        proto.into_rust().map(Arc::new)
    }
}

impl<R1, R2, P1, P2> RustType<(P1, P2)> for (R1, R2)
where
    R1: RustType<P1>,
    R2: RustType<P2>,
{
    fn into_proto(&self) -> (P1, P2) {
        (self.0.into_proto(), self.1.into_proto())
    }

    fn from_proto(proto: (P1, P2)) -> Result<Self, TryFromProtoError> {
        let first = proto.0.into_rust()?;
        let second = proto.1.into_rust()?;

        Ok((first, second))
    }
}

impl RustType<()> for () {
    fn into_proto(&self) -> () {
        *self
    }

    fn from_proto(proto: ()) -> Result<Self, TryFromProtoError> {
        Ok(proto)
    }
}

impl RustType<u64> for usize {
    fn into_proto(&self) -> u64 {
        u64::cast_from(*self)
    }

    fn from_proto(proto: u64) -> Result<Self, TryFromProtoError> {
        usize::try_from(proto).map_err(TryFromProtoError::from)
    }
}

impl RustType<u32> for char {
    fn into_proto(&self) -> u32 {
        (*self).into()
    }

    fn from_proto(proto: u32) -> Result<Self, TryFromProtoError> {
        char::try_from(proto).map_err(TryFromProtoError::from)
    }
}

impl RustType<u32> for u8 {
    fn into_proto(&self) -> u32 {
        u32::from(*self)
    }

    fn from_proto(proto: u32) -> Result<Self, TryFromProtoError> {
        u8::try_from(proto).map_err(TryFromProtoError::from)
    }
}

impl RustType<u32> for u16 {
    fn into_proto(&self) -> u32 {
        u32::from(*self)
    }

    fn from_proto(repr: u32) -> Result<Self, TryFromProtoError> {
        u16::try_from(repr).map_err(TryFromProtoError::from)
    }
}

impl RustType<i32> for i8 {
    fn into_proto(&self) -> i32 {
        i32::from(*self)
    }

    fn from_proto(proto: i32) -> Result<Self, TryFromProtoError> {
        i8::try_from(proto).map_err(TryFromProtoError::from)
    }
}

impl RustType<i32> for i16 {
    fn into_proto(&self) -> i32 {
        i32::from(*self)
    }

    fn from_proto(repr: i32) -> Result<Self, TryFromProtoError> {
        i16::try_from(repr).map_err(TryFromProtoError::from)
    }
}

impl RustType<u64> for std::num::NonZeroUsize {
    fn into_proto(&self) -> u64 {
        usize::from(*self).into_proto()
    }

    fn from_proto(proto: u64) -> Result<Self, TryFromProtoError> {
        Ok(usize::from_proto(proto)?.try_into()?)
    }
}

impl<T> RustType<T> for NonNeg<T>
where
    T: Clone + Signed + fmt::Display,
{
    fn into_proto(&self) -> T {
        (**self).clone()
    }

    fn from_proto(proto: T) -> Result<Self, TryFromProtoError> {
        Ok(NonNeg::<T>::try_from(proto)?)
    }
}

impl RustType<ProtoDuration> for std::time::Duration {
    fn into_proto(&self) -> ProtoDuration {
        ProtoDuration {
            secs: self.as_secs(),
            nanos: self.subsec_nanos(),
        }
    }

    fn from_proto(proto: ProtoDuration) -> Result<Self, TryFromProtoError> {
        Ok(std::time::Duration::new(proto.secs, proto.nanos))
    }
}

impl<'a> RustType<String> for Cow<'a, str> {
    fn into_proto(&self) -> String {
        self.to_string()
    }
    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(Cow::Owned(proto))
    }
}

impl RustType<String> for Box<str> {
    fn into_proto(&self) -> String {
        self.to_string()
    }
    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(proto.into())
    }
}

/// The symmetric counterpart of [`RustType`], similar to
/// what [`Into`] is to [`From`].
///
/// The `Rust` parameter is generic, as opposed to the `Proto`
/// associated type in [`RustType`] because the same Protobuf type
/// can be used to encode many different Rust types.
///
/// Clients should only implement [`RustType`].
pub trait ProtoType<Rust>: Sized {
    /// See [`RustType::from_proto`].
    fn into_rust(self) -> Result<Rust, TryFromProtoError>;

    /// See [`RustType::into_proto`].
    fn from_rust(rust: &Rust) -> Self;
}

/// Blanket implementation for [`ProtoType`], so clients only need
/// to implement [`RustType`].
impl<P, R> ProtoType<R> for P
where
    R: RustType<P>,
{
    #[inline]
    fn into_rust(self) -> Result<R, TryFromProtoError> {
        R::from_proto(self)
    }

    #[inline]
    fn from_rust(rust: &R) -> Self {
        R::into_proto(rust)
    }
}

pub fn any_duration() -> impl Strategy<Value = std::time::Duration> {
    (0..u64::MAX, 0..1_000_000_000u32)
        .prop_map(|(secs, nanos)| std::time::Duration::new(secs, nanos))
}

/// Convenience syntax for trying to convert a `Self` value of type
/// `Option<U>` to `T` if the value is `Some(value)`, or returning
/// [`TryFromProtoError::MissingField`] if the value is `None`.
pub trait IntoRustIfSome<T> {
    fn into_rust_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError>;
}

/// A blanket implementation for `Option<U>` where `U` is the
/// `RustType::Proto` type for `T`.
impl<R, P> IntoRustIfSome<R> for Option<P>
where
    R: RustType<P>,
{
    fn into_rust_if_some<S: ToString>(self, field: S) -> Result<R, TryFromProtoError> {
        R::from_proto(self.ok_or_else(|| TryFromProtoError::missing_field(field))?)
    }
}

/// Convenience syntax for trying to convert a `Self` value of type
/// `Option<U>` to `T` if the value is `Some(value)`, or returning
/// [`TryFromProtoError::MissingField`] if the value is `None`.
pub trait TryIntoIfSome<T> {
    fn try_into_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError>;
}

/// A blanket implementation for `Option<U>` where `U` is the
/// `Proto$T` type for `T`.
impl<T, U> TryIntoIfSome<T> for Option<U>
where
    T: TryFrom<U, Error = TryFromProtoError>,
{
    fn try_into_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError> {
        self.ok_or_else(|| TryFromProtoError::missing_field(field))?
            .try_into()
    }
}

/// Blanket command for testing if `R` can be converted to its corresponding
/// `ProtoType` and back.
pub fn protobuf_roundtrip<R, P>(val: &R) -> anyhow::Result<R>
where
    P: ProtoType<R> + ::prost::Message + Default,
{
    let vec = P::from_rust(val).encode_to_vec();
    let val = P::decode(&*vec)?.into_rust()?;
    Ok(val)
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn duration_protobuf_roundtrip(expect in any_duration() ) {
            let actual = protobuf_roundtrip::<_, ProtoDuration>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
