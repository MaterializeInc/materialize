// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversion traits between Rust types and the durable catalog types in
//! [`mz_catalog_protos`].
//!
//! These mirror [`mz_proto::RustType`], [`mz_proto::ProtoType`] and
//! [`mz_proto::ProtoMapEntry`]. They are local to this crate because the
//! orphan rule forbids implementing a foreign trait for a foreign type with a
//! foreign type parameter, for example `mz_proto::RustType<objects::GlobalId>`
//! for `mz_repr::GlobalId`. A local trait lets `mz_catalog_protos` stay free of
//! the crates that define the Rust types.
//!
//! NOTE: Do not import these traits and their `mz_proto` namesakes into the
//! same scope. Method calls such as `into_proto()` become ambiguous for any
//! type that implements both.

use std::collections::BTreeMap;

use mz_proto::TryFromProtoError;

/// A trait for representing a Rust type `Self` as a value of type `Proto`
/// for the purpose of persisting it in the durable catalog.
///
/// Encoding with [`RustType::into_proto()`] is infallible. Decoding with
/// [`RustType::from_proto()`] fails with a [`TryFromProtoError`] if `Proto`
/// holds a value that `Self` cannot represent.
///
/// Convenience syntax for the above methods is available from the matching
/// [`ProtoType`].
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
    fn from_proto(proto: Proto) -> Result<Self, TryFromProtoError>;
}

/// A trait that allows `Self` to be used as an entry in a
/// `Vec<Self>` representing a Rust `*Map<K, V>`.
pub trait ProtoMapEntry<K, V> {
    fn from_rust<'a>(entry: (&'a K, &'a V)) -> Self;
    fn into_rust(self) -> Result<(K, V), TryFromProtoError>;
}

/// The symmetric counterpart of [`RustType`], similar to what [`Into`] is to
/// [`From`].
///
/// Clients should only implement [`RustType`].
pub trait ProtoType<Rust>: Sized {
    /// See [`RustType::from_proto`].
    fn into_rust(self) -> Result<Rust, TryFromProtoError>;

    /// See [`RustType::into_proto`].
    fn from_rust(rust: &Rust) -> Self;
}

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

rust_type_id![String, u64, Vec<u8>];

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
