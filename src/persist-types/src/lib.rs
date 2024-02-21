// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for the persist crate.

#![warn(missing_docs)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use bytes::BufMut;

use crate::columnar::Schema;

pub mod codec_impls;
pub mod columnar;
pub mod dyn_col;
pub mod dyn_struct;
pub mod parquet;
pub mod part;
pub mod stats;
pub mod timestamp;

/// Encoding and decoding operations for a type usable as a persisted key or
/// value.
pub trait Codec: Sized + 'static {
    /// The type of the associated schema for [Self].
    ///
    /// This is a separate type because Row is not self-describing. For Row, you
    /// need a RelationDesc to determine the types of any columns that are
    /// Datum::Null.
    type Schema: Schema<Self>;

    /// Name of the codec.
    ///
    /// This name is stored for the key and value when a stream is first created
    /// and the same key and value codec must be used for that stream afterward.
    fn codec_name() -> String;
    /// Encode a key or value for permanent storage.
    ///
    /// This must perfectly round-trip Self through [Codec::decode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut;
    /// Decode a key or value previous encoded with this codec's
    /// [Codec::encode].
    ///
    /// This must perfectly round-trip Self through [Codec::encode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    ///
    /// It should also gracefully handle data encoded by future versions of
    /// encode (likely with an error).
    //
    // TODO: Mechanically, this could return a ref to the original bytes
    // without any copies, see if we can make the types work out for that.
    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String>;

    /// A type used with [Self::decode_from] for allocation reuse. Set to `()`
    /// if unnecessary.
    type Storage;
    /// An alternate form of [Self::decode] which enables amortizing allocs.
    ///
    /// First, instead of returning `Self`, it takes `&mut Self` as a parameter,
    /// allowing for reuse of the internal `Row`/`SourceData` allocations.
    ///
    /// Second, it adds a `type Storage` to `Codec` and also passes it in as
    /// `&mut Option<Self::Storage>`, allowing for reuse of
    /// `ProtoRow`/`ProtoSourceData` allocations. If `Some`, this impl may
    /// attempt to reuse allocations with it. It may also leave allocations in
    /// it for use in future calls to `decode_from`.
    ///
    /// A default implementation is provided for `Codec` impls that can't take
    /// advantage of this.
    fn decode_from<'a>(
        &mut self,
        buf: &'a [u8],
        _storage: &mut Option<Self::Storage>,
    ) -> Result<(), String> {
        *self = Self::decode(buf)?;
        Ok(())
    }
}

/// Encoding and decoding operations for a type usable as a persisted timestamp
/// or diff.
pub trait Codec64: Sized + Clone + 'static {
    /// Name of the codec.
    ///
    /// This name is stored for the timestamp and diff when a stream is first
    /// created and the same timestamp and diff codec must be used for that
    /// stream afterward.
    fn codec_name() -> String;

    /// Encode a timestamp or diff for permanent storage.
    ///
    /// This must perfectly round-trip Self through [Codec64::decode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn encode(&self) -> [u8; 8];

    /// Decode a timestamp or diff previous encoded with this codec's
    /// [Codec64::encode].
    ///
    /// This must perfectly round-trip Self through [Codec64::encode]. If the
    /// encode function for this codec ever changes, decode must be able to
    /// handle bytes output by all previous versions of encode.
    fn decode(buf: [u8; 8]) -> Self;
}

/// An opaque fencing token used in compare_and_downgrade_since.
pub trait Opaque: PartialEq + Clone + Sized + 'static {
    /// The value of the opaque token when no compare_and_downgrade_since calls
    /// have yet been successful.
    fn initial() -> Self;
}

/// Advance a timestamp by the least amount possible such that
/// `ts.less_than(ts.step_forward())` is true.
///
/// TODO: Unify this with repr's TimestampManipulation. Or, ideally, get rid of
/// it entirely by making persist-txn methods take an `advance_to` argument.
pub trait StepForward {
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;
}

impl StepForward for u64 {
    fn step_forward(&self) -> Self {
        self.checked_add(1).unwrap()
    }
}
