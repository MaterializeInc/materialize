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

mod codec_impls;

/// Encoding and decoding operations for a type usable as a persisted key or
/// value.
pub trait Codec: Sized + 'static {
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
}

/// Encoding and decoding operations for a type usable as a persisted timestamp
/// or diff.
pub trait Codec64: Sized + 'static {
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
