// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence for Materialize dataflows.

#![warn(missing_docs)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::fmt;

pub mod error;
pub mod file;
pub mod indexed;
pub mod mem;
#[cfg(test)]
pub mod nemesis;
pub mod operators;
pub mod storage;
pub mod unreliable;

// TODO
// - This method of getting the metadata handle ends up being pretty clunky in
//   practice. Maybe instead the user should pass in a mutable reference to a
//   `Meta` they've constructed like `probe_with`?
// - Error handling. Right now, there are a bunch of `expect`s and this likely
//   needs to hook into the error streams that Materialize hands around.
// - Backward compatibility of persisted data, particularly the encoded keys and
//   values.
// - Restarting with a different number of workers.
// - Abomonation is convenient for prototyping, but we'll likely want to reuse
//   one of the popular serialization libraries.
// - Tighten up the jargon and usage of that jargon: write, update, persist,
//   drain, entry, update, data, buffer, blob, buffer (again), indexed, future,
//   trace.
// - Think through all the <, <=, !<= usages and document them more correctly
//   (aka replace before/after an antichain with in advance of/not in advance
//   of).
// - Clean up the various ways we pass a (sometimes encoded) entry/update
//   around, there are many for no particular reason: returning an iterator,
//   accepting a closure, accepting a mutable Vec, implementing Snapshot, etc.
// - Meta TODO: These were my immediate thoughts but there's stuff I'm
//   forgetting. Flesh this list out.

// Testing edge cases:
// - Failure while draining from buffer into future.
// - Equality edge cases around all the various timestamp/frontier checks.

/// A type usable as a persisted key or value.
///
/// TODO: Replace Abomonation with something like an Encode+Decode trait.
pub trait Data: Clone + EncodeDecode + fmt::Debug + Ord {}
impl<T: Clone + EncodeDecode + fmt::Debug + Ord> Data for T {}

/// WIP
pub trait EncodeDecode: Sized + 'static {
    /// WIP
    fn codec_name() -> &'static str;
    /// WIP
    fn size_hint(&self) -> usize;
    /// WIP
    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E);
    /// WIP
    ///
    /// TODO: Mechanically, this could return a ref to the original bytes
    /// without any copies, see if we can make the types work out for that.
    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String>;
}

mod encode_decode {
    use crate::EncodeDecode;

    impl EncodeDecode for () {
        fn codec_name() -> &'static str {
            "()"
        }

        fn size_hint(&self) -> usize {
            0
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, _buf: &mut E) {
            // No-op.
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            if !buf.is_empty() {
                return Err(format!("decode expected empty buf got {} bytes", buf.len()));
            }
            Ok(())
        }
    }

    impl EncodeDecode for String {
        fn codec_name() -> &'static str {
            "String"
        }

        fn size_hint(&self) -> usize {
            self.as_bytes().len()
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(self.as_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())
        }
    }

    impl EncodeDecode for Vec<u8> {
        fn codec_name() -> &'static str {
            "Vec<u8>"
        }

        fn size_hint(&self) -> usize {
            self.len()
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(self)
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(buf.to_owned())
        }
    }
}
