// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::io;

use crate::io::ParseBuf;

pub mod codec;
pub mod sync_framed;

/// Text protocol marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Text;

/// Binary protocol marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Binary;

/// Serialization for various MySql types.
pub trait MySerialize {
    /// Serializes self into the `buf`.
    fn serialize(&self, buf: &mut Vec<u8>);
}

/// Deserialization for various MySql types.
pub trait MyDeserialize<'de>: Sized {
    /// Size hint of a serialized value (in bytes), if it's constant.
    const SIZE: Option<usize>;

    /// Some structs defines deserialization in the context of another value.
    ///
    /// Use `()` here if the deserialization procedure is defined without premises.
    type Ctx;

    /// Deserializes self from the given `buf`.
    ///
    /// Imlementation must consume corresponding amount of bytes from the `buf`.
    ///
    /// # Panic
    ///
    /// Implementation must panic on insufficient buffer length if `Self::SIZE.is_some()`.
    /// One should use `ParseBuf::checked_parse` for checked deserialization.
    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self>;
}
