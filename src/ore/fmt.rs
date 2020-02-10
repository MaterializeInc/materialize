// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Formatting utilities.

use std::fmt;
use std::io;

use bytes::buf::{BufMut, BufMutExt};
use bytes::BytesMut;

/// A trait for objects that can be infallibly written to.
///
/// Like [`std::fmt::Write`], except that the methods do not return errors.
/// Implementations are provided for [`String`], [`BytesMut`], and `Vec<u8>`, as
/// writing to these types cannot fail.
///
/// Objects that implement `FormatBuffer` can be passed to the [`write!`] macro:
///
/// ```
/// use ore::fmt::FormatBuffer;
///
/// let mut buf = String::new();
/// write!(buf, "{:.02}", 1.0 / 7.0);
/// assert_eq!(buf, "0.14");
/// ```
///
/// The trait is particularly useful for functions that need to generically
/// write to either a [`String`] or a byte buffer:
///
/// ```
/// use ore::fmt::FormatBuffer;
///
/// fn write_timezone_offset<F>(buf: &mut F, mut offset_seconds: i32)
/// where
///     F: FormatBuffer,
/// {
///     if offset_seconds >= 0 {
///         buf.write_char('+');
///     } else {
///         buf.write_char('-');
///     }
///     let offset_seconds = offset_seconds.abs();
///     write!(buf, "{:02}:{:02}", offset_seconds / 60 / 60, offset_seconds / 60 % 60);
/// }
///
/// let offset_seconds = -18000;
///
/// let mut s = String::new();
/// write_timezone_offset(&mut s, offset_seconds);
///
/// let mut v = Vec::new();
/// write_timezone_offset(&mut v, offset_seconds);
///
/// assert_eq!(s, "-05:00");
/// assert_eq!(v, s.as_bytes());
/// ```
///
/// The implementations of `FormatBuffer` for `Vec<u8>` and `BytesMut` are
/// guaranteed to only write valid UTF-8 bytes into the underlying buffer.
pub trait FormatBuffer {
    /// Glue for usage of the [`write!`] macro with implementors of this trait.
    ///
    /// This method should not be invoked manually, but rather through the
    /// `write!` macro itself.
    fn write_fmt(&mut self, fmt: fmt::Arguments);

    /// Writes a [`char`] into this buffer.
    fn write_char(&mut self, c: char);

    /// Writes a string into this buffer.
    fn write_str(&mut self, s: &str);
}

impl FormatBuffer for String {
    fn write_fmt(&mut self, fmt: fmt::Arguments) {
        fmt::Write::write_fmt(self, fmt).expect("fmt::Write::write_fmt cannot fail on a String");
    }

    fn write_char(&mut self, c: char) {
        self.push(c)
    }

    fn write_str(&mut self, s: &str) {
        self.push_str(s)
    }
}

impl FormatBuffer for Vec<u8> {
    fn write_fmt(&mut self, fmt: fmt::Arguments) {
        io::Write::write_fmt(&mut self.writer(), fmt)
            .expect("io::Write::write_fmt cannot fail on Vec<u8>")
    }

    fn write_char(&mut self, c: char) {
        self.extend(c.encode_utf8(&mut [0; 4]).as_bytes())
    }

    fn write_str(&mut self, s: &str) {
        self.extend(s.as_bytes())
    }
}

impl FormatBuffer for BytesMut {
    fn write_fmt(&mut self, fmt: fmt::Arguments) {
        io::Write::write_fmt(&mut self.writer(), fmt)
            .expect("io::Write::write_fmt cannot fail on BytesMut")
    }

    fn write_char(&mut self, c: char) {
        self.put(c.encode_utf8(&mut [0; 4]).as_bytes())
    }

    fn write_str(&mut self, s: &str) {
        self.put(s.as_bytes())
    }
}
