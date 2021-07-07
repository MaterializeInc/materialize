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

//! Formatting utilities.

use std::fmt;
use std::io;

#[cfg(feature = "network")]
use bytes::{BufMut, BytesMut};

/// A trait for objects that can be infallibly written to.
///
/// Like [`std::fmt::Write`], except that the methods do not return errors.
/// Implementations are provided for [`String`], `bytes::BytesMut`, and
/// `Vec<u8>`, as writing to these types cannot fail.
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
pub trait FormatBuffer: AsRef<[u8]> {
    /// Glue for usage of the [`write!`] macro with implementors of this trait.
    ///
    /// This method should not be invoked manually, but rather through the
    /// `write!` macro itself.
    fn write_fmt(&mut self, fmt: fmt::Arguments);

    /// Writes a [`char`] into this buffer.
    fn write_char(&mut self, c: char);

    /// Writes a string into this buffer.
    fn write_str(&mut self, s: &str);

    /// Returns the number of bytes in the buffer.
    fn len(&self) -> usize;

    /// Reports whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Truncates the buffer to the specified length.
    ///
    /// # Panics
    ///
    /// Implementors may panic if `len` does not lie on a character boundary.
    fn truncate(&mut self, len: usize);

    /// Returns a mutable reference to the bytes in the buffer.
    ///
    /// # Safety
    ///
    /// If the byte slice was valid UTF-8, it must remain valid UTF-8 when the
    // returned mutable reference is dropped. `FormatBuffer`s may be
    /// [`String`]s or other data types whose memory safety depends upon only
    /// containing valid UTF-8.
    unsafe fn as_bytes_mut(&mut self) -> &mut [u8];
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

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        str::as_bytes_mut(self)
    }
}

impl FormatBuffer for Vec<u8> {
    fn write_fmt(&mut self, fmt: fmt::Arguments) {
        io::Write::write_fmt(self, fmt).expect("io::Write::write_fmt cannot fail on Vec<u8>")
    }

    fn write_char(&mut self, c: char) {
        self.extend(c.encode_utf8(&mut [0; 4]).as_bytes())
    }

    fn write_str(&mut self, s: &str) {
        self.extend(s.as_bytes())
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        self
    }
}

#[cfg(feature = "network")]
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

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, len: usize) {
        self.truncate(len)
    }

    unsafe fn as_bytes_mut(&mut self) -> &mut [u8] {
        self
    }
}
