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

//! Lexing utilities.

/// A cursor over a string with a variety of lexing convenience methods.
#[derive(Debug)]
pub struct LexBuf<'a> {
    buf: &'a str,
    pos: usize,
}

impl<'a> LexBuf<'a> {
    /// Creates a new lexical buffer.
    ///
    /// The internal cursor is initialized to point at the start of `buf`.
    pub fn new(buf: &'a str) -> LexBuf<'a> {
        LexBuf { buf, pos: 0 }
    }

    /// Returns the next character in the buffer, if any, without advancing the
    /// internal cursor.
    pub fn peek(&self) -> Option<char> {
        self.buf[self.pos..].chars().next()
    }

    /// Returns a slice containing the next `n` characters in the buffer.
    ///
    /// Returns `None` if there are not at least `n` characters remaining.
    /// Advances the internal cursor `n` characters, or to the end of the buffer
    /// if there are less than `n` characters remaining.
    pub fn next_n(&mut self, n: usize) -> Option<&'a str> {
        let start = self.pos;
        for _ in 0..n {
            self.next()?;
        }
        Some(&self.buf[start..self.pos])
    }

    /// Returns the previous character in the buffer, positioning the internal
    /// cursor to before the character.
    ///
    /// The next call to `LexBuf::next` will return the same character.
    ///
    /// # Panics
    ///
    /// Panics if `prev` is called when the internal cursor is positioned at
    /// the beginning of the buffer.
    pub fn prev(&mut self) -> char {
        if let Some(c) = self.buf[..self.pos].chars().rev().next() {
            self.pos -= c.len_utf8();
            c
        } else {
            panic!("LexBuf::prev called on buffer at position 0")
        }
    }

    /// Advances the internal cursor past the next character in the buffer if
    /// the character is `ch`.
    ///
    /// Returns whether the cursor was advanced.
    pub fn consume(&mut self, ch: char) -> bool {
        if self.peek() == Some(ch) {
            self.next();
            true
        } else {
            false
        }
    }

    /// Searches the buffer for `delim`, returning the string from the current
    /// cursor position to the start of `delim`.
    ///
    /// Returns `None` if `delim` is not found in the buffer. The internal
    /// cursor is advanced past the end of `delim`, or to the end of the buffer
    /// if `delim` is not found.
    pub fn take_to_delimiter(&mut self, delim: &str) -> Option<&'a str> {
        if let Some(pos) = self.buf[self.pos..].find(delim) {
            let s = &self.buf[self.pos..self.pos + pos];
            self.pos += pos + delim.len();
            Some(s)
        } else {
            self.pos = self.buf.len();
            None
        }
    }

    /// Searches the remaining buffer for the first character to fail to satisfy
    /// `predicate`, returning the string from the current cursor position to
    /// the failing character.
    ///
    /// Advances the cursor to the character that failed the predicate, or to
    /// the end of the string if no character failed the predicate.
    pub fn take_while<P>(&mut self, mut predicate: P) -> &'a str
    where
        P: FnMut(char) -> bool,
    {
        let pos = self.pos;
        while let Some(ch) = self.peek() {
            if predicate(ch) {
                self.next();
            } else {
                break;
            }
        }
        &self.buf[pos..self.pos]
    }

    /// Reports the current position of the cursor in the buffer.
    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Returns the string that the lexical buffer wraps.
    ///
    /// Note that the entire string is returned, regardless of the position of
    /// the buffer's internal cursor.
    pub fn inner(&self) -> &'a str {
        self.buf
    }
}

impl<'a> Iterator for LexBuf<'a> {
    type Item = char;

    /// Returns the next character in the buffer, if any, advancing the internal
    /// cursor past the character.
    ///
    /// It is safe to call `next` after it returns `None`.
    fn next(&mut self) -> Option<char> {
        let c = self.peek();
        if let Some(c) = c {
            self.pos += c.len_utf8();
        }
        c
    }
}
