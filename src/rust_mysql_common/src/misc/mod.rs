// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::io::{self};

pub mod raw;

/// Returns length of length-encoded-integer representation of `x`.
pub fn lenenc_int_len(x: u64) -> u64 {
    if x < 251 {
        1
    } else if x < 65_536 {
        3
    } else if x < 16_777_216 {
        4
    } else {
        9
    }
}

/// Returns length of lenght-encoded-string representation of `s`.
pub fn lenenc_str_len(s: &[u8]) -> u64 {
    let len = s.len() as u64;
    lenenc_int_len(len) + len
}

pub(crate) fn unexpected_buf_eof() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "can't parse: buf doesn't have enough data",
    )
}

/// Splits server 'version' string into three numeric pieces.
///
/// It'll return `(0, 0, 0)` in case of error.
pub fn split_version<T: AsRef<[u8]>>(version_str: T) -> (u8, u8, u8) {
    let bytes = version_str.as_ref();
    split_version_inner(bytes).unwrap_or((0, 0, 0))
}

// Split into its own function for two reasons:
// 1. Generic function will be instantiated for every type, increasing code size
// 2. It allows using Option and ? operator without breaking public API
fn split_version_inner(input: &[u8]) -> Option<(u8, u8, u8)> {
    let mut nums = [0_u8; 3];
    let mut iter = input.split(|c| *c == b'.');
    for (i, chunk) in (&mut iter).take(2).enumerate() {
        nums[i] = btoi::btoi(chunk).ok()?;
    }
    // allow junk at the end of the final part of the version
    let chunk_with_junk = iter.next()?;
    let end_of_digits = chunk_with_junk.iter().position(|c| *c < b'0' || *c > b'9');
    let chunk = match end_of_digits {
        Some(pos) => &chunk_with_junk[..pos],
        None => chunk_with_junk,
    };
    nums[2] = btoi::btoi(chunk).ok()?;

    Some((nums[0], nums[1], nums[2]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_split_version() {
        assert_eq!((1, 2, 3), split_version("1.2.3"));
        assert_eq!((10, 20, 30), split_version("10.20.30foo"));
        assert_eq!((0, 0, 0), split_version("100.200.300foo"));
        assert_eq!((0, 0, 0), split_version("100.200foo"));
        assert_eq!((0, 0, 0), split_version("1,2.3"));
        assert_eq!((0, 0, 0), split_version("1"));
        assert_eq!((0, 0, 0), split_version("1.2"));
    }
}
