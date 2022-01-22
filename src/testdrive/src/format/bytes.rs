// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;

/// Unescapes a testdrive byte string.
///
/// The escape character is `\` and the only interesting escape sequence is
/// `\xNN`, where each `N` is a valid hexadecimal digit. All other characters
/// following a backslash are taken literally.
pub fn unescape(s: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let mut out = vec![];
    let mut s = s.iter().copied().fuse();
    while let Some(b) = s.next() {
        match b {
            b'\\' if s.next() == Some(b'x') => match (next_hex(&mut s), next_hex(&mut s)) {
                (Some(c1), Some(c0)) => out.push((c1 << 4) + c0),
                _ => bail!("invalid hexadecimal escape"),
            },
            b'\\' => continue,
            _ => out.push(b),
        }
    }
    Ok(out)
}

/// Retrieves the value of the next hexadecimal digit in `iter`, if the next
/// byte is a valid hexadecimal digit.
fn next_hex<I>(iter: &mut I) -> Option<u8>
where
    I: Iterator<Item = u8>,
{
    iter.next().and_then(|c| match c {
        b'A'..=b'F' => Some(c - b'A' + 10),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'0'..=b'9' => Some(c - b'0'),
        _ => None,
    })
}
