// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Encoding and decoding support for various formats that represent binary data
//! as text data.

use mz_ore::fmt::FormatBuffer;
use mz_repr::strconv;
use uncased::UncasedStr;

use crate::EvalError;

/// An encoding format.
pub trait Format {
    /// Encodes a byte slice into its string representation according to this
    /// format.
    fn encode(&self, bytes: &[u8]) -> String;

    /// Decodes a byte slice from its string representation according to this
    /// format.
    fn decode(&self, s: &str) -> Result<Vec<u8>, EvalError>;
}

/// PostgreSQL-style Base64 encoding.
///
/// PostgreSQL follows RFC 2045, which requires that lines are broken after 76
/// characters when encoding and that all whitespace characters are ignored when
/// decoding. See <http://materialize.com/docs/sql/functions/encode> for
/// details.
struct Base64Format;

impl Base64Format {
    const CHARSET: &'static [u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    fn encode_sextet(v: u8) -> char {
        char::from(Self::CHARSET[usize::from(v)])
    }

    fn decode_sextet(b: u8) -> Result<u8, EvalError> {
        match b {
            b'A'..=b'Z' => Ok(b - b'A'),
            b'a'..=b'z' => Ok(b - b'a' + 26),
            b'0'..=b'9' => Ok(b + 4),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(EvalError::InvalidBase64Symbol(char::from(b))),
        }
    }
}

impl Format for Base64Format {
    // Support for PostgreSQL-style (which is really MIME-style) Base64 encoding
    // was, frustratingly, removed from Rust's `base64` crate. So we roll our
    // own Base64 encoder and decoder here.

    fn encode(&self, bytes: &[u8]) -> String {
        // Process input in chunks of three octets. Each chunk is converted to
        // four sextets. Each sextet is encoded as a printable ASCII character
        // via `CHARSET`.
        //
        // When the input length is not divisible by three, the last chunk is
        // partial. Sextets that are entirely determined by missing octets are
        // encoded as `=`. Sextets that are partially determined by a missing
        // octect assume the octet was zero.
        //
        // Line breaks are emitted after every 76 characters.

        let mut buf = String::new();
        for chunk in bytes.chunks(3) {
            match chunk {
                [o1, o2, o3] => {
                    let s1 = (o1 & 0b11111100) >> 2;
                    let s2 = (o1 & 0b00000011) << 4 | (o2 & 0b11110000) >> 4;
                    let s3 = (o2 & 0b00001111) << 2 | (o3 & 0b11000000) >> 6;
                    let s4 = o3 & 0b00111111;
                    buf.push(Self::encode_sextet(s1));
                    buf.push(Self::encode_sextet(s2));
                    buf.push(Self::encode_sextet(s3));
                    buf.push(Self::encode_sextet(s4));
                }
                [o1, o2] => {
                    let s1 = (o1 & 0b11111100) >> 2;
                    let s2 = (o1 & 0b00000011) << 4 | (o2 & 0b11110000) >> 4;
                    let s3 = (o2 & 0b00001111) << 2;
                    buf.push(Self::encode_sextet(s1));
                    buf.push(Self::encode_sextet(s2));
                    buf.push(Self::encode_sextet(s3));
                    buf.push('=');
                }
                [o1] => {
                    let s1 = (o1 & 0b11111100) >> 2;
                    let s2 = (o1 & 0b00000011) << 4;
                    buf.push(Self::encode_sextet(s1));
                    buf.push(Self::encode_sextet(s2));
                    buf.push('=');
                    buf.push('=');
                }
                _ => unreachable!(),
            }
            if buf.len() % 77 == 76 {
                buf.push('\n');
            }
        }
        buf
    }

    fn decode(&self, s: &str) -> Result<Vec<u8>, EvalError> {
        // Process input in chunks of four bytes, after filtering out any bytes
        // that represent whitespace. Each byte is decoded into a sextet
        // according to the reverse charset mapping maintained in
        // `Self::decode_sextet`. The four sextets are converted to three octets
        // and emitted.
        //
        // When the last character in a chunk is `=` or the last two characters
        // in a chunk are both `=`, the chunk is missing its last one or two
        // sextets, respectively. Octets that are entirely determined by missing
        // sextets are elided. Octets that are partially determined by a missing
        // sextet assume the sextet was zero.
        //
        // It is an error for a `=` character to appear in another position in
        // a chunk. It is also an error if a chunk is incomplete.

        let mut buf = vec![];
        let mut bytes = s
            .as_bytes()
            .iter()
            .copied()
            .filter(|ch| !matches!(ch, b' ' | b'\t' | b'\n' | b'\r'));
        loop {
            match (bytes.next(), bytes.next(), bytes.next(), bytes.next()) {
                (Some(c1), Some(c2), Some(b'='), Some(b'=')) => {
                    let s1 = Self::decode_sextet(c1)?;
                    let s2 = Self::decode_sextet(c2)?;
                    buf.push(s1 << 2 | (s2 & 0b110000) >> 4);
                }
                (Some(c1), Some(c2), Some(c3), Some(b'=')) => {
                    let s1 = Self::decode_sextet(c1)?;
                    let s2 = Self::decode_sextet(c2)?;
                    let s3 = Self::decode_sextet(c3)?;
                    buf.push(s1 << 2 | (s2 & 0b110000) >> 4);
                    buf.push((s2 & 0b001111) << 4 | (s3 & 0b111100) >> 2);
                }
                (Some(b'='), _, _, _) | (_, Some(b'='), _, _) | (_, _, Some(b'='), _) => {
                    return Err(EvalError::InvalidBase64Equals)
                }
                (Some(c1), Some(c2), Some(c3), Some(c4)) => {
                    let s1 = Self::decode_sextet(c1)?;
                    let s2 = Self::decode_sextet(c2)?;
                    let s3 = Self::decode_sextet(c3)?;
                    let s4 = Self::decode_sextet(c4)?;
                    buf.push(s1 << 2 | (s2 & 0b110000) >> 4);
                    buf.push((s2 & 0b001111) << 4 | (s3 & 0b111100) >> 2);
                    buf.push((s3 & 0b000011) << 6 | s4);
                }
                (None, None, None, None) => return Ok(buf),
                _ => return Err(EvalError::InvalidBase64EndSequence),
            }
        }
    }
}

struct EscapeFormat;

impl Format for EscapeFormat {
    fn encode(&self, bytes: &[u8]) -> String {
        let mut buf = String::new();
        for b in bytes {
            match b {
                b'\0' | (b'\x80'..=b'\xff') => {
                    buf.push('\\');
                    write!(&mut buf, "{:03o}", b);
                }
                b'\\' => buf.push_str("\\\\"),
                _ => buf.push(char::from(*b)),
            }
        }
        buf
    }

    fn decode(&self, s: &str) -> Result<Vec<u8>, EvalError> {
        Ok(strconv::parse_bytes_traditional(s)?)
    }
}

struct HexFormat;

impl Format for HexFormat {
    fn encode(&self, bytes: &[u8]) -> String {
        hex::encode(bytes)
    }

    fn decode(&self, s: &str) -> Result<Vec<u8>, EvalError> {
        // Can't use `hex::decode` here, as it doesn't tolerate whitespace
        // between encoded bytes.
        Ok(strconv::parse_bytes_hex(s)?)
    }
}

pub fn lookup_format(s: &str) -> Result<&'static dyn Format, EvalError> {
    let s = UncasedStr::new(s);
    if s == "base64" {
        Ok(&Base64Format)
    } else if s == "escape" {
        Ok(&EscapeFormat)
    } else if s == "hex" {
        Ok(&HexFormat)
    } else {
        Err(EvalError::InvalidEncodingName(s.as_str().into()))
    }
}
