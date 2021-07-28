// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use crate::Codec;

impl Codec for () {
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

impl Codec for String {
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

impl Codec for Vec<u8> {
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
