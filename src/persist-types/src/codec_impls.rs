// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of [Codec] for stdlib types.

use bytes::BufMut;

use crate::{Codec, Codec64};

impl Codec for () {
    fn codec_name() -> String {
        "()".into()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
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
    fn codec_name() -> String {
        "String".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.as_bytes())
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        String::from_utf8(buf.to_owned()).map_err(|err| err.to_string())
    }
}

impl Codec for Vec<u8> {
    fn codec_name() -> String {
        "Vec<u8>".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        buf.put(self.as_slice())
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        Ok(buf.to_owned())
    }
}

const RESULT_OK: u8 = 0;
const RESULT_ERR: u8 = 1;
impl<T: Codec, E: Codec> Codec for Result<T, E> {
    fn codec_name() -> String {
        "Result".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        match self {
            Ok(r) => {
                buf.put(&[RESULT_OK][..]);
                r.encode(buf);
            }
            Err(err) => {
                buf.put(&[RESULT_ERR][..]);
                err.encode(buf);
            }
        }
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let typ = buf[0];
        let result = match typ {
            RESULT_OK => {
                let result_slice = &buf[1..(buf.len())];
                Ok(T::decode(result_slice)?)
            }
            RESULT_ERR => {
                let err_slice = &buf[1..(buf.len())];
                Err(E::decode(err_slice)?)
            }
            typ => return Err(format!("Unexpected Result variant: {}.", typ)),
        };
        Ok(result)
    }
}

impl Codec64 for i64 {
    fn codec_name() -> String {
        "i64".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        i64::from_le_bytes(buf)
    }
}

impl Codec64 for u64 {
    fn codec_name() -> String {
        "u64".to_owned()
    }

    fn encode(&self) -> [u8; 8] {
        self.to_le_bytes()
    }

    fn decode(buf: [u8; 8]) -> Self {
        u64::from_le_bytes(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::Codec;

    #[test]
    fn test_result_ok_roundtrip() -> Result<(), String> {
        let original: Result<String, String> = Ok("ciao!".to_string());
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded: Result<String, String> = Result::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }

    #[test]
    fn test_result_err_roundtrip() -> Result<(), String> {
        let original: Result<String, String> = Err("ciao!".to_string());
        let mut encoded = Vec::new();
        original.encode(&mut encoded);
        let decoded: Result<String, String> = Result::decode(&encoded)?;

        assert_eq!(decoded, original);

        Ok(())
    }

    #[test]
    fn test_result_decoding_error() -> Result<(), String> {
        let encoded = vec![42];
        let decoded: Result<Result<String, String>, String> = Result::decode(&encoded);

        assert_eq!(decoded, Err("Unexpected Result variant: 42.".to_string()));

        Ok(())
    }
}
