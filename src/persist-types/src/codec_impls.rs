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

const RESULT_OK: u8 = 0;
const RESULT_ERR: u8 = 1;
impl<T: Codec, E: Codec> Codec for Result<T, E> {
    fn codec_name() -> &'static str {
        "Result"
    }

    fn size_hint(&self) -> usize {
        match self {
            Ok(r) => r.size_hint() + 1 + 8,
            Err(err) => err.size_hint() + 1 + 8,
        }
    }

    fn encode<B: for<'a> Extend<&'a u8>>(&self, buf: &mut B) {
        match self {
            Ok(r) => {
                buf.extend(&[RESULT_OK]);
                r.encode(buf);
            }
            Err(err) => {
                buf.extend(&[RESULT_ERR]);
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
