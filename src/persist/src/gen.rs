// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

#![allow(missing_docs)]

include!(concat!(env!("OUT_DIR"), "/mod.rs"));

use std::io::Read;

use bytes::BufMut;
use md5::{Digest, Md5};
use prost::Message;

use crate::error::Error;
use crate::gen::persist::ProtoMeta;

impl ProtoMeta {
    /// A versioning for how we store the protobuf serialization.
    ///
    /// Protobuf handles most of our backward and forward compatibility, but we
    /// don't just store the raw protobuf message serialization. This version
    /// number determines the exactly what that format is.
    ///
    /// Once we commit to backward compatibility, this should only change if we
    /// decide to e.g. switch from protobuf to some other encoding entirely.
    ///
    /// All versions less than this were developmental. If encountered, it's
    /// safe to delete all data in blob storage. If a greater version is seen,
    /// then some major change has happened and this code has no idea what is
    /// going on and should refuse to touch it.
    ///
    /// The following is an EBNF-ish spec for the format:
    ///
    /// ```none
    /// encoding = 11u8 v11_encoding
    /// v11_encoding = proto_meta md5_checksum
    /// proto_meta = u8* (the protobuf serialization of ProtoMeta)
    /// md5_checksum = u8 u8 u8 u8 (little endian, md5 of proto_meta)
    /// ```
    // TODO: Once this gets bumped to 8, we can clean up:
    // - The TODO in Blob::Cache.check_meta_build_version.
    pub const ENCODING_VERSION: u8 = 11;

    /// The [Self::ENCODING_VERSION] of this previously encoded ProtoMeta.
    ///
    /// Returns an error if the input is malformed.
    pub fn encoded_version(buf: &[u8]) -> Result<u8, Error> {
        buf.get(0)
            .copied()
            .ok_or_else(|| Error::from("missing encoding version"))
    }

    // NB: This len is intentionally hardcoded (not derived from the md5 crate)
    // so that a change to the crate can't break us. The compiler statically
    // checks that they match because this const is used in the return type.
    // (MD5 is not going to change, so this is all a moot point, but still
    // better to be defensive.)
    const CHECKSUM_LEN: usize = 16;
    fn md5_checksum(buf: &[u8]) -> [u8; Self::CHECKSUM_LEN] {
        let mut h = Md5::new();
        h.update(&buf);
        h.finalize().into()
    }
}

impl mz_persist_types::Codec for ProtoMeta {
    fn codec_name() -> String {
        "protobuf+md5[ProtoMeta]".into()
    }

    fn encode<B>(&self, buf: &mut B)
    where
        B: BufMut,
    {
        // TODO: Move checksum to be a field on the proto instead. We can encode
        // the proto, checksum'ing as we go, and then manually append it onto
        // the end.
        //
        // TODO: Regardless of the above TODO, compute the checksum as we go and
        // avoid this temp Vec.
        let temp = self.encode_to_vec();
        buf.put_slice(&[Self::ENCODING_VERSION]);
        buf.put_slice(&temp);
        let checksum = Self::md5_checksum(&temp);
        buf.put_slice(&checksum);
    }

    fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
        let mut buf = buf;

        let mut version = [0u8; 1];
        match buf.read_exact(&mut version) {
            Ok(_) if version == [Self::ENCODING_VERSION] => {}
            Ok(_) => return Err(format!("unsupported version: {}", version[0])),
            Err(_) => return Err("missing version".into()),
        }

        if buf.len() < Self::CHECKSUM_LEN {
            return Err("missing/incomplete checksum".into());
        }
        let checksum_start = buf.len() - Self::CHECKSUM_LEN;
        let (buf, checksum) = (&buf[..checksum_start], &buf[checksum_start..]);
        if checksum != &Self::md5_checksum(&buf) {
            return Err("checksum mismatch".into());
        }

        <Self as Message>::decode(buf).map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_types::Codec;

    use super::*;

    #[test]
    fn checksum() {
        let meta = ProtoMeta::default();
        let mut encoded = Vec::new();
        Codec::encode(&meta, &mut encoded);

        // Intact checksum matches.
        assert_eq!(<ProtoMeta as Codec>::decode(&encoded), Ok(meta));

        // Data has been mutated.
        let mut bad_data = encoded.clone();
        bad_data[1] += 1;
        assert_eq!(
            <ProtoMeta as Codec>::decode(&bad_data),
            Err("checksum mismatch".into())
        );

        // Checksum has been mutated.
        let mut bad_checksum = encoded.clone();
        *bad_checksum.last_mut().unwrap() += 1;
        assert_eq!(
            <ProtoMeta as Codec>::decode(&bad_checksum),
            Err("checksum mismatch".into())
        );
    }

    #[test]
    fn decode_errors() {
        // This is not a test of protobuf's roundtrip-ability, so don't
        // bother too much with the test data.
        let meta = ProtoMeta {
            seqno: 7,
            ..Default::default()
        };
        let mut encoded = Vec::new();
        Codec::encode(&meta, &mut encoded);

        // Sanity check that we don't just always return errors.
        assert_eq!(<ProtoMeta as Codec>::decode(&encoded), Ok(meta));

        // Every subset that's missing at least one byte should error, not panic
        // or succeed.
        for i in 0..encoded.len() - 1 {
            assert!(<ProtoMeta as Codec>::decode(&encoded[..i]).is_err());
        }
    }
}
