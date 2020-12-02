// Copyright Materialize, Inc., Flavien Raynaud and other contributors.
//
// Use of this software is governed by the Apache License, Version 2.0

//! Logic for all supported compression codecs in Avro.
use std::io::{Read, Write};
use std::str::FromStr;

use anyhow::Error;
use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;

use crate::error::{DecodeError, Error as AvroError};
use crate::types::{ToAvro, Value};

/// The compression codec used to compress blocks.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Codec {
    /// The `Null` codec simply passes through data uncompressed.
    Null,
    /// The `Deflate` codec writes the data block using the deflate algorithm
    /// as specified in RFC 1951, and typically implemented using the zlib library.
    /// Note that this format (unlike the "zlib format" in RFC 1950) does not have a checksum.
    Deflate,
    #[cfg(feature = "snappy")]
    /// The `Snappy` codec uses Google's [Snappy](http://google.github.io/snappy/)
    /// compression library. Each compressed block is followed by the 4-byte, big-endian
    /// CRC32 checksum of the uncompressed data in the block.
    Snappy,
}

impl ToAvro for Codec {
    fn avro(self) -> Value {
        Value::Bytes(
            match self {
                Codec::Null => "null",
                Codec::Deflate => "deflate",
                #[cfg(feature = "snappy")]
                Codec::Snappy => "snappy",
            }
            .to_owned()
            .into_bytes(),
        )
    }
}

impl FromStr for Codec {
    type Err = AvroError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "null" => Ok(Codec::Null),
            "deflate" => Ok(Codec::Deflate),
            #[cfg(feature = "snappy")]
            "snappy" => Ok(Codec::Snappy),
            other => Err(DecodeError::UnrecognizedCodec(other.to_string()).into()),
        }
    }
}

impl Codec {
    /// Compress a stream of bytes in-place.
    pub fn compress(self, stream: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(stream)?;
                *stream = encoder.finish()?;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use byteorder::ByteOrder;

                let mut encoded: Vec<u8> = vec![0; snap::raw::max_compress_len(stream.len())];
                let compressed_size =
                    snap::raw::Encoder::new().compress(&stream[..], &mut encoded[..])?;

                let crc = crc::crc32::checksum_ieee(&stream[..]);
                byteorder::BigEndian::write_u32(&mut encoded[compressed_size..], crc);
                encoded.truncate(compressed_size + 4);

                *stream = encoded;
            }
        };

        Ok(())
    }

    /// Decompress a stream of bytes in-place.
    pub fn decompress(self, stream: &mut Vec<u8>) -> Result<(), AvroError> {
        match self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut decoded = Vec::new();
                {
                    let mut decoder = DeflateDecoder::new(&**stream);
                    decoder.read_to_end(&mut decoded)?;
                }
                *stream = decoded;
            }
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                use byteorder::ByteOrder;

                let decompressed_size = snap::raw::decompress_len(&stream[..stream.len() - 4])
                    .map_err(std::io::Error::from)?;
                let mut decoded = vec![0; decompressed_size];
                snap::raw::Decoder::new()
                    .decompress(&stream[..stream.len() - 4], &mut decoded[..])
                    .map_err(std::io::Error::from)?;

                let expected_crc = byteorder::BigEndian::read_u32(&stream[stream.len() - 4..]);
                let actual_crc = crc::crc32::checksum_ieee(&decoded);

                if expected_crc != actual_crc {
                    return Err(DecodeError::BadSnappyChecksum {
                        expected: expected_crc,
                        actual: actual_crc,
                    }
                    .into());
                }
                *stream = decoded;
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static INPUT: &[u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

    #[test]
    fn null_compress_and_decompress() {
        let codec = Codec::Null;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[test]
    fn deflate_compress_and_decompress() {
        let codec = Codec::Deflate;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compress_and_decompress() {
        let codec = Codec::Snappy;
        let mut stream = INPUT.to_vec();
        codec.compress(&mut stream).unwrap();
        assert_ne!(INPUT, stream.as_slice());
        assert!(INPUT.len() > stream.len());
        codec.decompress(&mut stream).unwrap();
        assert_eq!(INPUT, stream.as_slice());
    }
}
