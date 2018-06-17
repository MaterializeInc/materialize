//! Logic for all supported compression codecs in Avro.
use std::io::{Read, Write};
use std::str::FromStr;

use failure::Error;
use libflate::deflate::{Decoder, Encoder};
#[cfg(feature = "snappy")]
use snap::{Reader, Writer};

use types::{ToAvro, Value};
use util::DecodeError;

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
            }.to_owned()
                .into_bytes(),
        )
    }
}

impl FromStr for Codec {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "null" => Ok(Codec::Null),
            "deflate" => Ok(Codec::Deflate),
            #[cfg(feature = "snappy")]
            "snappy" => Ok(Codec::Snappy),
            _ => Err(DecodeError::new("unrecognized codec")),
        }
    }
}

impl Codec {
    /// Compress a stream of bytes in-place.
    pub fn compress(&self, stream: &mut Vec<u8>) -> Result<(), Error> {
        match *self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut encoder = Encoder::new(Vec::new());
                encoder.write_all(stream)?;
                *stream = encoder.finish().into_result()?;
            },
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                let mut writer = Writer::new(Vec::new());
                writer.write_all(stream)?;
                *stream = writer.into_inner()?; // .into_inner() will also call .flush()
            },
        };

        Ok(())
    }

    /// Decompress a stream of bytes in-place.
    pub fn decompress(&self, stream: &mut Vec<u8>) -> Result<(), Error> {
        match *self {
            Codec::Null => (),
            Codec::Deflate => {
                let mut decoded = Vec::new();
                {
                    // either the compiler or I is dumb
                    let mut decoder = Decoder::new(&stream[..]);
                    decoder.read_to_end(&mut decoded)?;
                }
                *stream = decoded;
            },
            #[cfg(feature = "snappy")]
            Codec::Snappy => {
                let mut read = Vec::new();
                {
                    let mut reader = Reader::new(&stream[..]);
                    reader.read_to_end(&mut read)?;
                }
                *stream = read;
            },
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static INPUT: &'static [u8] = b"theanswertolifetheuniverseandeverythingis42theanswertolifetheuniverseandeverythingis4theanswertolifetheuniverseandeverythingis2";

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
