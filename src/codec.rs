use std::io::{Read, Write};
use std::str::FromStr;

use failure::Error;
use libflate::deflate::{Decoder, Encoder};
#[cfg(feature = "snappy")] use snap::{Reader, Writer};

use types::{ToAvro, Value};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Codec {
    Null,
    Deflate,
    #[cfg(feature = "snappy")] Snappy,
}

impl ToAvro for Codec {
    fn avro(self) -> Value {
        Value::Bytes(
            match self {
                Codec::Null => "null",
                Codec::Deflate => "deflate",
                #[cfg(feature = "snappy")] Codec::Snappy => "snappy",
            }
                .to_owned().into_bytes())
    }
}

impl FromStr for Codec {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "null" => Ok(Codec::Null),
            "deflate" => Ok(Codec::Deflate),
            #[cfg(feature = "snappy")] "snappy" => Ok(Codec::Snappy),
            _ => Err(()),
        }
    }
}

impl Codec {
    pub fn compress(&self, stream: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            &Codec::Null => (),
            &Codec::Deflate => {
                let mut encoder = Encoder::new(Vec::new());
                encoder.write(stream)?;
                *stream = encoder.finish().into_result()?;
            },
            #[cfg(feature = "snappy")] &Codec::Snappy => {
                let mut writer = Writer::new(Vec::new());
                writer.write(stream)?;
                *stream = writer.into_inner()?;  // .into_inner() will also call .flush()
            },
        };

        Ok(())
    }

    pub fn decompress(&self, stream: &mut Vec<u8>) -> Result<(), Error> {
        match self {
            &Codec::Null => (),
            &Codec::Deflate => {
                let mut decoded = Vec::new();
                {  // either the compiler or I is dumb
                    let mut decoder = Decoder::new(&stream[..]);
                    decoder.read_to_end(&mut decoded)?;
                }
                *stream = decoded;
            },
            #[cfg(feature = "snappy")] &Codec::Snappy => {
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