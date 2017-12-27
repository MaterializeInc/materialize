use std::str::FromStr;

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
            "deflate" => Ok(Codec::Null),
            #[cfg(feature = "snappy")] "snappy" => Ok(Codec::Snappy),
            _ => Err(()),
        }
    }
}
