# avro-rs

[![GitHub version](https://badge.fury.io/gh/flavray%2Favro-rs.svg)](https://badge.fury.io/gh/flavray%2Favro-rs)
[![Build Status](https://travis-ci.org/flavray/avro-rs.svg?branch=master)](https://travis-ci.org/flavray/avro-rs)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/flavray/avro-rs/blob/master/LICENSE)

[Documentation](https://docs.rs/avro-rs)

A library for working with [Apache Avro](https://avro.apache.org/) in Rust.

Please check our [documentation](https://docs.rs/avro-rs) for examples, tutorials and API reference.

## Example

Add to your `Cargo.toml`:

```toml
[dependencies]
avro-rs = "0.2"
```

Then try to write and read in Avro format like below:

```rust
extern crate avro_rs;

#[macro_use]
extern crate serde_derive;
extern crate failure;

use avro_rs::{Codec, Reader, Schema, Writer, from_value, types::Record};
use failure::Error;

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), Error> {
    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

    let schema = Schema::parse_str(raw_schema)?;

    println!("{:?}", schema);

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    let mut record = Record::new(writer.schema()).unwrap();
    record.put("a", 27i64);
    record.put("b", "foo");

    writer.append(record)?;

    let test = Test {
        a: 27,
        b: "foo".to_owned(),
    };

    writer.append_ser(test)?;

    writer.flush()?;

    let input = writer.into_inner();
    let reader = Reader::with_schema(&schema, &input[..])?;

    for record in reader {
        println!("{:?}", from_value::<Test>(&record?));
    }
    Ok(())
}
```

## License
This project is licensed under [MIT License](https://github.com/flavray/avro-rs/blob/master/LICENSE).
Please note that this is not an official project maintained by [Apache Avro](https://avro.apache.org/).

## Contributing
Everyone is encouraged to contribute! You can contribute by forking the GitHub repo and making a pull request or opening an issue.
All contributions will be licensed under [MIT License](https://github.com/flavray/avro-rs/blob/master/LICENSE).
