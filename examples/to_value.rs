extern crate avro_rs;

#[macro_use]
extern crate serde_derive;
extern crate failure;

use avro_rs::to_value;
use failure::Error;

#[derive(Debug, Deserialize, Serialize)]
struct Test {
    a: i64,
    b: String,
}

fn main() -> Result<(), Error> {
    let test = Test { a: 27, b: "foo".to_owned() };
    println!("{:?}", to_value(test)?);

    Ok(())
}
