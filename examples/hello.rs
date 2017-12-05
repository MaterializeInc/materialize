extern crate avro;
extern crate serde_json;

use avro::Schema;
use serde_json::Value;

fn main() {
    let data = r#"[
        {
            "type": "array",
            "items": {
                "type": "enum",
                "name": "Suit",
                "doc": "Playing cart suits",
                "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
        },
        {
            "type": "fixed",
            "name": "md5",
            "size": 16
        },
        {
            "type": "map",
            "values": ["null", "boolean"]
        },
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        }
    ]"#;

    let value: Value = serde_json::from_str(data).unwrap();

    println!("Hello, World!");
    println!("{:?}", Schema::parse(&value));
}
