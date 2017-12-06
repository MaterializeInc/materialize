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
            "namespace": "example.avro",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string", "default": "Bob"},
                {"name": "favorite_number",  "type": ["int", "null"]},
                {"name": "favorite_color", "type": ["string", "null"]}
            ]
        },
        {
            "type": "record",
            "namespace": "test_namespace",
            "source": "test_source",
            "name": "test_name",
            "doc": "test_doc",
            "fields": [
                {"type": "string", "doc": "test_doc1", "name": "key1"},
                {"type": "string", "doc": "test_doc2", "name": "key2"}
            ]
        }
    ]"#;

    let value: Value = serde_json::from_str(data).unwrap();

    println!("Hello, World!");
    println!("{:?}", Schema::parse(&value));
}
