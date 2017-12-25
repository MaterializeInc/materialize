#![feature(type_ascription)]

extern crate avro;
extern crate serde_json;

use serde_json::Value as JsonValue;

use avro::schema::Schema;
use avro::types::Record;
use avro::writer::Writer;

fn display(res: Vec<u8>) {
    for c in res {
        print!("{:02X} ", c);
    }
    println!();
}

fn main() {
    let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long"},
                {"name": "b", "type": "string"}
            ]
        }
    "#;

    let json_schema: JsonValue = serde_json::from_str(raw_schema).unwrap();
    let schema = Schema::parse(&json_schema).unwrap();

    println!("{:?}", schema);

    let mut record = Record::new(&schema).unwrap();
    record.put("a", 27);
    record.put("b", "foo");

    let mut writer = Writer::new(&schema, Vec::new());
    writer.append(record).unwrap();

    display(writer.into_inner());
}
