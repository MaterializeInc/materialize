extern crate avro;
extern crate serde_json;

use avro::Codec;
use avro::reader::Reader;
use avro::schema::Schema;
use avro::types::{Record, ToAvro, Value};
use avro::writer::Writer;

struct Test<'a> {
    a: i64,
    b: &'a str,
}

impl<'a> ToAvro for Test<'a> {
    fn avro(self) -> Value {
        let mut record = Record::new();
        record.put("a", self.a);
        record.put("b", self.b);
        record.avro()
    }
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

    let schema = Schema::parse_str(raw_schema).unwrap();

    println!("{:?}", schema);

    let mut record = Record::with_schema(&schema).unwrap();
    record.put("a", 27);
    record.put("b", "foo");

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);
    writer.append(record).unwrap();

    let test = Test {
        a: 27,
        b: "foo",
    };

    writer.append(test.avro()).unwrap();

    let input = writer.into_inner();
    let reader = Reader::new(&input[..]);

    for record in reader {
        println!("{:?}", record);
    }
}
