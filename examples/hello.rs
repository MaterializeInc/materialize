extern crate avro;
extern crate serde_json;

use avro::reader::Reader;
use avro::schema::Schema;
use avro::types::{Record, ToAvro, Value};
use avro::writer::Writer;

fn display(res: Vec<u8>) {
    for c in res {
        print!("{:02X} ", c);
    }
    println!();
}

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

    let mut writer = Writer::new(&schema, Vec::new());
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
