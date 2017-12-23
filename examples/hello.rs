#![feature(type_ascription)]

extern crate avro;
extern crate serde_json;

use serde_json::Value as JsonValue;

use avro::encode::EncodeAvro;
use avro::schema::{Name, RecordField, Schema};
use avro::types::{Record, ToAvro, Value};

struct TestRecord<'a> {
    key1: &'a str,
    key2: &'a str,
    key3: Option<bool>,
    key4: Vec<i32>,
}

fn display(res: Vec<u8>) {
    for c in res {
        print!("{:02X} ", c);
    }
    println!();
}

/*
impl<'a> ToAvro for TestRecord<'a> {
    fn avro(self) -> Value {
        Value::Record(self.schema(), vec![
            ("key1", self.key1),
            ("key2", self.key2),
            ("key3", self.key3),
            ("key4", self.key3),
        ])
    }

    fn schema(&self) -> Schema {
        Schema::record(
            Name::new("test_record"),
            vec!(
                RecordField::new("key1", Schema::String, 0),
                RecordField::new("key2", Schema::String, 1),
                RecordField::new("key3", Schema::Union(Box::new(Schema::Boolean)), 2),
                RecordField::new("key4", Schema::Array(Box::new(Schema::Int)), 3),
            ),
        )
    }
}
*/

fn main() {
    /*
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
                {"type": "string", "doc": "test_doc1", "name": "key1", "order": "descending"},
                {"type": "string", "doc": "test_doc2", "name": "key2"}
            ]
        },
        {
            "type": "record",
            "name": "org.apache.avro.file.Header",
            "fields" : [
                {"name": "magic", "type": {"type": "fixed", "name": "magic", "size": 4}},
                {"name": "meta", "type": {"type": "map", "values": "bytes"}},
                {"name": "sync", "type": {"type": "fixed", "name": "sync", "size": 16}}
            ]
        }
    ]"#;

    let value: Value = serde_json::from_str(data).unwrap();

    println!("Hello, World!");
    println!("{:?}", Schema::parse(&value));
    */

    /*
    let record_schema = r#"
        {
            "type": "record",
            "namespace": "test_namespace",
            "source": "test_source",
            "name": "test_name",
            "doc": "test_doc",
            "fields": [
                {"type": "string", "doc": "test_doc1", "name": "key1", "order": "descending"},
                {"type": {"type": "array", "items": "int"}, "name": "key4"}
            ]
        }
    "#;

                {"default": null, "type": ["null", "boolean"], "name": "key3"},
                {"type": {"type": "fixed", "name": "magic", "size": 4}, "doc": "test_doc2", "name": "key2"},
                {"type": {"type": "array", "items": "int"}, "name": "key3"},
                {
                    "type": {
                        "type": "record",
                        "name": "inner",
                        "fields": [
                            {"type": "int", "name": "inner1"}
                        ]
                    },
                    "name": "key5"
                }
    */

    let record_schema = r#"
        {
            "type": "record",
            "namespace": "test_namespace",
            "source": "test_source",
            "name": "test_name",
            "doc": "test_doc",
            "fields": [
                {"type": "long", "name": "a"},
                {"type": "string", "name": "b"},
                {"type": {"type": "fixed", "name": "magic", "size": 4}, "doc": "test_doc2", "name": "c"}
            ]
        }
    "#;

    let v: JsonValue = serde_json::from_str(record_schema).unwrap();
    let schema = Schema::parse(&v).unwrap();

    println!("{:?}", schema);

    /*
    let mut record = Record::new(&schema).unwrap();
    record.put("key1", "hello");
    record.put("key4", Type::Array(vec![Type::Int(1)]));
    */

    let mut record = Record::new(&schema).unwrap();
    record.put("a", 27);
    record.put("b", "foo");
    record.put("c", "word".as_bytes());

    display(record.avro().encode());

    display(Value::Int(27).encode());

    display("foo".avro().encode());

    /*
    if let Ok(res) = BinarySerializer::serialize(&schema, &record.avro()) {
        display(res);
    }

    if let Ok(res) = BinarySerializer::serialize(&Schema::Int, &Type::Int(27)) {
        display(res);
    }

    let record = TestRecord {
        key1: "hello",
        key2: "word",
        key3: None,
        key4: vec![1, 2, 3],
    };

    let record_avro = record.avro();
    println!("{:?}", record_avro);
    println!("{:?}", record_avro.validate(&schema));

    let serializer = BinarySerializer::new(&schema);

    println!("{}", serializer.serialize(Value::String("foo".to_owned())));
    println!("{}", serializer.serialize(Value::Int(64)));
    println!("{}", serializer.serialize(Value::Float(42.0)));
    println!("{}", serializer.serialize(Value::Array(Schema::Null, vec![Value::Long(3), Value::Long(27)])));

    let mut record = Record::new(&schema).unwrap();
    record.put("key1", "hello");
    record.put("key2", "word");
    // record.put("key3", vec![1, 2, 3]);
    record.put("key3", None: Option<bool>);

    println!("{:?}", record.avro(&schema));
    */

    /*
    let inner_schema = match schema {
        Schema::Record(ref inner) => Some(&inner.fields[4].schema),
        _ => None,
    }.unwrap();

    let mut record = AvroRecord::new(&schema).unwrap();
    record.put("key1", "hello");
    record.put("key2", "word");
    record.put("key3", vec![1, 2, 3]);
    record.put("key4", None: Option<bool>);

    let mut inner_record = AvroRecord::new(inner_schema).unwrap();
    inner_record.put("inner1", 42);

    record.put("key5", inner_record);

    println!("{:?}", record);
    */
}
