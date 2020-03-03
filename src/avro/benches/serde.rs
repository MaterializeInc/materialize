#![feature(test)]
extern crate test;

use avro_rs::{
    schema::Schema,
    types::{Record, ToAvro, Value},
    Reader, Writer,
};

static RAW_SMALL_SCHEMA: &'static str = r#"
{
  "namespace": "test",
  "type": "record",
  "name": "Test",
  "fields": [
    {
      "type": {
        "type": "string"
      },
      "name": "field"
    }
  ]
}
"#;

static RAW_BIG_SCHEMA: &'static str = r#"
{
  "namespace": "my.example",
  "type": "record",
  "name": "userInfo",
  "fields": [
    {
      "default": "NONE",
      "type": "string",
      "name": "username"
    },
    {
      "default": -1,
      "type": "int",
      "name": "age"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "phone"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "housenum"
    },
    {
      "default": {},
      "type": {
        "fields": [
          {
            "default": "NONE",
            "type": "string",
            "name": "street"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "city"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "state_prov"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "country"
          },
          {
            "default": "NONE",
            "type": "string",
            "name": "zip"
          }
        ],
        "type": "record",
        "name": "mailing_address"
      },
      "name": "address"
    }
  ]
}
"#;

static RAW_ADDRESS_SCHEMA: &'static str = r#"
{
  "fields": [
    {
      "default": "NONE",
      "type": "string",
      "name": "street"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "city"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "state_prov"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "country"
    },
    {
      "default": "NONE",
      "type": "string",
      "name": "zip"
    }
  ],
  "type": "record",
  "name": "mailing_address"
}
"#;

fn make_small_record() -> (Schema, Value) {
    let small_schema = Schema::parse_str(RAW_SMALL_SCHEMA).unwrap();
    let small_record = {
        let mut small_record = Record::new(&small_schema).unwrap();
        small_record.put("field", "foo");
        small_record.avro()
    };
    (small_schema, small_record)
}

fn make_big_record() -> (Schema, Value) {
    let big_schema = Schema::parse_str(RAW_BIG_SCHEMA).unwrap();
    let address_schema = Schema::parse_str(RAW_ADDRESS_SCHEMA).unwrap();
    let mut address = Record::new(&address_schema).unwrap();
    address.put("street", "street");
    address.put("city", "city");
    address.put("state_prov", "state_prov");
    address.put("country", "country");
    address.put("zip", "zip");

    let big_record = {
        let mut big_record = Record::new(&big_schema).unwrap();
        big_record.put("username", "username");
        big_record.put("age", 10i32);
        big_record.put("phone", "000000000");
        big_record.put("housenum", "0000");
        big_record.put("address", address);
        big_record.avro()
    };

    (big_schema, big_record)
}

fn make_records(record: &Value, count: usize) -> Vec<Value> {
    let mut records = Vec::new();
    for _ in 0..count {
        records.push(record.clone());
    }
    records
}

fn write(schema: &Schema, records: &[Value]) -> Vec<u8> {
    let mut writer = Writer::new(&schema, Vec::new());
    writer.extend_from_slice(records).unwrap();
    writer.into_inner()
}

fn read(schema: &Schema, bytes: &[u8]) {
    let reader = Reader::with_schema(schema, bytes).unwrap();

    for record in reader {
        let _ = record.unwrap();
    }
}

fn read_schemaless(bytes: &[u8]) {
    let reader = Reader::new(bytes).unwrap();

    for record in reader {
        let _ = record.unwrap();
    }
}

fn bench_write(b: &mut test::Bencher, make_record: &Fn() -> (Schema, Value), n_records: usize) {
    let (schema, record) = make_record();
    let records = make_records(&record, n_records);
    b.iter(|| write(&schema, &records));
}

fn bench_read(b: &mut test::Bencher, make_record: &Fn() -> (Schema, Value), n_records: usize) {
    let (schema, record) = make_record();
    let records = make_records(&record, n_records);
    let bytes = write(&schema, &records);
    println!("bytes.len() = {}", bytes.len());
    println!("records.len() = {}", records.len());
    b.iter(|| read(&schema, &bytes));
}

fn bench_from_file(b: &mut test::Bencher, file_path: &str) {
    use std::fs;
    let bytes = fs::read(file_path).unwrap();
    println!("{} had {} bytes", file_path, bytes.len());
    b.iter(|| read_schemaless(&bytes));
}

#[bench]
fn bench_small_schema_write_1_record(b: &mut test::Bencher) {
    bench_write(b, &make_small_record, 1);
}

#[bench]
fn bench_small_schema_write_100_record(b: &mut test::Bencher) {
    bench_write(b, &make_small_record, 100);
}

#[bench]
fn bench_small_schema_write_10000_record(b: &mut test::Bencher) {
    bench_write(b, &make_small_record, 10000);
}

#[bench]
fn bench_small_schema_read_1_record(b: &mut test::Bencher) {
    bench_read(b, &make_small_record, 1);
}

#[bench]
fn bench_small_schema_read_100_record(b: &mut test::Bencher) {
    bench_read(b, &make_small_record, 100);
}

#[bench]
fn bench_small_schema_read_10000_record(b: &mut test::Bencher) {
    bench_read(b, &make_small_record, 10000);
}

#[bench]
fn bench_big_schema_write_1_record(b: &mut test::Bencher) {
    bench_write(b, &make_big_record, 1);
}

#[bench]
fn bench_big_schema_write_100_record(b: &mut test::Bencher) {
    bench_write(b, &make_big_record, 100);
}

#[bench]
fn bench_big_schema_write_10000_record(b: &mut test::Bencher) {
    bench_write(b, &make_big_record, 10000);
}

#[bench]
fn bench_big_schema_read_1_record(b: &mut test::Bencher) {
    bench_read(b, &make_big_record, 1);
}

#[bench]
fn bench_big_schema_read_100_record(b: &mut test::Bencher) {
    bench_read(b, &make_big_record, 100);
}

#[bench]
fn bench_big_schema_read_10000_record(b: &mut test::Bencher) {
    bench_read(b, &make_big_record, 10000);
}

#[bench]
fn bench_big_schema_read_100000_record(b: &mut test::Bencher) {
    bench_read(b, &make_big_record, 100000);
}

// This benchmark reads from the `benches/quickstop-null.avro` file, which was pulled from
// the `goavro` project benchmarks:
// https://github.com/linkedin/goavro/blob/master/fixtures/quickstop-null.avro
// This was done for the sake of comparing this crate against the `goavro` implementation.
#[bench]
fn bench_file_quickstop_null(b: &mut test::Bencher) {
    bench_from_file(b, "benches/quickstop-null.avro");
}
