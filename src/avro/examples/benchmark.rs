// Copyright Materialize, Inc., Flavien Raynaud and other contributors.
//
// Use of this software is governed by the Apache License, Version 2.0

use std::time::{Duration, Instant};

use mz_avro::{
    schema::Schema,
    types::{Record, ToAvro, Value},
    Reader, Writer,
};

fn nanos(duration: Duration) -> u64 {
    duration.as_secs() * 1_000_000_000 + duration.subsec_nanos() as u64
}

fn seconds(nanos: u64) -> f64 {
    (nanos as f64) / 1_000_000_000f64
}

/*
fn duration(nanos: u64) -> Duration {
    Duration::new(nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
}
*/

fn benchmark(schema: &Schema, record: &Value, s: &str, count: usize, runs: usize) {
    let mut records = Vec::new();
    for __ in 0..count {
        records.push(record.clone());
    }

    let mut durations = Vec::with_capacity(runs);

    let mut bytes = None;
    for _ in 0..runs {
        let records = records.clone();

        let start = Instant::now();
        let mut writer = Writer::new(schema.clone(), Vec::new());
        writer.extend(records.into_iter()).unwrap();

        let duration = Instant::now().duration_since(start);
        durations.push(duration);

        bytes = Some(writer.into_inner());
    }

    let total_duration_write = durations.into_iter().fold(0u64, |a, b| a + nanos(b));

    // println!("Write: {} {} {:?}", count, runs, seconds(total_duration));

    let bytes = bytes.unwrap();

    let mut durations = Vec::with_capacity(runs);

    for _ in 0..runs {
        let bytes = &bytes[..];
        let durations = &mut durations;
        let start = Instant::now();
        let reader = Reader::with_schema(schema, bytes).unwrap();

        let mut read_records = Vec::with_capacity(count);
        for record in reader {
            read_records.push(record);
        }

        let duration = Instant::now().duration_since(start);
        durations.push(duration);

        assert_eq!(count, read_records.len());
    }

    let total_duration_read = durations.into_iter().fold(0u64, |a, b| a + nanos(b));

    // println!("Read: {} {} {:?}", count, runs, seconds(total_duration));
    let (s_w, s_r) = (seconds(total_duration_write), seconds(total_duration_read));

    println!("{},{},{},{},{}", count, runs, s, s_w, s_r);
}

fn main() {
    let raw_small_schema = r#"
        {"namespace": "test", "type": "record", "name": "Test", "fields": [{"type": {"type": "string"}, "name": "field"}]}
    "#;

    let raw_big_schema = r#"
        {"namespace": "my.example", "type": "record", "name": "userInfo", "fields": [{"default": "NONE", "type": "string", "name": "username"}, {"default": -1, "type": "int", "name": "age"}, {"default": "NONE", "type": "string", "name": "phone"}, {"default": "NONE", "type": "string", "name": "housenum"}, {"default": {}, "type": {"fields": [{"default": "NONE", "type": "string", "name": "street"}, {"default": "NONE", "type": "string", "name": "city"}, {"default": "NONE", "type": "string", "name": "state_prov"}, {"default": "NONE", "type": "string", "name": "country"}, {"default": "NONE", "type": "string", "name": "zip"}], "type": "record", "name": "mailing_address"}, "name": "address"}]}
    "#;

    let small_schema: Schema = raw_small_schema.parse().unwrap();
    let big_schema: Schema = raw_big_schema.parse().unwrap();

    println!("{:?}", small_schema);
    println!("{:?}", big_schema);

    let mut small_record = Record::new(small_schema.top_node()).unwrap();
    small_record.put("field", "foo");
    let small_record = small_record.avro();

    let raw_address_schema = r#"{"fields": [{"default": "NONE", "type": "string", "name": "street"}, {"default": "NONE", "type": "string", "name": "city"}, {"default": "NONE", "type": "string", "name": "state_prov"}, {"default": "NONE", "type": "string", "name": "country"}, {"default": "NONE", "type": "string", "name": "zip"}], "type": "record", "name": "mailing_address"}"#;
    let address_schema: Schema = raw_address_schema.parse().unwrap();
    let mut address = Record::new(address_schema.top_node()).unwrap();
    address.put("street", "street");
    address.put("city", "city");
    address.put("state_prov", "state_prov");
    address.put("country", "country");
    address.put("zip", "zip");

    let mut big_record = Record::new(big_schema.top_node()).unwrap();
    big_record.put("username", "username");
    big_record.put("age", 10i32);
    big_record.put("phone", "000000000");
    big_record.put("housenum", "0000");
    big_record.put("address", address);
    let big_record = big_record.avro();

    benchmark(&small_schema, &small_record, "S", 10000, 1);
    benchmark(&big_schema, &big_record, "B", 10000, 1);

    benchmark(&small_schema, &small_record, "S", 1, 100_000);
    benchmark(&small_schema, &small_record, "S", 100, 1000);
    benchmark(&small_schema, &small_record, "S", 10000, 10);

    benchmark(&big_schema, &big_record, "B", 1, 100_000);
    benchmark(&big_schema, &big_record, "B", 100, 1000);
    benchmark(&big_schema, &big_record, "B", 10000, 10);
}
