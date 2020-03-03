//! Benchmarks for comparing `avro-rs` to `serde_json`. These benchmarks are meant to be
//! comparable to those found in `benches/serde.rs`.
#![feature(test)]
extern crate test;

use serde_json::Value;
use std::collections::HashMap;

fn make_big_json_record() -> Value {
    let mut address = HashMap::new();
    address.insert("street".to_owned(), "street".to_owned());
    address.insert("city".to_owned(), "city".to_owned());
    address.insert("state_prov".to_owned(), "state_prov".to_owned());
    address.insert("country".to_owned(), "country".to_owned());
    address.insert("zip".to_owned(), "zip".to_owned());
    let address_json = serde_json::to_value(address).unwrap();
    let mut big_record = std::collections::HashMap::new();
    big_record.insert(
        "username".to_owned(),
        serde_json::to_value("username").unwrap(),
    );
    big_record.insert("age".to_owned(), serde_json::to_value(10i32).unwrap());
    big_record.insert(
        "phone".to_owned(),
        serde_json::to_value("000000000").unwrap(),
    );
    big_record.insert("housenum".to_owned(), serde_json::to_value("0000").unwrap());
    big_record.insert("address".to_owned(), address_json);
    serde_json::to_value(big_record).unwrap()
}

fn make_json_records(record: &Value, count: usize) -> Vec<Value> {
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(record.clone());
    }
    records
}

fn write_json(records: &[Value]) -> Vec<u8> {
    serde_json::to_vec(records).unwrap()
}

fn read_json(bytes: &[u8]) {
    let reader: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    for record in reader.as_array().unwrap() {
        let _ = record;
    }
}

fn bench_read_json(b: &mut test::Bencher, make_record: &Fn() -> (Value), n_records: usize) {
    let record = make_record();
    let records = make_json_records(&record, n_records);
    let bytes = write_json(&records);
    println!("bytes.len() = {}", bytes.len());
    println!("records.len() = {}", records.len());
    b.iter(|| read_json(&bytes));
}

#[bench]
fn bench_big_schema_json_read_10000_record(b: &mut test::Bencher) {
    bench_read_json(b, &make_big_json_record, 10000);
}
