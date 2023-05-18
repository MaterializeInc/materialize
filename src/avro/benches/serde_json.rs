// Copyright 2018 Flavien Raynaud.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is derived from the avro-rs project, available at
// https://github.com/flavray/avro-rs. It was incorporated
// directly into Materialize on March 3, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Benchmarks for comparing `avro-rs` to `serde_json`. These benchmarks are meant to be
//! comparable to those found in `benches/serde.rs`.
#![feature(test)]
extern crate test;

use std::collections::BTreeMap;

use serde_json::Value;

fn make_big_json_record() -> Value {
    let mut address = BTreeMap::new();
    address.insert("street".to_owned(), "street".to_owned());
    address.insert("city".to_owned(), "city".to_owned());
    address.insert("state_prov".to_owned(), "state_prov".to_owned());
    address.insert("country".to_owned(), "country".to_owned());
    address.insert("zip".to_owned(), "zip".to_owned());
    let address_json = serde_json::to_value(address).unwrap();
    let mut big_record = std::collections::BTreeMap::new();
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
