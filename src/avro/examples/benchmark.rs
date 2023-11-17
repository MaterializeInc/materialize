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

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

// TODO(benesch): remove this once this module no longer makes use of
// potentially dangerous `as` conversions.
#![allow(clippy::as_conversions)]

use std::time::{Duration, Instant};

use mz_avro::schema::Schema;
use mz_avro::types::{Record, ToAvro, Value};
use mz_avro::{Reader, Writer};

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
