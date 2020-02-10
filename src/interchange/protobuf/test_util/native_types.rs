// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native types to match [`super::test_util::proto`] types
//!
//! Primarily this exists so that we can more easily deserialize from JSON

use std::default::Default;

use protobuf::{RepeatedField, SingularPtrField};
use serde::Deserialize;

use super::gen::fuzz;

// proto::fuzz stubs

#[derive(Clone, Debug, Deserialize)]
pub enum Color {
    Red,
    Yellow,
    Blue,
}

impl From<Color> for fuzz::Color {
    fn from(color: Color) -> fuzz::Color {
        match color {
            Color::Red => fuzz::Color::RED,
            Color::Yellow => fuzz::Color::YELLOW,
            Color::Blue => fuzz::Color::BLUE,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Record {
    pub int_field: i32,
    pub string_field: String,
    pub int64_field: i64,
    pub bytes_field: Vec<u8>,
    pub color_field: Color,
    pub uint_field: u32,
    pub uint64_field: u64,
    pub float_field: f32,
    pub double_field: f64,
}

impl From<Record> for fuzz::TestRecord {
    fn from(rec: Record) -> fuzz::TestRecord {
        fuzz::TestRecord {
            int_field: rec.int_field,
            string_field: rec.string_field,
            int64_field: rec.int64_field,
            bytes_field: rec.bytes_field,
            color_field: rec.color_field.into(),
            uint_field: rec.uint_field,
            uint64_field: rec.uint64_field,
            float_field: rec.float_field,
            double_field: rec.double_field,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RepeatedRecord {
    pub int_field: Vec<i32>,
    pub double_field: Vec<f64>,
    pub string_field: Vec<String>,
}

impl From<RepeatedRecord> for fuzz::TestRepeatedRecord {
    fn from(rec: RepeatedRecord) -> fuzz::TestRepeatedRecord {
        fuzz::TestRepeatedRecord {
            int_field: rec.int_field,
            double_field: rec.double_field,
            string_field: rec.string_field.into(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct NestedRecord {
    pub test_record: Record,
    pub test_repeated_record: RepeatedRecord,
}

impl From<NestedRecord> for fuzz::TestNestedRecord {
    fn from(rec: NestedRecord) -> fuzz::TestNestedRecord {
        fuzz::TestNestedRecord {
            test_record: SingularPtrField::some(rec.test_record.into()),
            test_repeated_record: SingularPtrField::some(rec.test_repeated_record.into()),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct RepeatedNestedRecord {
    pub test_record: Vec<Record>,
    pub test_repeated_record: Vec<RepeatedRecord>,
    pub test_nested_record: Vec<NestedRecord>,
}

impl From<RepeatedNestedRecord> for fuzz::TestRepeatedNestedRecord {
    fn from(rec: RepeatedNestedRecord) -> fuzz::TestRepeatedNestedRecord {
        fuzz::TestRepeatedNestedRecord {
            test_record: iter_into(rec.test_record),
            test_repeated_record: iter_into(rec.test_repeated_record),
            test_nested_record: iter_into(rec.test_nested_record),
            ..Default::default()
        }
    }
}

fn iter_into<T>(items: Vec<impl Into<T>>) -> RepeatedField<T> {
    RepeatedField::from_vec(items.into_iter().map(Into::into).collect())
}
