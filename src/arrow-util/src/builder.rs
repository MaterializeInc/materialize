// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{builder::*, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, RelationDesc, Row, ScalarType};

pub struct ArrowBuilder {
    columns: Vec<ArrowColumn>,
    /// A crude estimate of the size of the data in the builder
    /// based on the size of the rows added to it.
    row_size_bytes: usize,
}

impl ArrowBuilder {
    /// Initializes a new ArrowBuilder with the schema of the provided RelationDesc.
    /// `item_capacity` is used to initialize the capacity of each column's builder which defines
    /// the number of values that can be appended to each column before reallocating.
    /// `data_capacity` is used to initialize the buffer size of the string and binary builders.
    /// Errors if the relation contains an unimplemented type.
    pub fn new(
        desc: &RelationDesc,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Result<Self, anyhow::Error> {
        let mut columns = vec![];
        let mut errs = vec![];
        let mut seen_names = BTreeMap::new();
        for (col_name, col_type) in desc.iter() {
            let mut col_name = col_name.to_string();
            // If we allow columns with the same name we encounter two issues:
            // 1. The arrow crate will accidentally reuse the same buffers for the columns
            // 2. Many parquet readers will error when trying to read the file metadata
            // Instead we append a number to the end of the column name for any duplicates.
            // TODO(roshan): We should document this when writing the copy-to-s3 MZ docs.
            seen_names
                .entry(col_name.clone())
                .and_modify(|e: &mut u32| {
                    *e += 1;
                    col_name += &e.to_string();
                })
                .or_insert(1);
            match scalar_to_arrow_datatype(&col_type.scalar_type) {
                Ok(data_type) => {
                    columns.push(ArrowColumn::new(
                        col_name,
                        col_type.nullable,
                        data_type,
                        item_capacity,
                        data_capacity,
                    )?);
                }
                Err(err) => errs.push(err.to_string()),
            }
        }
        if !errs.is_empty() {
            anyhow::bail!("Relation contains unimplemented arrow types: {:?}", errs);
        }
        Ok(Self {
            columns,
            row_size_bytes: 0,
        })
    }

    /// Returns a copy of the schema of the ArrowBuilder.
    pub fn schema(&self) -> Schema {
        let mut fields = vec![];
        for col in self.columns.iter() {
            fields.push(Field::new(
                col.field_name.clone(),
                col.data_type.clone(),
                col.nullable,
            ));
        }
        Schema::new(fields)
    }

    /// Converts the ArrowBuilder into an arrow RecordBatch.
    pub fn to_record_batch(self) -> Result<RecordBatch, ArrowError> {
        let mut arrays = vec![];
        let mut fields = vec![];
        for mut col in self.columns.into_iter() {
            arrays.push(col.inner.finish());
            fields.push(Field::new(col.field_name, col.data_type, col.nullable));
        }
        RecordBatch::try_new(Schema::new(fields).into(), arrays)
    }

    /// Appends a row to the builder.
    /// Errors if the row contains an unimplemented or out-of-range value.
    pub fn add_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        for (col, datum) in self.columns.iter_mut().zip(row.iter()) {
            col.inner.append_datum(datum)?;
        }
        self.row_size_bytes += row.byte_len();
        Ok(())
    }

    pub fn row_size_bytes(&self) -> usize {
        self.row_size_bytes
    }
}

fn scalar_to_arrow_datatype(scalar_type: &ScalarType) -> Result<DataType, anyhow::Error> {
    let data_type = match scalar_type {
        ScalarType::Bool => DataType::Boolean,
        ScalarType::Int16 => DataType::Int16,
        ScalarType::Int32 => DataType::Int32,
        ScalarType::Int64 => DataType::Int64,
        ScalarType::UInt16 => DataType::UInt16,
        ScalarType::UInt32 => DataType::UInt32,
        ScalarType::UInt64 => DataType::UInt64,
        ScalarType::Float32 => DataType::Float32,
        ScalarType::Float64 => DataType::Float64,
        ScalarType::Date => DataType::Date32,
        // The resolution of our time and timestamp types is microseconds, which is lucky
        // since the original parquet 'ConvertedType's support microsecond resolution but not
        // nanosecond resolution. The newer parquet 'LogicalType's support nanosecond resolution,
        // but many readers don't support them yet.
        ScalarType::Time => DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
        ScalarType::Timestamp { .. } => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        ScalarType::TimestampTz { .. } => DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            // When appending values we always use UTC timestamps, and setting this to a non-empty
            // value allows readers to know that tz-aware timestamps can be compared directly.
            Some("+00:00".into()),
        ),
        ScalarType::Bytes => DataType::LargeBinary,
        ScalarType::Char { length } => {
            if length.map_or(false, |l| l.into_u32() < i32::MAX.unsigned_abs()) {
                DataType::Utf8
            } else {
                DataType::LargeUtf8
            }
        }
        ScalarType::VarChar { max_length } => {
            if max_length.map_or(false, |l| l.into_u32() < i32::MAX.unsigned_abs()) {
                DataType::Utf8
            } else {
                DataType::LargeUtf8
            }
        }
        ScalarType::String => DataType::LargeUtf8,
        ScalarType::MzTimestamp => {
            // MZ timestamps are in milliseconds
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        // arrow::datatypes::IntervalUnit::MonthDayNano is not yet implemented in the arrow parquet writer
        // https://github.com/apache/arrow-rs/blob/0d031cc8aa81296cb1bdfedea7a7cb4ec6aa54ea/parquet/src/arrow/arrow_writer/mod.rs#L859
        // ScalarType::Interval => DataType::Interval(arrow::datatypes::IntervalUnit::DayTime)
        _ => anyhow::bail!("{:?} unimplemented", scalar_type),
    };
    Ok(data_type)
}

struct ArrowColumn {
    field_name: String,
    nullable: bool,
    data_type: DataType,
    inner: ColBuilder,
}

impl ArrowColumn {
    fn new(
        field_name: String,
        nullable: bool,
        data_type: DataType,
        item_capacity: usize,
        data_capacity: usize,
    ) -> Result<Self, anyhow::Error> {
        let inner: ColBuilder = match &data_type {
            DataType::Boolean => {
                ColBuilder::BooleanBuilder(BooleanBuilder::with_capacity(item_capacity))
            }
            DataType::Int16 => ColBuilder::Int16Builder(Int16Builder::with_capacity(item_capacity)),
            DataType::Int32 => ColBuilder::Int32Builder(Int32Builder::with_capacity(item_capacity)),
            DataType::Int64 => ColBuilder::Int64Builder(Int64Builder::with_capacity(item_capacity)),
            DataType::UInt8 => ColBuilder::UInt8Builder(UInt8Builder::with_capacity(item_capacity)),
            DataType::UInt16 => {
                ColBuilder::UInt16Builder(UInt16Builder::with_capacity(item_capacity))
            }
            DataType::UInt32 => {
                ColBuilder::UInt32Builder(UInt32Builder::with_capacity(item_capacity))
            }
            DataType::UInt64 => {
                ColBuilder::UInt64Builder(UInt64Builder::with_capacity(item_capacity))
            }
            DataType::Float32 => {
                ColBuilder::Float32Builder(Float32Builder::with_capacity(item_capacity))
            }
            DataType::Float64 => {
                ColBuilder::Float64Builder(Float64Builder::with_capacity(item_capacity))
            }
            DataType::Date32 => {
                ColBuilder::Date32Builder(Date32Builder::with_capacity(item_capacity))
            }
            DataType::Time64(arrow::datatypes::TimeUnit::Microsecond) => {
                ColBuilder::Time64MicrosecondBuilder(Time64MicrosecondBuilder::with_capacity(
                    item_capacity,
                ))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                ColBuilder::TimestampMicrosecondBuilder(TimestampMicrosecondBuilder::with_capacity(
                    item_capacity,
                ))
            }
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                ColBuilder::TimestampMillisecondBuilder(TimestampMillisecondBuilder::with_capacity(
                    item_capacity,
                ))
            }
            DataType::LargeBinary => ColBuilder::LargeBinaryBuilder(
                LargeBinaryBuilder::with_capacity(item_capacity, data_capacity),
            ),
            DataType::Utf8 => ColBuilder::StringBuilder(StringBuilder::with_capacity(
                item_capacity,
                data_capacity,
            )),
            DataType::LargeUtf8 => ColBuilder::LargeStringBuilder(
                LargeStringBuilder::with_capacity(item_capacity, data_capacity),
            ),
            _ => anyhow::bail!("{:?} unimplemented", data_type),
        };
        Ok(Self {
            field_name,
            nullable,
            data_type,
            inner,
        })
    }
}

macro_rules! make_col_builder {
    ($($x:ident), *) => {
        /// An enum wrapper for all arrow builder types that we support. Used to store
        /// a builder for each column and avoid dynamic dispatch and downcasting
        /// when appending data.
        enum ColBuilder {
            $(
                $x($x),
            )*
        }

        impl ColBuilder {
            fn append_null(&mut self) {
                match self {
                    $(
                        ColBuilder::$x(builder) => builder.append_null(),
                    )*
                }
            }

            fn finish(&mut self) -> ArrayRef {
                match self {
                    $(
                        ColBuilder::$x(builder) => Arc::new(builder.finish()),
                    )*
                }
            }
        }
    };
}

make_col_builder!(
    BooleanBuilder,
    Int16Builder,
    Int32Builder,
    Int64Builder,
    UInt8Builder,
    UInt16Builder,
    UInt32Builder,
    UInt64Builder,
    Float32Builder,
    Float64Builder,
    Date32Builder,
    Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
    LargeBinaryBuilder,
    StringBuilder,
    LargeStringBuilder
);

impl ColBuilder {
    fn append_datum(&mut self, datum: Datum) -> Result<(), anyhow::Error> {
        match (self, datum) {
            (s, Datum::Null) => s.append_null(),
            (ColBuilder::BooleanBuilder(builder), Datum::False) => builder.append_value(false),
            (ColBuilder::BooleanBuilder(builder), Datum::True) => builder.append_value(true),
            (ColBuilder::Int16Builder(builder), Datum::Int16(i)) => builder.append_value(i),
            (ColBuilder::Int32Builder(builder), Datum::Int32(i)) => builder.append_value(i),
            (ColBuilder::Int64Builder(builder), Datum::Int64(i)) => builder.append_value(i),
            (ColBuilder::UInt8Builder(builder), Datum::UInt8(i)) => builder.append_value(i),
            (ColBuilder::UInt16Builder(builder), Datum::UInt16(i)) => builder.append_value(i),
            (ColBuilder::UInt32Builder(builder), Datum::UInt32(i)) => builder.append_value(i),
            (ColBuilder::UInt64Builder(builder), Datum::UInt64(i)) => builder.append_value(i),
            (ColBuilder::Float32Builder(builder), Datum::Float32(f)) => builder.append_value(*f),
            (ColBuilder::Float64Builder(builder), Datum::Float64(f)) => builder.append_value(*f),
            (ColBuilder::Date32Builder(builder), Datum::Date(d)) => {
                builder.append_value(d.unix_epoch_days())
            }
            (ColBuilder::Time64MicrosecondBuilder(builder), Datum::Time(t)) => {
                let micros_since_midnight = i64::cast_from(t.num_seconds_from_midnight())
                    * 1_000_000
                    + i64::cast_from(t.nanosecond().checked_div(1000).unwrap());
                builder.append_value(micros_since_midnight)
            }
            (ColBuilder::TimestampMicrosecondBuilder(builder), Datum::Timestamp(ts)) => {
                builder.append_value(ts.and_utc().timestamp_micros())
            }
            (ColBuilder::TimestampMicrosecondBuilder(builder), Datum::TimestampTz(ts)) => {
                builder.append_value(ts.timestamp_micros())
            }
            (ColBuilder::LargeBinaryBuilder(builder), Datum::Bytes(b)) => builder.append_value(b),
            (ColBuilder::StringBuilder(builder), Datum::String(s)) => builder.append_value(s),
            (ColBuilder::LargeStringBuilder(builder), Datum::String(s)) => builder.append_value(s),
            (ColBuilder::TimestampMillisecondBuilder(builder), Datum::MzTimestamp(ts)) => {
                builder.append_value(ts.try_into().expect("checked"))
            }
            (_builder, datum) => {
                anyhow::bail!("Datum {:?} does not match builder", datum)
            }
        }
        Ok(())
    }
}
