// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reader for [`arrow`] data that outputs [`Row`]s.

use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, StructArray,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, RelationDesc, Row, RowArena, RowPacker, ScalarType};

pub struct ArrowReader {
    len: usize,
    readers: Vec<ColReader>,
}

impl ArrowReader {
    pub fn new(desc: &RelationDesc, array: StructArray) -> Result<Self, String> {
        let inner_columns = array.columns();
        let desc_columns = desc.typ().columns();

        if inner_columns.len() != desc_columns.len() {
            return Err(format!(
                "wrong number of columns {} vs {}",
                inner_columns.len(),
                desc_columns.len()
            ));
        }

        let mut readers = Vec::with_capacity(desc_columns.len());
        for (col_name, col_type) in desc.iter() {
            let column = array
                .column_by_name(col_name.as_str())
                .ok_or_else(|| format!("'{col_name}' not found"))?;
            let reader = scalar_type_and_array_to_reader(&col_type.scalar_type, column.clone())
                .expect("OH NO");

            readers.push(reader);
        }

        Ok(ArrowReader {
            len: array.len(),
            readers,
        })
    }

    pub fn read(&self, idx: usize, row: &mut Row) {
        let mut packer = row.packer();
        for reader in &self.readers {
            reader.read(idx, &mut packer);
        }
    }

    pub fn read_all(&self, rows: &mut Vec<Row>) -> usize {
        for idx in 0..self.len {
            let mut row = Row::default();
            self.read(idx, &mut row);
            rows.push(row);
        }
        self.len
    }
}

fn scalar_type_and_array_to_reader(
    scalar_type: &ScalarType,
    array: Arc<dyn Array>,
) -> Result<ColReader, String> {
    fn downcast_array<T: arrow::array::Array + Clone + 'static>(array: Arc<dyn Array>) -> T {
        array
            .as_any()
            .downcast_ref::<T>()
            .expect("checked DataType")
            .clone()
    }

    match (scalar_type, array.data_type()) {
        (ScalarType::Bool, DataType::Boolean) => {
            Ok(ColReader::Boolean(downcast_array::<BooleanArray>(array)))
        }
        (ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64, DataType::Int8) => {
            let array = downcast_array::<Int8Array>(array);
            let cast: Box<dyn Fn(i8) -> Datum<'static>> = match scalar_type {
                ScalarType::Int16 => Box::new(|x| Datum::Int16(i16::cast_from(x))),
                ScalarType::Int32 => Box::new(|x| Datum::Int32(i32::cast_from(x))),
                ScalarType::Int64 => Box::new(|x| Datum::Int64(i64::cast_from(x))),
                _ => unreachable!("checked above"),
            };
            Ok(ColReader::Int8 { array, cast })
        }
        (ScalarType::Int16, DataType::Int16) => {
            Ok(ColReader::Int16(downcast_array::<Int16Array>(array)))
        }
        (ScalarType::Int32, DataType::Int32) => {
            Ok(ColReader::Int32(downcast_array::<Int32Array>(array)))
        }
        (ScalarType::Int64, DataType::Int64) => {
            Ok(ColReader::Int64(downcast_array::<Int64Array>(array)))
        }
        (ScalarType::UInt16, DataType::UInt16) => {
            Ok(ColReader::UInt16(downcast_array::<UInt16Array>(array)))
        }
        (ScalarType::UInt32, DataType::UInt32) => {
            Ok(ColReader::UInt32(downcast_array::<UInt32Array>(array)))
        }
        (ScalarType::UInt64, DataType::UInt64) => {
            Ok(ColReader::UInt64(downcast_array::<UInt64Array>(array)))
        }
        (ScalarType::String, DataType::Utf8) => {
            Ok(ColReader::String(downcast_array::<StringArray>(array)))
        }
        other => todo!("support {other:?}"),
    }
}

enum ColReader {
    Boolean(arrow::array::BooleanArray),
    Int16(arrow::array::Int16Array),
    Int32(arrow::array::Int32Array),
    Int64(arrow::array::Int64Array),
    UInt8(arrow::array::UInt8Array),
    UInt16(arrow::array::UInt16Array),
    UInt32(arrow::array::UInt32Array),
    UInt64(arrow::array::UInt64Array),
    Float16(arrow::array::Float16Array),
    Float32(arrow::array::Float32Array),
    Float64(arrow::array::Float64Array),
    String(arrow::array::StringArray),
    Int8 {
        array: arrow::array::Int8Array,
        cast: Box<dyn Fn(i8) -> Datum<'static>>,
    },
    Record {
        fields: Vec<Box<ColReader>>,
        nulls: Option<NullBuffer>,
    },
}

impl ColReader {
    fn read(&self, idx: usize, packer: &mut RowPacker) {
        let datum =
            match self {
                ColReader::Boolean(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(|x| if x { Datum::True } else { Datum::False }),
                ColReader::Int8 { array, cast } => {
                    array.is_valid(idx).then(|| array.value(idx)).map(cast)
                }
                ColReader::Int16(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::Int16),
                ColReader::Int32(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::Int32),
                ColReader::Int64(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::Int64),
                ColReader::UInt16(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::UInt16),
                ColReader::UInt32(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::UInt32),
                ColReader::UInt64(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::UInt64),
                ColReader::String(array) => array
                    .is_valid(idx)
                    .then(|| array.value(idx))
                    .map(Datum::String),
                ColReader::Record { fields, nulls } => {
                    let is_valid = nulls.as_ref().map(|n| n.is_valid(idx)).unwrap_or(true);
                    if !is_valid {
                        packer.push(Datum::Null);
                        return;
                    }

                    packer.push_list_with(|packer| {
                        for field in fields {
                            field.read(idx, packer);
                        }
                    });

                    // Return early because we've already packed the necessasry Datums.
                    return;
                }
                _ => unimplemented!("TODO"),
            };

        match datum {
            Some(d) => packer.push(d),
            None => packer.push(Datum::Null),
        }
    }
}
