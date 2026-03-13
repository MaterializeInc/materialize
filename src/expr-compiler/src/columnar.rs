// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columnar buffer types for passing data across the WASM boundary.
//!
//! These types convert between Materialize's row-oriented [`mz_repr::Row`] format
//! and a columnar layout suitable for WASM linear memory.

use mz_repr::{Datum, Row, SqlScalarType};

/// A batch of columnar data extracted from rows.
#[derive(Debug)]
pub struct ColumnBatch {
    pub num_rows: usize,
    pub columns: Vec<TypedColumn>,
}

/// A single column of typed data with a validity bitmap.
#[derive(Debug, Clone)]
pub enum TypedColumn {
    Bool {
        values: Vec<bool>,
        validity: Vec<bool>,
    },
    Int16 {
        values: Vec<i16>,
        validity: Vec<bool>,
    },
    Int32 {
        values: Vec<i32>,
        validity: Vec<bool>,
    },
    Int64 {
        values: Vec<i64>,
        validity: Vec<bool>,
    },
    Float32 {
        values: Vec<f32>,
        validity: Vec<bool>,
    },
    Float64 {
        values: Vec<f64>,
        validity: Vec<bool>,
    },
    String {
        /// Concatenated string bytes.
        data: Vec<u8>,
        /// Byte offsets into `data`. Length is `num_rows + 1`.
        offsets: Vec<i32>,
        validity: Vec<bool>,
    },
}

/// Result of evaluating a compiled expression on a column batch.
#[derive(Debug)]
pub struct ResultColumn {
    /// The computed output column.
    pub column: TypedColumn,
    /// Per-row error flag.
    pub errors: Vec<bool>,
    /// Error code for rows where `errors[i]` is true.
    /// Uses `EvalError` discriminant values.
    pub error_codes: Vec<u32>,
}

impl TypedColumn {
    /// Returns the number of rows in this column.
    pub fn len(&self) -> usize {
        match self {
            TypedColumn::Bool { validity, .. }
            | TypedColumn::Int16 { validity, .. }
            | TypedColumn::Int32 { validity, .. }
            | TypedColumn::Int64 { validity, .. }
            | TypedColumn::Float32 { validity, .. }
            | TypedColumn::Float64 { validity, .. }
            | TypedColumn::String { validity, .. } => validity.len(),
        }
    }

    /// Returns whether this column has zero rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the validity bitmap.
    pub fn validity(&self) -> &[bool] {
        match self {
            TypedColumn::Bool { validity, .. }
            | TypedColumn::Int16 { validity, .. }
            | TypedColumn::Int32 { validity, .. }
            | TypedColumn::Int64 { validity, .. }
            | TypedColumn::Float32 { validity, .. }
            | TypedColumn::Float64 { validity, .. }
            | TypedColumn::String { validity, .. } => validity,
        }
    }

    /// Allocates an empty column of the given type with capacity for `n` rows.
    pub fn new_for_type(scalar_type: &SqlScalarType, n: usize) -> Self {
        match scalar_type {
            SqlScalarType::Bool => TypedColumn::Bool {
                values: vec![false; n],
                validity: vec![false; n],
            },
            SqlScalarType::Int16 => TypedColumn::Int16 {
                values: vec![0; n],
                validity: vec![false; n],
            },
            SqlScalarType::Int32 => TypedColumn::Int32 {
                values: vec![0; n],
                validity: vec![false; n],
            },
            SqlScalarType::Int64 => TypedColumn::Int64 {
                values: vec![0; n],
                validity: vec![false; n],
            },
            SqlScalarType::Float32 => TypedColumn::Float32 {
                values: vec![0.0; n],
                validity: vec![false; n],
            },
            SqlScalarType::Float64 => TypedColumn::Float64 {
                values: vec![0.0; n],
                validity: vec![false; n],
            },
            SqlScalarType::String => TypedColumn::String {
                data: Vec::new(),
                offsets: vec![0; n + 1],
                validity: vec![false; n],
            },
            _ => TypedColumn::Int64 {
                values: vec![0; n],
                validity: vec![false; n],
            },
        }
    }
}

/// Converts a slice of rows into a [`ColumnBatch`] given the expected column types.
///
/// Each `input_types` entry describes one column's scalar type and nullability.
/// Rows are unpacked datum-by-datum into the corresponding typed column vectors.
pub fn rows_to_columns(rows: &[Row], input_types: &[(SqlScalarType, bool)]) -> ColumnBatch {
    let num_rows = rows.len();
    let num_cols = input_types.len();

    let mut columns: Vec<TypedColumn> = input_types
        .iter()
        .map(|(st, _)| TypedColumn::new_for_type(st, num_rows))
        .collect();

    for (row_idx, row) in rows.iter().enumerate() {
        let datums: Vec<Datum<'_>> = row.unpack();
        for col_idx in 0..num_cols {
            let datum = if col_idx < datums.len() {
                datums[col_idx]
            } else {
                Datum::Null
            };
            set_datum(&mut columns[col_idx], row_idx, datum);
        }
    }

    ColumnBatch { num_rows, columns }
}

/// Converts a [`ColumnBatch`] back to rows, selecting the given projection columns.
pub fn columns_to_rows(batch: &ColumnBatch, projection: &[usize]) -> Vec<Row> {
    let mut rows = Vec::with_capacity(batch.num_rows);
    for row_idx in 0..batch.num_rows {
        let datums: Vec<Datum<'_>> = projection
            .iter()
            .map(|&col_idx| get_datum(&batch.columns[col_idx], row_idx))
            .collect();
        rows.push(Row::pack_slice(&datums));
    }
    rows
}

/// Writes a datum value into a typed column at the given row index.
fn set_datum(column: &mut TypedColumn, row_idx: usize, datum: Datum<'_>) {
    match column {
        TypedColumn::Bool { values, validity } => {
            if let Datum::True = datum {
                values[row_idx] = true;
                validity[row_idx] = true;
            } else if let Datum::False = datum {
                values[row_idx] = false;
                validity[row_idx] = true;
            }
            // Null: validity stays false
        }
        TypedColumn::Int16 { values, validity } => {
            if let Datum::Int16(v) = datum {
                values[row_idx] = v;
                validity[row_idx] = true;
            }
        }
        TypedColumn::Int32 { values, validity } => {
            if let Datum::Int32(v) = datum {
                values[row_idx] = v;
                validity[row_idx] = true;
            }
        }
        TypedColumn::Int64 { values, validity } => {
            if let Datum::Int64(v) = datum {
                values[row_idx] = v;
                validity[row_idx] = true;
            }
        }
        TypedColumn::Float32 { values, validity } => {
            if let Datum::Float32(v) = datum {
                values[row_idx] = v.into_inner();
                validity[row_idx] = true;
            }
        }
        TypedColumn::Float64 { values, validity } => {
            if let Datum::Float64(v) = datum {
                values[row_idx] = v.into_inner();
                validity[row_idx] = true;
            }
        }
        TypedColumn::String {
            data,
            offsets,
            validity,
        } => {
            if let Datum::String(s) = datum {
                data.extend_from_slice(s.as_bytes());
                validity[row_idx] = true;
            }
            // Update the offset for the next row.
            #[allow(clippy::as_conversions)]
            if row_idx + 1 < offsets.len() {
                offsets[row_idx + 1] = data.len() as i32;
            }
        }
    }
}

/// Reads a datum from a typed column at the given row index.
fn get_datum<'a>(column: &'a TypedColumn, row_idx: usize) -> Datum<'a> {
    match column {
        TypedColumn::Bool { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else if values[row_idx] {
                Datum::True
            } else {
                Datum::False
            }
        }
        TypedColumn::Int16 { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                Datum::Int16(values[row_idx])
            }
        }
        TypedColumn::Int32 { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                Datum::Int32(values[row_idx])
            }
        }
        TypedColumn::Int64 { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                Datum::Int64(values[row_idx])
            }
        }
        TypedColumn::Float32 { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                Datum::Float32(values[row_idx].into())
            }
        }
        TypedColumn::Float64 { values, validity } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                Datum::Float64(values[row_idx].into())
            }
        }
        TypedColumn::String {
            data,
            offsets,
            validity,
        } => {
            if !validity[row_idx] {
                Datum::Null
            } else {
                #[allow(clippy::as_conversions)]
                let start = offsets[row_idx] as usize;
                #[allow(clippy::as_conversions)]
                let end = offsets[row_idx + 1] as usize;
                let bytes = &data[start..end];
                // Safety: we only store valid UTF-8 from Datum::String.
                let s = std::str::from_utf8(bytes).expect("valid UTF-8 in string column");
                Datum::String(s)
            }
        }
    }
}

impl ResultColumn {
    /// Creates a new result column from an output typed column.
    pub fn new(column: TypedColumn) -> Self {
        let n = column.len();
        ResultColumn {
            column,
            errors: vec![false; n],
            error_codes: vec![0; n],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::{Datum, Row, SqlScalarType};

    #[mz_ore::test]
    fn test_round_trip_int64() {
        let rows = vec![
            Row::pack_slice(&[Datum::Int64(1), Datum::Int64(2)]),
            Row::pack_slice(&[Datum::Int64(3), Datum::Int64(4)]),
            Row::pack_slice(&[Datum::Null, Datum::Int64(5)]),
        ];
        let types = vec![(SqlScalarType::Int64, true), (SqlScalarType::Int64, true)];

        let batch = rows_to_columns(&rows, &types);
        assert_eq!(batch.num_rows, 3);
        assert_eq!(batch.columns.len(), 2);

        let result = columns_to_rows(&batch, &[0, 1]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].unpack(), vec![Datum::Int64(1), Datum::Int64(2)]);
        assert_eq!(result[1].unpack(), vec![Datum::Int64(3), Datum::Int64(4)]);
        assert_eq!(result[2].unpack(), vec![Datum::Null, Datum::Int64(5)]);
    }

    #[mz_ore::test]
    fn test_round_trip_bool() {
        let rows = vec![
            Row::pack_slice(&[Datum::True]),
            Row::pack_slice(&[Datum::False]),
            Row::pack_slice(&[Datum::Null]),
        ];
        let types = vec![(SqlScalarType::Bool, true)];

        let batch = rows_to_columns(&rows, &types);
        let result = columns_to_rows(&batch, &[0]);
        assert_eq!(result[0].unpack(), vec![Datum::True]);
        assert_eq!(result[1].unpack(), vec![Datum::False]);
        assert_eq!(result[2].unpack(), vec![Datum::Null]);
    }
}
