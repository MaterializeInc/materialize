// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Vectorized (columnar) evaluation of `MapFilterProject` plans.
//!
//! Instead of evaluating expressions row-by-row, this module operates on
//! columns of data, amortizing interpretation overhead over many rows.
//!
//! The key type is [`ColumnDatum`], which mirrors [`mz_repr::Datum`] but is
//! owned and laid out in columnar form via the `columnar` crate. When a
//! column is homogeneous (all values are the same variant, e.g. all `Int64`),
//! we can extract the typed container and operate on it in bulk. When it is
//! not homogeneous (e.g. contains errors or mixed types), we fall back to
//! row-at-a-time evaluation.

use columnar::{Columnar, Len, Push};

use crate::{BinaryFunc, MirScalarExpr};

/// An owned datum value suitable for columnar storage.
///
/// This mirrors [`mz_repr::Datum`] but is `'static` and `Columnar`-derivable.
/// When stored in a columnar container, all `Int32` values are stored together
/// in a `Vec<i32>`, all `Int64` values in a `Vec<i64>`, etc.
///
/// Additional `Datum` variants (Date, Timestamp, Numeric, etc.) will be added
/// as we expand vectorized evaluation to cover more types.
#[derive(Clone, Debug, Columnar)]
pub enum ColumnDatum {
    /// Null value.
    Null,
    /// Boolean true.
    True,
    /// Boolean false.
    False,
    /// 16-bit signed integer.
    Int16(i16),
    /// 32-bit signed integer.
    Int32(i32),
    /// 64-bit signed integer.
    Int64(i64),
    /// 8-bit unsigned integer.
    UInt8(u8),
    /// 16-bit unsigned integer.
    UInt16(u16),
    /// 32-bit unsigned integer.
    UInt32(u32),
    /// 64-bit unsigned integer.
    UInt64(u64),
    /// 32-bit floating point number.
    Float32(f32),
    /// 64-bit floating point number.
    Float64(f64),
    /// An error produced during evaluation.
    Error(String),
}

/// A column of datum values in columnar layout.
///
/// The inner container is the `columnar`-derived container for `ColumnDatum`.
/// When the column is homogeneous (e.g. all `Int64`), we can access the
/// underlying typed vector directly.
pub struct DatumColumn {
    /// The columnar container holding all values.
    // Debug is not derived because the generated container type is complex.
    pub data: <ColumnDatum as Columnar>::Container,
}

impl std::fmt::Debug for DatumColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatumColumn")
            .field("len", &self.len())
            .finish()
    }
}

impl DatumColumn {
    /// Create a new empty column.
    pub fn new() -> Self {
        DatumColumn {
            data: Default::default(),
        }
    }

    /// The number of values in this column.
    pub fn len(&self) -> usize {
        Len::len(&self.data)
    }

    /// Whether this column is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Push a datum value into this column.
    pub fn push(&mut self, datum: ColumnDatum) {
        Push::push(&mut self.data, &datum);
    }
}

/// Convert an iterator of [`Row`] references into columnar form.
///
/// Each row is unpacked into datums, and the datums at each position are
/// collected into a [`DatumColumn`]. The result has one `DatumColumn` per
/// column (i.e., per datum position in the rows).
///
/// All rows must have the same arity.
pub fn rows_to_columns<'a>(rows: impl Iterator<Item = &'a mz_repr::Row>, arity: usize) -> Vec<DatumColumn> {
    let mut columns: Vec<DatumColumn> = (0..arity).map(|_| DatumColumn::new()).collect();
    for row in rows {
        let datums = row.unpack();
        debug_assert_eq!(datums.len(), arity);
        for (col_idx, datum) in datums.into_iter().enumerate() {
            columns[col_idx].push(datum_to_column_datum(datum));
        }
    }
    columns
}

/// A vectorized expression tree, compiled from [`MirScalarExpr`].
///
/// Each variant mirrors the corresponding `MirScalarExpr` variant, but
/// evaluation operates on entire columns rather than individual datums.
///
/// The `Scalar` variant provides a fallback for expressions that have not
/// yet been ported to vectorized evaluation.
#[derive(Debug)]
pub enum VectorScalarExpr {
    /// Reference to an input or previously-computed column.
    Column(usize),
    /// A constant value, broadcast to fill the batch.
    Literal(ColumnDatum),
    /// A binary function applied to two sub-expressions.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<VectorScalarExpr>,
        expr2: Box<VectorScalarExpr>,
    },
    /// Fallback: evaluate row-at-a-time using the original scalar expression.
    Scalar(MirScalarExpr),
}

impl VectorScalarExpr {
    /// Try to convert a `MirScalarExpr` into a vectorized expression.
    ///
    /// Returns `None` if the expression (or any sub-expression) cannot be
    /// vectorized, in which case the caller should use the `Scalar` fallback.
    pub fn try_from_mir(expr: &MirScalarExpr) -> Option<Self> {
        match expr {
            MirScalarExpr::Column(index, _name) => Some(VectorScalarExpr::Column(*index)),
            MirScalarExpr::Literal(Ok(row), _typ) => {
                let datum = row.unpack_first();
                let cd = column_datum_from_datum(datum)?;
                Some(VectorScalarExpr::Literal(cd))
            }
            MirScalarExpr::Literal(Err(_), _typ) => {
                // Error literals: we could represent these, but for now
                // fall back to scalar.
                None
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                if binary_func_is_vectorized(func) {
                    let e1 = VectorScalarExpr::try_from_mir(expr1)?;
                    let e2 = VectorScalarExpr::try_from_mir(expr2)?;
                    Some(VectorScalarExpr::CallBinary {
                        func: func.clone(),
                        expr1: Box::new(e1),
                        expr2: Box::new(e2),
                    })
                } else {
                    None
                }
            }
            // All other expression types fall back to scalar for now.
            _ => None,
        }
    }

    /// Convert a `MirScalarExpr` into a vectorized expression, using the
    /// `Scalar` fallback for any sub-expression that cannot be vectorized.
    pub fn from_mir_or_scalar(expr: &MirScalarExpr) -> Self {
        Self::try_from_mir(expr).unwrap_or_else(|| VectorScalarExpr::Scalar(expr.clone()))
    }

    /// Evaluate this expression against a set of input columns, producing
    /// a new column of results.
    ///
    /// `columns` contains the input columns (from the original row) plus
    /// any columns computed by prior expressions.
    pub fn eval(&self, columns: &[&DatumColumn], batch_len: usize) -> DatumColumn {
        match self {
            VectorScalarExpr::Column(index) => {
                // Return a reference to the existing column.
                // For now, we clone the data. A future optimization would use
                // reference-counted or borrowed columns.
                let src = columns[*index];
                let mut result = DatumColumn::new();
                // Copy column data element by element.
                // We borrow the container to get an indexable form.
                let borrowed = columnar::Borrow::borrow(&src.data);
                for i in 0..src.len() {
                    let datum = columnar::Index::get(&borrowed, i);
                    result.data.push(datum);
                }
                result
            }
            VectorScalarExpr::Literal(datum) => {
                // Broadcast the literal to fill the batch.
                let mut result = DatumColumn::new();
                for _ in 0..batch_len {
                    result.push(datum.clone());
                }
                result
            }
            VectorScalarExpr::CallBinary { func, expr1, expr2 } => {
                // When both sub-expressions are column references, pass the
                // existing columns directly to avoid copying them.
                match (expr1.as_ref(), expr2.as_ref()) {
                    (VectorScalarExpr::Column(i1), VectorScalarExpr::Column(i2)) => {
                        eval_binary_vectorized(func, columns[*i1], columns[*i2], batch_len)
                    }
                    (VectorScalarExpr::Column(i1), _) => {
                        let col2 = expr2.eval(columns, batch_len);
                        eval_binary_vectorized(func, columns[*i1], &col2, batch_len)
                    }
                    (_, VectorScalarExpr::Column(i2)) => {
                        let col1 = expr1.eval(columns, batch_len);
                        eval_binary_vectorized(func, &col1, columns[*i2], batch_len)
                    }
                    _ => {
                        let col1 = expr1.eval(columns, batch_len);
                        let col2 = expr2.eval(columns, batch_len);
                        eval_binary_vectorized(func, &col1, &col2, batch_len)
                    }
                }
            }
            VectorScalarExpr::Scalar(expr) => {
                // Fallback: evaluate row-at-a-time.
                eval_scalar_fallback(expr, columns, batch_len)
            }
        }
    }
}

/// Returns `true` if the given binary function has a vectorized implementation.
fn binary_func_is_vectorized(func: &BinaryFunc) -> bool {
    use crate::func;
    matches!(
        func,
        BinaryFunc::AddInt16(func::AddInt16)
            | BinaryFunc::AddInt32(func::AddInt32)
            | BinaryFunc::AddInt64(func::AddInt64)
            | BinaryFunc::SubInt16(func::SubInt16)
            | BinaryFunc::SubInt32(func::SubInt32)
            | BinaryFunc::SubInt64(func::SubInt64)
            | BinaryFunc::MulInt16(func::MulInt16)
            | BinaryFunc::MulInt32(func::MulInt32)
            | BinaryFunc::MulInt64(func::MulInt64)
    )
}

/// Evaluate a binary function on two columns, producing a result column.
///
/// When both input columns are homogeneous and of the expected type, this
/// operates directly on the typed vectors. Otherwise, falls back to
/// element-at-a-time evaluation.
fn eval_binary_vectorized(
    func: &BinaryFunc,
    col1: &DatumColumn,
    col2: &DatumColumn,
    batch_len: usize,
) -> DatumColumn {
    use crate::func;

    // Try the fast path: both columns are homogeneous and the right type.
    // The columnar container for `ColumnDatum` has a field per variant.
    // We check if a variant's container has length == batch_len, meaning
    // all values are that variant.
    let data1 = &col1.data;
    let data2 = &col2.data;

    match func {
        BinaryFunc::AddInt64(func::AddInt64) => {
            if Len::len(&data1.Int64) == batch_len && Len::len(&data2.Int64) == batch_len {
                return eval_add_int64_vectorized(&data1.Int64, &data2.Int64);
            }
        }
        BinaryFunc::SubInt64(func::SubInt64) => {
            if Len::len(&data1.Int64) == batch_len && Len::len(&data2.Int64) == batch_len {
                return eval_sub_int64_vectorized(&data1.Int64, &data2.Int64);
            }
        }
        BinaryFunc::MulInt64(func::MulInt64) => {
            if Len::len(&data1.Int64) == batch_len && Len::len(&data2.Int64) == batch_len {
                return eval_mul_int64_vectorized(&data1.Int64, &data2.Int64);
            }
        }
        BinaryFunc::AddInt32(func::AddInt32) => {
            if Len::len(&data1.Int32) == batch_len && Len::len(&data2.Int32) == batch_len {
                return eval_add_int32_vectorized(&data1.Int32, &data2.Int32);
            }
        }
        BinaryFunc::SubInt32(func::SubInt32) => {
            if Len::len(&data1.Int32) == batch_len && Len::len(&data2.Int32) == batch_len {
                return eval_sub_int32_vectorized(&data1.Int32, &data2.Int32);
            }
        }
        BinaryFunc::MulInt32(func::MulInt32) => {
            if Len::len(&data1.Int32) == batch_len && Len::len(&data2.Int32) == batch_len {
                return eval_mul_int32_vectorized(&data1.Int32, &data2.Int32);
            }
        }
        BinaryFunc::AddInt16(func::AddInt16) => {
            if Len::len(&data1.Int16) == batch_len && Len::len(&data2.Int16) == batch_len {
                return eval_add_int16_vectorized(&data1.Int16, &data2.Int16);
            }
        }
        BinaryFunc::SubInt16(func::SubInt16) => {
            if Len::len(&data1.Int16) == batch_len && Len::len(&data2.Int16) == batch_len {
                return eval_sub_int16_vectorized(&data1.Int16, &data2.Int16);
            }
        }
        BinaryFunc::MulInt16(func::MulInt16) => {
            if Len::len(&data1.Int16) == batch_len && Len::len(&data2.Int16) == batch_len {
                return eval_mul_int16_vectorized(&data1.Int16, &data2.Int16);
            }
        }
        _ => {}
    }

    // Slow path: element-at-a-time through the columnar container.
    eval_binary_slow(func, col1, col2, batch_len)
}

/// Discriminant values for `ColumnDatum` variants in the columnar container.
/// These must match the enum definition order.
const DISCRIMINANT_INT16: u8 = 3;
const DISCRIMINANT_INT32: u8 = 4;
const DISCRIMINANT_INT64: u8 = 5;
const DISCRIMINANT_ERROR: u8 = 12;

/// Build a `DatumColumn` directly from a homogeneous `Vec<T>` and its
/// discriminant, bypassing per-element enum dispatch.
///
/// This constructs the columnar container's `variant` and `offset` arrays
/// in bulk, which is much faster than pushing elements one at a time.
macro_rules! datum_column_from_typed_vec {
    ($vec:expr, $discriminant:expr, $field:ident) => {{
        let len = $vec.len();
        let mut data: <ColumnDatum as Columnar>::Container = Default::default();
        data.$field = $vec;
        data.variant = vec![$discriminant; len];
        data.offset = (0..len as u64).collect();
        DatumColumn { data }
    }};
}

/// Build a `DatumColumn` that is mostly one typed vec but with some error
/// positions. `results` has the computed values, `errors` has indices where
/// overflow occurred.
macro_rules! datum_column_from_typed_vec_with_errors {
    ($results:expr, $errors:expr, $err_msg:expr, $discriminant:expr, $field:ident) => {{
        if $errors.is_empty() {
            // Common case: no errors at all.
            datum_column_from_typed_vec!($results, $discriminant, $field)
        } else {
            // Rare case: some overflows. Build element by element but only
            // for the error positions; the rest go in bulk.
            let len = $results.len();
            let mut data: <ColumnDatum as Columnar>::Container = Default::default();
            // Reserve space for the non-error values.
            data.$field.reserve(len - $errors.len());
            // No reserve for Strings type — it doesn't support it.
            data.variant = Vec::with_capacity(len);
            data.offset = Vec::with_capacity(len);

            let mut error_idx = 0;
            let mut typed_offset = 0u64;
            let mut error_offset = 0u64;
            for i in 0..len {
                if error_idx < $errors.len() && $errors[error_idx] == i {
                    data.variant.push(DISCRIMINANT_ERROR);
                    data.offset.push(error_offset);
                    Push::push(&mut data.Error, $err_msg);
                    error_offset += 1;
                    error_idx += 1;
                } else {
                    data.variant.push($discriminant);
                    data.offset.push(typed_offset);
                    data.$field.push($results[i]);
                    typed_offset += 1;
                }
            }
            DatumColumn { data }
        }
    }};
}

/// Detect overflow for addition: `(a ^ result) & (b ^ result) < 0`.
///
/// Uses a vectorized OR-reduction to check if *any* overflow occurred,
/// and only collects the specific indices in the (rare) overflow case.
#[inline]
fn detect_add_overflow<T>(a: &[T], b: &[T], results: &[T]) -> Vec<usize>
where
    T: std::ops::BitXor<Output = T> + std::ops::BitAnd<Output = T> + std::ops::BitOr<Output = T> + Ord + Default + Copy,
{
    // Vectorized OR-reduction: accumulate all overflow indicators.
    // No branches — the compiler can SIMD this into a horizontal OR.
    let any_overflow = a.iter().zip(b.iter()).zip(results.iter())
        .fold(T::default(), |acc, ((x, y), r)| acc | ((*x ^ *r) & (*y ^ *r)));
    if any_overflow < T::default() {
        // Rare path: at least one overflow, find which ones.
        (0..a.len())
            .filter(|&i| (a[i] ^ results[i]) & (b[i] ^ results[i]) < T::default())
            .collect()
    } else {
        Vec::new()
    }
}

/// Detect overflow for subtraction: `(a ^ b) & (a ^ result) < 0`.
#[inline]
fn detect_sub_overflow<T>(a: &[T], b: &[T], results: &[T]) -> Vec<usize>
where
    T: std::ops::BitXor<Output = T> + std::ops::BitAnd<Output = T> + std::ops::BitOr<Output = T> + Ord + Default + Copy,
{
    let any_overflow = a.iter().zip(b.iter()).zip(results.iter())
        .fold(T::default(), |acc, ((x, y), r)| acc | ((*x ^ *y) & (*x ^ *r)));
    if any_overflow < T::default() {
        (0..a.len())
            .filter(|&i| (a[i] ^ b[i]) & (a[i] ^ results[i]) < T::default())
            .collect()
    } else {
        Vec::new()
    }
}

/// Vectorized add for `i64` columns.
///
/// Uses wrapping arithmetic in a tight loop (SIMD-friendly), then detects
/// overflows via a vectorized OR-reduction.
#[inline]
fn eval_add_int64_vectorized(a: &[i64], b: &[i64]) -> DatumColumn {
    let arith_start = std::time::Instant::now();
    let results: Vec<i64> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_add(*y)).collect();
    let arith_ns = arith_start.elapsed().as_nanos();

    let overflow_start = std::time::Instant::now();
    let errors = detect_add_overflow(a, b, &results);
    let overflow_ns = overflow_start.elapsed().as_nanos();

    let container_start = std::time::Instant::now();
    let result = datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT64, Int64);
    let container_ns = container_start.elapsed().as_nanos();

    tracing::info!(
        len = a.len(),
        arith_ns = arith_ns as u64,
        overflow_ns = overflow_ns as u64,
        container_ns = container_ns as u64,
        "int64 add breakdown"
    );
    result
}

/// Vectorized subtract for `i64` columns.
#[inline]
fn eval_sub_int64_vectorized(a: &[i64], b: &[i64]) -> DatumColumn {
    let results: Vec<i64> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_sub(*y)).collect();
    let errors = detect_sub_overflow(a, b, &results);
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT64, Int64)
}

/// Vectorized multiply for `i64` columns.
///
/// Multiplication overflow cannot be detected with a simple bit trick, so we
/// use `checked_mul` but still write results into a bulk `Vec<i64>`.
#[inline]
fn eval_mul_int64_vectorized(a: &[i64], b: &[i64]) -> DatumColumn {
    let len = a.len();
    let mut results = Vec::with_capacity(len);
    let mut errors = Vec::new();
    for i in 0..len {
        match a[i].checked_mul(b[i]) {
            Some(v) => results.push(v),
            None => {
                results.push(0); // placeholder
                errors.push(i);
            }
        }
    }
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT64, Int64)
}

/// Vectorized add for `i32` columns.
#[inline]
fn eval_add_int32_vectorized(a: &[i32], b: &[i32]) -> DatumColumn {
    let results: Vec<i32> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_add(*y)).collect();
    let errors = detect_add_overflow(a, b, &results);
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT32, Int32)
}

/// Vectorized subtract for `i32` columns.
#[inline]
fn eval_sub_int32_vectorized(a: &[i32], b: &[i32]) -> DatumColumn {
    let results: Vec<i32> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_sub(*y)).collect();
    let errors = detect_sub_overflow(a, b, &results);
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT32, Int32)
}

/// Vectorized multiply for `i32` columns.
#[inline]
fn eval_mul_int32_vectorized(a: &[i32], b: &[i32]) -> DatumColumn {
    let len = a.len();
    let mut results = Vec::with_capacity(len);
    let mut errors = Vec::new();
    for i in 0..len {
        match a[i].checked_mul(b[i]) {
            Some(v) => results.push(v),
            None => {
                results.push(0);
                errors.push(i);
            }
        }
    }
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT32, Int32)
}

/// Vectorized add for `i16` columns.
#[inline]
fn eval_add_int16_vectorized(a: &[i16], b: &[i16]) -> DatumColumn {
    let results: Vec<i16> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_add(*y)).collect();
    let errors = detect_add_overflow(a, b, &results);
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT16, Int16)
}

/// Vectorized subtract for `i16` columns.
#[inline]
fn eval_sub_int16_vectorized(a: &[i16], b: &[i16]) -> DatumColumn {
    let results: Vec<i16> = a.iter().zip(b.iter()).map(|(x, y)| x.wrapping_sub(*y)).collect();
    let errors = detect_sub_overflow(a, b, &results);
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT16, Int16)
}

/// Vectorized multiply for `i16` columns.
#[inline]
fn eval_mul_int16_vectorized(a: &[i16], b: &[i16]) -> DatumColumn {
    let len = a.len();
    let mut results = Vec::with_capacity(len);
    let mut errors = Vec::new();
    for i in 0..len {
        match a[i].checked_mul(b[i]) {
            Some(v) => results.push(v),
            None => {
                results.push(0);
                errors.push(i);
            }
        }
    }
    datum_column_from_typed_vec_with_errors!(results, errors, "integer out of range", DISCRIMINANT_INT16, Int16)
}

/// Slow path: evaluate a binary function element-at-a-time.
///
/// Constructs a `MirScalarExpr::CallBinary` that reads from column positions
/// 0 and 1 of a synthetic two-element datum row, then evaluates per-element.
fn eval_binary_slow(
    func: &BinaryFunc,
    col1: &DatumColumn,
    col2: &DatumColumn,
    batch_len: usize,
) -> DatumColumn {
    use mz_repr::RowArena;

    // Build a MirScalarExpr that applies this function to columns 0 and 1.
    let synth_expr = MirScalarExpr::CallBinary {
        func: func.clone(),
        expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
        expr2: Box::new(MirScalarExpr::Column(1, Default::default())),
    };

    let arena = RowArena::new();
    let mut result = DatumColumn::new();
    for i in 0..batch_len {
        let d1 = index_as_datum(&col1.data, i, &arena);
        let d2 = index_as_datum(&col2.data, i, &arena);
        match (d1, d2) {
            (Ok(a), Ok(b)) => {
                let datums = [a, b];
                match synth_expr.eval(&datums, &arena) {
                    Ok(d) => result.push(datum_to_column_datum(d)),
                    Err(e) => result.push(ColumnDatum::Error(e.to_string())),
                }
            }
            (Err(e), _) | (_, Err(e)) => {
                result.push(ColumnDatum::Error(e));
            }
        }
    }
    result
}

/// Fallback: evaluate a MirScalarExpr row-at-a-time over columnar input.
fn eval_scalar_fallback(
    expr: &MirScalarExpr,
    columns: &[&DatumColumn],
    batch_len: usize,
) -> DatumColumn {
    use mz_repr::{Datum, RowArena};

    let arena = RowArena::new();
    let mut result = DatumColumn::new();
    let num_cols = columns.len();

    for row_idx in 0..batch_len {
        // Reconstruct a row of datums from the columns.
        let mut datums: Vec<Datum<'_>> = Vec::with_capacity(num_cols);
        let mut has_error = false;
        for col in columns.iter() {
            match index_as_datum(&col.data, row_idx, &arena) {
                Ok(d) => datums.push(d),
                Err(e) => {
                    result.push(ColumnDatum::Error(e));
                    has_error = true;
                    break;
                }
            }
        }
        if has_error {
            continue;
        }
        match expr.eval(&datums, &arena) {
            Ok(d) => result.push(datum_to_column_datum(d)),
            Err(e) => result.push(ColumnDatum::Error(e.to_string())),
        }
    }
    result
}

/// Convert a `Datum` to a `ColumnDatum`.
///
/// Types not yet represented in `ColumnDatum` produce an `Error` variant,
/// which will cause a fallback to scalar evaluation when encountered.
fn datum_to_column_datum(datum: mz_repr::Datum<'_>) -> ColumnDatum {
    use mz_repr::Datum;
    match datum {
        Datum::Null => ColumnDatum::Null,
        Datum::True => ColumnDatum::True,
        Datum::False => ColumnDatum::False,
        Datum::Int16(v) => ColumnDatum::Int16(v),
        Datum::Int32(v) => ColumnDatum::Int32(v),
        Datum::Int64(v) => ColumnDatum::Int64(v),
        Datum::UInt8(v) => ColumnDatum::UInt8(v),
        Datum::UInt16(v) => ColumnDatum::UInt16(v),
        Datum::UInt32(v) => ColumnDatum::UInt32(v),
        Datum::UInt64(v) => ColumnDatum::UInt64(v),
        Datum::Float32(v) => ColumnDatum::Float32(v.into_inner()),
        Datum::Float64(v) => ColumnDatum::Float64(v.into_inner()),
        other => ColumnDatum::Error(format!("unsupported datum type: {:?}", other)),
    }
}

/// Convert a `ColumnDatum` (from a `Datum` reference into the columnar container)
/// to a `mz_repr::Datum`, using the arena for any allocations.
pub fn index_as_datum<'a>(
    data: &<ColumnDatum as Columnar>::Container,
    index: usize,
    _arena: &'a mz_repr::RowArena,
) -> Result<mz_repr::Datum<'a>, String> {
    use columnar::{Borrow, Index};
    use mz_repr::Datum;
    use ordered_float::OrderedFloat;

    let borrowed = Borrow::borrow(data);
    let ref_val = borrowed.get(index);
    match ref_val {
        ColumnDatumReference::Null(_) => Ok(Datum::Null),
        ColumnDatumReference::True(_) => Ok(Datum::True),
        ColumnDatumReference::False(_) => Ok(Datum::False),
        ColumnDatumReference::Int16(v) => Ok(Datum::Int16(*v)),
        ColumnDatumReference::Int32(v) => Ok(Datum::Int32(*v)),
        ColumnDatumReference::Int64(v) => Ok(Datum::Int64(*v)),
        ColumnDatumReference::UInt8(v) => Ok(Datum::UInt8(*v)),
        ColumnDatumReference::UInt16(v) => Ok(Datum::UInt16(*v)),
        ColumnDatumReference::UInt32(v) => Ok(Datum::UInt32(*v)),
        ColumnDatumReference::UInt64(v) => Ok(Datum::UInt64(*v)),
        ColumnDatumReference::Float32(v) => Ok(Datum::Float32(OrderedFloat(*v))),
        ColumnDatumReference::Float64(v) => Ok(Datum::Float64(OrderedFloat(*v))),
        ColumnDatumReference::Error(e) => Err(e.to_string()),
    }
}

/// Try to convert a `Datum` to a `ColumnDatum`.
/// Returns `None` for types not yet supported.
fn column_datum_from_datum(datum: mz_repr::Datum<'_>) -> Option<ColumnDatum> {
    use mz_repr::Datum;
    match datum {
        Datum::Null => Some(ColumnDatum::Null),
        Datum::True => Some(ColumnDatum::True),
        Datum::False => Some(ColumnDatum::False),
        Datum::Int16(v) => Some(ColumnDatum::Int16(v)),
        Datum::Int32(v) => Some(ColumnDatum::Int32(v)),
        Datum::Int64(v) => Some(ColumnDatum::Int64(v)),
        Datum::UInt8(v) => Some(ColumnDatum::UInt8(v)),
        Datum::UInt16(v) => Some(ColumnDatum::UInt16(v)),
        Datum::UInt32(v) => Some(ColumnDatum::UInt32(v)),
        Datum::UInt64(v) => Some(ColumnDatum::UInt64(v)),
        Datum::Float32(v) => Some(ColumnDatum::Float32(v.into_inner())),
        Datum::Float64(v) => Some(ColumnDatum::Float64(v.into_inner())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a DatumColumn from an iterator of ColumnDatum.
    fn column_from_iter(iter: impl IntoIterator<Item = ColumnDatum>) -> DatumColumn {
        let mut col = DatumColumn::new();
        for d in iter {
            col.push(d);
        }
        col
    }

    /// Helper: extract i64 value from a column at given index, panicking on mismatch.
    fn get_i64(col: &DatumColumn, index: usize) -> i64 {
        let arena = mz_repr::RowArena::new();
        match index_as_datum(&col.data, index, &arena) {
            Ok(mz_repr::Datum::Int64(v)) => v,
            other => panic!("Expected Int64, got {:?}", other),
        }
    }

    /// Helper: check if column at index is an error.
    fn is_error(col: &DatumColumn, index: usize) -> bool {
        let arena = mz_repr::RowArena::new();
        index_as_datum(&col.data, index, &arena).is_err()
    }

    #[test]
    fn test_add_int64_vectorized() {
        let col_a = column_from_iter((0..100).map(|i| ColumnDatum::Int64(i)));
        let col_b = column_from_iter((100..200).map(|i| ColumnDatum::Int64(i)));
        let columns: Vec<&DatumColumn> = vec![&col_a, &col_b];

        let expr = VectorScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(crate::func::AddInt64),
            expr1: Box::new(VectorScalarExpr::Column(0)),
            expr2: Box::new(VectorScalarExpr::Column(1)),
        };

        let result = expr.eval(&columns, 100);
        assert_eq!(result.len(), 100);

        for i in 0..100 {
            assert_eq!(get_i64(&result, i), (i as i64) + (i as i64 + 100));
        }
    }

    #[test]
    fn test_literal_broadcast() {
        let col_a = column_from_iter((0..50).map(|i| ColumnDatum::Int64(i)));
        let columns: Vec<&DatumColumn> = vec![&col_a];

        let expr = VectorScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(crate::func::AddInt64),
            expr1: Box::new(VectorScalarExpr::Column(0)),
            expr2: Box::new(VectorScalarExpr::Literal(ColumnDatum::Int64(1000))),
        };

        let result = expr.eval(&columns, 50);
        assert_eq!(result.len(), 50);

        for i in 0..50 {
            assert_eq!(get_i64(&result, i), i as i64 + 1000);
        }
    }

    #[test]
    fn test_overflow_produces_error() {
        let col_a = column_from_iter(vec![ColumnDatum::Int64(i64::MAX)]);
        let col_b = column_from_iter(vec![ColumnDatum::Int64(1)]);
        let columns: Vec<&DatumColumn> = vec![&col_a, &col_b];

        let expr = VectorScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(crate::func::AddInt64),
            expr1: Box::new(VectorScalarExpr::Column(0)),
            expr2: Box::new(VectorScalarExpr::Column(1)),
        };

        let result = expr.eval(&columns, 1);
        assert_eq!(result.len(), 1);
        assert!(is_error(&result, 0));
    }

    #[test]
    fn test_evaluate_batch_simple() {
        use crate::MapFilterProject;
        use crate::linear::plan::SafeMfpPlan;

        // Build an MFP that computes: output = col0 + col1, no predicates,
        // projecting the computed column (index 2).
        let mfp = MapFilterProject {
            expressions: vec![
                MirScalarExpr::CallBinary {
                    func: BinaryFunc::AddInt64(crate::func::AddInt64),
                    expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
                    expr2: Box::new(MirScalarExpr::Column(1, Default::default())),
                },
            ],
            predicates: vec![],
            projection: vec![2], // output only the computed column
            input_arity: 2,
        };
        let plan = SafeMfpPlan { mfp };

        let col_a = column_from_iter((0..10).map(|i| ColumnDatum::Int64(i)));
        let col_b = column_from_iter((10..20).map(|i| ColumnDatum::Int64(i)));
        let input = vec![col_a, col_b];

        let results = plan.evaluate_batch(&input, 10);
        assert_eq!(results.len(), 10);

        for (i, result) in results.iter().enumerate() {
            let row = result.as_ref().unwrap().as_ref().unwrap();
            let datum = row.unpack_first();
            assert_eq!(datum, mz_repr::Datum::Int64(i as i64 + (i as i64 + 10)));
        }
    }

    #[test]
    fn test_evaluate_batch_with_predicate() {
        use crate::MapFilterProject;
        use crate::linear::plan::SafeMfpPlan;

        // Build an MFP: compute col0 + col1 (expr 0, column index 2),
        // then predicate: col0 > 5 (using a Gt comparison).
        // For simplicity, we'll use a CallBinary Gt predicate on col0 and a literal.
        // The predicate references columns at support level = input_arity (2),
        // meaning it's evaluated after no expressions (at column 2 boundary).
        //
        // Actually, predicates reference the support level which indicates how many
        // total columns (input + computed) must be available. Let's put the predicate
        // at support=2 (before any expressions) so it filters on input columns only.
        let gt_predicate = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gt(crate::func::Gt),
            expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
            expr2: Box::new(MirScalarExpr::Literal(
                Ok(mz_repr::Row::pack_slice(&[mz_repr::Datum::Int64(5)])),
                mz_repr::ReprColumnType {
                    scalar_type: mz_repr::ReprScalarType::Int64,
                    nullable: false,
                },
            )),
        };

        let mfp = MapFilterProject {
            expressions: vec![
                MirScalarExpr::CallBinary {
                    func: BinaryFunc::AddInt64(crate::func::AddInt64),
                    expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
                    expr2: Box::new(MirScalarExpr::Column(1, Default::default())),
                },
            ],
            predicates: vec![(2, gt_predicate)], // support=2, before expression 0
            projection: vec![2], // project the computed sum
            input_arity: 2,
        };
        let plan = SafeMfpPlan { mfp };

        let col_a = column_from_iter((0..10).map(|i| ColumnDatum::Int64(i)));
        let col_b = column_from_iter((10..20).map(|i| ColumnDatum::Int64(i)));
        let input = vec![col_a, col_b];

        let results = plan.evaluate_batch(&input, 10);
        assert_eq!(results.len(), 10);

        // Rows where col0 <= 5 should be None (filtered out).
        for i in 0..=5 {
            assert!(results[i].as_ref().unwrap().is_none(),
                "row {} should be filtered", i);
        }
        // Rows where col0 > 5 should have the sum.
        for i in 6..10 {
            let row = results[i].as_ref().unwrap().as_ref().unwrap();
            let datum = row.unpack_first();
            assert_eq!(datum, mz_repr::Datum::Int64(i as i64 + (i as i64 + 10)));
        }
    }

    #[test]
    fn test_try_from_mir() {
        use crate::MirScalarExpr;

        // A simple expression: column(0) + column(1)
        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::AddInt64(crate::func::AddInt64),
            expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
            expr2: Box::new(MirScalarExpr::Column(1, Default::default())),
        };

        let vectorized = VectorScalarExpr::try_from_mir(&expr);
        assert!(vectorized.is_some());
    }

    #[test]
    fn test_try_from_mir_unsupported_falls_back() {
        use crate::MirScalarExpr;

        // An If expression cannot be vectorized yet.
        let expr = MirScalarExpr::If {
            cond: Box::new(MirScalarExpr::Column(0, Default::default())),
            then: Box::new(MirScalarExpr::Column(1, Default::default())),
            els: Box::new(MirScalarExpr::Column(2, Default::default())),
        };

        let vectorized = VectorScalarExpr::try_from_mir(&expr);
        assert!(vectorized.is_none());

        // But from_mir_or_scalar should succeed with a Scalar fallback.
        let with_fallback = VectorScalarExpr::from_mir_or_scalar(&expr);
        assert!(matches!(with_fallback, VectorScalarExpr::Scalar(_)));
    }

    #[test]
    fn test_rows_to_columns_roundtrip() {
        use mz_repr::{Datum, Row};

        let rows: Vec<Row> = (0..100)
            .map(|i| Row::pack_slice(&[Datum::Int64(i), Datum::Int64(i * 10)]))
            .collect();

        let columns = rows_to_columns(rows.iter(), 2);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].len(), 100);
        assert_eq!(columns[1].len(), 100);

        // Verify values round-trip correctly.
        let arena = mz_repr::RowArena::new();
        for i in 0..100 {
            let d0 = index_as_datum(&columns[0].data, i, &arena).unwrap();
            let d1 = index_as_datum(&columns[1].data, i, &arena).unwrap();
            assert_eq!(d0, Datum::Int64(i as i64));
            assert_eq!(d1, Datum::Int64(i as i64 * 10));
        }
    }

    /// Compare vectorized batch evaluation against scalar row-at-a-time
    /// evaluation, ensuring they produce identical results.
    #[test]
    fn test_vectorized_matches_scalar() {
        use crate::MapFilterProject;
        use crate::linear::plan::SafeMfpPlan;
        use mz_repr::{Datum, DatumVec, Row, RowArena};

        // MFP: compute col0 + col1 (expression 0, becomes column 2),
        // predicate col0 > 3, project [2] (just the sum).
        let mfp = MapFilterProject {
            expressions: vec![
                MirScalarExpr::CallBinary {
                    func: BinaryFunc::AddInt64(crate::func::AddInt64),
                    expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
                    expr2: Box::new(MirScalarExpr::Column(1, Default::default())),
                },
            ],
            predicates: vec![(
                2,
                MirScalarExpr::CallBinary {
                    func: BinaryFunc::Gt(crate::func::Gt),
                    expr1: Box::new(MirScalarExpr::Column(0, Default::default())),
                    expr2: Box::new(MirScalarExpr::Literal(
                        Ok(Row::pack_slice(&[Datum::Int64(3)])),
                        mz_repr::ReprColumnType {
                            scalar_type: mz_repr::ReprScalarType::Int64,
                            nullable: false,
                        },
                    )),
                },
            )],
            projection: vec![2],
            input_arity: 2,
        };
        let plan = SafeMfpPlan { mfp };

        // Build test rows.
        let rows: Vec<Row> = (0..20)
            .map(|i| Row::pack_slice(&[Datum::Int64(i), Datum::Int64(100 + i)]))
            .collect();

        // --- Scalar path (row-at-a-time) ---
        let mut scalar_results: Vec<Option<Row>> = Vec::new();
        let mut datum_vec = DatumVec::new();
        for row in &rows {
            let arena = RowArena::new();
            let mut datums_local = datum_vec.borrow_with(row);
            let mut row_buf = Row::default();
            match plan.evaluate_into(&mut datums_local, &arena, &mut row_buf) {
                Ok(Some(r)) => scalar_results.push(Some(r.clone())),
                Ok(None) => scalar_results.push(None),
                Err(e) => panic!("scalar eval error: {}", e),
            }
        }

        // --- Vectorized path ---
        let columns = rows_to_columns(rows.iter(), 2);
        let batch_results = plan.evaluate_batch(&columns, rows.len());

        // --- Compare ---
        assert_eq!(scalar_results.len(), batch_results.len());
        for (i, (scalar, batch)) in scalar_results
            .iter()
            .zip(batch_results.iter())
            .enumerate()
        {
            match (scalar, batch) {
                (None, Ok(None)) => {} // both filtered
                (Some(s_row), Ok(Some(b_row))) => {
                    assert_eq!(
                        s_row, b_row,
                        "mismatch at row {}: scalar={:?}, batch={:?}",
                        i, s_row, b_row
                    );
                }
                _ => panic!(
                    "result type mismatch at row {}: scalar={:?}, batch={:?}",
                    i, scalar, batch
                ),
            }
        }
    }
}
