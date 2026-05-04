// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the PostgreSQL project. The original
// source code is subject to the terms of the PostgreSQL license, a copy of
// which can be found in the LICENSE file at the root of this repository.

//! Variadic functions.

use std::borrow::Cow;
use std::cmp;
use std::fmt;

use aws_lc_rs::hmac as aws_hmac;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use fallible_iterator::FallibleIterator;
use hmac::{Hmac, Mac};
use itertools::Itertools;
use md5::Md5;
use mz_expr_derive::sqlfunc;
use mz_lowertest::MzReflect;
use mz_ore::cast::{CastFrom, ReinterpretCast};
use mz_pgtz::timezone::TimezoneSpec;
use mz_repr::ReprColumnType;
use mz_repr::adt::array::{Array, ArrayDimension, ArrayDimensions, InvalidArrayError};
use mz_repr::adt::mz_acl_item::{AclItem, AclMode, MzAclItem};
use mz_repr::adt::range::{InvalidRangeError, Range, RangeBound, parse_range_bound_flags};
use mz_repr::adt::system::Oid;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::role_id::RoleId;
use mz_repr::{
    ColumnName, Datum, DatumList, FromDatum, InputDatumType, OptionalArg, OutputDatumType, Row,
    RowArena, SqlColumnType, SqlScalarType, Variadic,
};
use serde::{Deserialize, Serialize};

use crate::func::CaseLiteral;
use crate::func::{
    MAX_STRING_FUNC_RESULT_BYTES, array_create_scalar, build_regex, date_bin, parse_timezone,
    regexp_match_static, regexp_replace_parse_flags, regexp_split_to_array_re, stringify_datum,
    timezone_time,
};
use crate::{EvalError, MirScalarExpr};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::JsonbRef;

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct And;

impl fmt::Display for And {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("AND")
    }
}

impl LazyVariadicFunc for And {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        // If any is false, then return false. Else, if any is null, then return null. Else, return true.
        let mut null = false;
        let mut err = None;
        for expr in exprs {
            match expr.eval(datums, temp_storage) {
                Ok(Datum::False) => return Ok(Datum::False), // short-circuit
                Ok(Datum::True) => {}
                // No return in these two cases, because we might still see a false
                Ok(Datum::Null) => null = true,
                Err(this_err) => err = std::cmp::max(err.take(), Some(this_err)),
                _ => unreachable!(),
            }
        }
        match (err, null) {
            (Some(err), _) => Err(err),
            (None, true) => Ok(Datum::Null),
            (None, false) => Ok(Datum::True),
        }
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        let in_nullable = input_types.iter().any(|t| t.nullable);
        SqlScalarType::Bool.nullable(in_nullable)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn could_error(&self) -> bool {
        false
    }

    fn is_monotone(&self) -> bool {
        true
    }

    fn is_associative(&self) -> bool {
        true
    }

    fn is_infix_op(&self) -> bool {
        true
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ArrayCreate {
    pub elem_type: SqlScalarType,
}

/// Constructs a new multidimensional array out of an arbitrary number of
/// lower-dimensional arrays.
///
/// For example, if given three 1D arrays of length 2, this function will
/// construct a 2D array with dimensions 3x2.
///
/// The input datums in `datums` must all be arrays of the same dimensions.
/// (The arrays must also be of the same element type, but that is checked by
/// the SQL type system, rather than checked here at runtime.)
///
/// If all input arrays are zero-dimensional arrays, then the output is a zero-
/// dimensional array. Otherwise, the lower bound of the additional dimension is
/// one and the length of the new dimension is equal to `datums.len()`.
///
/// Null elements are allowed and considered to be zero-dimensional arrays.
#[sqlfunc(
    ArrayCreate,
    output_type_expr = "match &self.elem_type { SqlScalarType::Array(_) => self.elem_type.clone().nullable(false), _ => SqlScalarType::Array(Box::new(self.elem_type.clone())).nullable(false) }",
    introduces_nulls = false
)]
fn array_create<'a>(
    &self,
    datums: Variadic<Datum<'a>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    match &self.elem_type {
        SqlScalarType::Array(_) => array_create_multidim(&datums, temp_storage),
        _ => array_create_scalar(&datums, temp_storage),
    }
}
fn array_create_multidim<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut dim: Option<ArrayDimensions> = None;
    for datum in datums {
        let actual_dims = match datum {
            Datum::Null => ArrayDimensions::default(),
            Datum::Array(arr) => arr.dims(),
            d => panic!("unexpected datum {d}"),
        };
        if let Some(expected) = &dim {
            if actual_dims.ndims() != expected.ndims() {
                let actual = actual_dims.ndims().into();
                let expected = expected.ndims().into();
                // All input arrays must have the same dimensionality.
                return Err(InvalidArrayError::WrongCardinality { actual, expected }.into());
            }
            if let Some((e, a)) = expected
                .into_iter()
                .zip_eq(actual_dims.into_iter())
                .find(|(e, a)| e != a)
            {
                let actual = a.length;
                let expected = e.length;
                // All input arrays must have the same dimensionality.
                return Err(InvalidArrayError::WrongCardinality { actual, expected }.into());
            }
        }
        dim = Some(actual_dims);
    }
    // Per PostgreSQL, if all input arrays are zero dimensional, so is the output.
    if dim.as_ref().map_or(true, ArrayDimensions::is_empty) {
        return Ok(temp_storage.try_make_datum(|packer| packer.try_push_array(&[], &[]))?);
    }

    let mut dims = vec![ArrayDimension {
        lower_bound: 1,
        length: datums.len(),
    }];
    if let Some(d) = datums.first() {
        dims.extend(d.unwrap_array().dims());
    };
    let elements = datums
        .iter()
        .flat_map(|d| d.unwrap_array().elements().iter());
    let datum =
        temp_storage.try_make_datum(move |packer| packer.try_push_array(&dims, elements))?;
    Ok(datum)
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ArrayFill {
    pub elem_type: SqlScalarType,
}

#[sqlfunc(
    ArrayFill,
    output_type_expr = "SqlScalarType::Array(Box::new(self.elem_type.clone())).nullable(false)",
    introduces_nulls = false
)]
fn array_fill<'a>(
    &self,
    fill: Datum<'a>,
    dims: Option<Array<'a>>,
    lower_bounds: OptionalArg<Option<Array<'a>>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    const MAX_SIZE: usize = 1 << 28 - 1;
    const NULL_ARR_ERR: &str = "dimension array or low bound array";
    const NULL_ELEM_ERR: &str = "dimension values";

    if matches!(fill, Datum::Array(_)) {
        return Err(EvalError::Unsupported {
            feature: "array_fill with arrays".into(),
            discussion_no: None,
        });
    }

    let Some(arr) = dims else {
        return Err(EvalError::MustNotBeNull(NULL_ARR_ERR.into()));
    };

    let dimensions = arr
        .elements()
        .iter()
        .map(|d| match d {
            Datum::Null => Err(EvalError::MustNotBeNull(NULL_ELEM_ERR.into())),
            d => Ok(usize::cast_from(u32::reinterpret_cast(d.unwrap_int32()))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let lower_bounds = match *lower_bounds {
        Some(d) => {
            let Some(arr) = d else {
                return Err(EvalError::MustNotBeNull(NULL_ARR_ERR.into()));
            };

            arr.elements()
                .iter()
                .map(|l| match l {
                    Datum::Null => Err(EvalError::MustNotBeNull(NULL_ELEM_ERR.into())),
                    l => Ok(isize::cast_from(l.unwrap_int32())),
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        None => {
            vec![1isize; dimensions.len()]
        }
    };

    if lower_bounds.len() != dimensions.len() {
        return Err(EvalError::ArrayFillWrongArraySubscripts);
    }

    let fill_count: usize = dimensions
        .iter()
        .cloned()
        .map(Some)
        .reduce(|a, b| match (a, b) {
            (Some(a), Some(b)) => a.checked_mul(b),
            _ => None,
        })
        .flatten()
        .ok_or(EvalError::MaxArraySizeExceeded(MAX_SIZE))?;

    if matches!(
        mz_repr::datum_size(&fill).checked_mul(fill_count),
        None | Some(MAX_SIZE..)
    ) {
        return Err(EvalError::MaxArraySizeExceeded(MAX_SIZE));
    }

    let array_dimensions = if fill_count == 0 {
        vec![ArrayDimension {
            lower_bound: 1,
            length: 0,
        }]
    } else {
        dimensions
            .into_iter()
            .zip_eq(lower_bounds)
            .map(|(length, lower_bound)| ArrayDimension {
                lower_bound,
                length,
            })
            .collect()
    };

    Ok(temp_storage.try_make_datum(|packer| {
        packer.try_push_array(&array_dimensions, vec![fill; fill_count])
    })?)
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ArrayIndex {
    pub offset: i64,
}
#[sqlfunc(ArrayIndex, sqlname = "array_index", introduces_nulls = true)]
fn array_index<'a, T: FromDatum<'a>>(
    &self,
    array: Array<'a, T>,
    indices: Variadic<i64>,
) -> Option<T> {
    mz_ore::soft_assert_no_log!(
        self.offset == 0 || self.offset == 1,
        "offset must be either 0 or 1"
    );

    let dims = array.dims();
    if dims.len() != indices.len() {
        // You missed the datums "layer"
        return None;
    }

    let mut final_idx = 0;

    for (d, idx) in dims.into_iter().zip_eq(indices.iter()) {
        // Lower bound is written in terms of 1-based indexing, which offset accounts for.
        let idx = isize::cast_from(*idx + self.offset);

        let (lower, upper) = d.dimension_bounds();

        // This index missed all of the data at this layer. The dimension bounds are inclusive,
        // while range checks are exclusive, so adjust.
        if !(lower..upper + 1).contains(&idx) {
            return None;
        }

        // We discover how many indices our last index represents physically.
        final_idx *= d.length;

        // Because both index and lower bound are handled in 1-based indexing, taking their
        // difference moves us back into 0-based indexing. Similarly, if the lower bound is
        // negative, subtracting a negative value >= to itself ensures its non-negativity.
        final_idx += usize::try_from(idx - d.lower_bound)
            .expect("previous bounds check ensures physical index is at least 0");
    }

    array.elements().typed_iter().nth(final_idx)
}

#[sqlfunc]
fn array_position<'a>(
    array: Array<'a>,
    search: Datum<'a>,
    initial_pos: OptionalArg<Option<i32>>,
) -> Result<Option<i32>, EvalError> {
    if array.dims().len() > 1 {
        return Err(EvalError::MultiDimensionalArraySearch);
    }

    if search == Datum::Null {
        return Ok(None);
    }

    let skip = match initial_pos.0 {
        None => 0,
        Some(None) => return Err(EvalError::MustNotBeNull("initial position".into())),
        Some(Some(o)) => usize::try_from(o).unwrap_or(0).saturating_sub(1),
    };

    let Some(r) = array.elements().iter().skip(skip).position(|d| d == search) else {
        return Ok(None);
    };

    // Adjust count for the amount we skipped, plus 1 for adjusting to PG indexing scheme.
    let p = i32::try_from(r + skip + 1).expect("fewer than i32::MAX elements in array");
    Ok(Some(p))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ArrayToString {
    pub elem_type: SqlScalarType,
}

#[sqlfunc]
fn array_to_string<'a>(
    &self,
    array: Array<'a>,
    delimiter: &str,
    null_str_arg: OptionalArg<Option<&str>>,
) -> Result<String, EvalError> {
    // `flatten` treats absent arguments (`None`) the same as explicit NULL
    // (`Some(None)`), both becoming `None`.
    let null_str = null_str_arg.flatten();
    let mut out = String::new();
    for elem in array.elements().iter() {
        if elem.is_null() {
            if let Some(null_str) = null_str {
                out.push_str(null_str);
                out.push_str(delimiter);
            }
        } else {
            stringify_datum(&mut out, elem, &self.elem_type)?;
            out.push_str(delimiter);
        }
    }
    if out.len() > 0 {
        // Lop off last delimiter only if string is not empty
        out.truncate(out.len() - delimiter.len());
    }
    Ok(out)
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct Coalesce;

impl fmt::Display for Coalesce {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("coalesce")
    }
}

impl LazyVariadicFunc for Coalesce {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        for e in exprs {
            let d = e.eval(datums, temp_storage)?;
            if !d.is_null() {
                return Ok(d);
            }
        }
        Ok(Datum::Null)
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        // Note that the parser doesn't allow empty argument lists for variadic functions
        // that use the standard function call syntax (ArrayCreate and co. are different
        // because of the special syntax for calling them).
        let nullable = input_types.iter().all(|typ| typ.nullable);
        SqlColumnType::union_many(input_types).nullable(nullable)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn could_error(&self) -> bool {
        false
    }

    fn is_monotone(&self) -> bool {
        true
    }

    fn is_associative(&self) -> bool {
        true
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct RangeCreate {
    pub elem_type: SqlScalarType,
}

impl fmt::Display for RangeCreate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match &self.elem_type {
            SqlScalarType::Int32 => "int4range",
            SqlScalarType::Int64 => "int8range",
            SqlScalarType::Date => "daterange",
            SqlScalarType::Numeric { .. } => "numrange",
            SqlScalarType::Timestamp { .. } => "tsrange",
            SqlScalarType::TimestampTz { .. } => "tstzrange",
            _ => unreachable!(),
        })
    }
}

impl EagerVariadicFunc for RangeCreate {
    type Input<'a> = (Datum<'a>, Datum<'a>, Datum<'a>);
    type Output<'a> = Result<Datum<'a>, EvalError>;

    fn call<'a>(
        &self,
        (lower, upper, flags_datum): Self::Input<'a>,
        temp_storage: &'a RowArena,
    ) -> Self::Output<'a> {
        let flags = match flags_datum {
            Datum::Null => {
                return Err(EvalError::InvalidRange(
                    InvalidRangeError::NullRangeBoundFlags,
                ));
            }
            o => o.unwrap_str(),
        };

        let (lower_inclusive, upper_inclusive) = parse_range_bound_flags(flags)?;

        let mut range = Range::new(Some((
            RangeBound::new(lower, lower_inclusive),
            RangeBound::new(upper, upper_inclusive),
        )));

        range.canonicalize()?;

        Ok(temp_storage.make_datum(|row| {
            row.push_range(range).expect("errors already handled");
        }))
    }

    fn output_type(&self, _input_types: &[SqlColumnType]) -> SqlColumnType {
        SqlScalarType::Range {
            element_type: Box::new(self.elem_type.clone()),
        }
        .nullable(false)
    }

    fn introduces_nulls(&self) -> bool {
        false
    }
}

#[sqlfunc(sqlname = "datediff")]
fn date_diff_date(unit_str: &str, a: Date, b: Date) -> Result<i64, EvalError> {
    let unit = unit_str
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit_str.into()))?;

    // Convert the Date into a timestamp so we can calculate age.
    let a_ts = CheckedTimestamp::try_from(NaiveDate::from(a).and_hms_opt(0, 0, 0).unwrap())?;
    let b_ts = CheckedTimestamp::try_from(NaiveDate::from(b).and_hms_opt(0, 0, 0).unwrap())?;
    let diff = b_ts.diff_as(&a_ts, unit)?;
    Ok(diff)
}

#[sqlfunc(sqlname = "datediff")]
fn date_diff_time(unit_str: &str, a: NaiveTime, b: NaiveTime) -> Result<i64, EvalError> {
    let unit = unit_str
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit_str.into()))?;

    // Convert the Time into a timestamp so we can calculate age.
    let a_ts =
        CheckedTimestamp::try_from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(a))?;
    let b_ts =
        CheckedTimestamp::try_from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(b))?;
    let diff = b_ts.diff_as(&a_ts, unit)?;
    Ok(diff)
}

#[sqlfunc(sqlname = "datediff")]
fn date_diff_timestamp(
    unit: &str,
    a: CheckedTimestamp<NaiveDateTime>,
    b: CheckedTimestamp<NaiveDateTime>,
) -> Result<i64, EvalError> {
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let diff = b.diff_as(&a, unit)?;
    Ok(diff)
}

#[sqlfunc(sqlname = "datediff")]
fn date_diff_timestamp_tz(
    unit: &str,
    a: CheckedTimestamp<DateTime<Utc>>,
    b: CheckedTimestamp<DateTime<Utc>>,
) -> Result<i64, EvalError> {
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let diff = b.diff_as(&a, unit)?;
    Ok(diff)
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ErrorIfNull;

impl fmt::Display for ErrorIfNull {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("error_if_null")
    }
}

impl LazyVariadicFunc for ErrorIfNull {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let first = exprs[0].eval(datums, temp_storage)?;
        match first {
            Datum::Null => {
                let err_msg = match exprs[1].eval(datums, temp_storage)? {
                    Datum::Null => {
                        return Err(EvalError::Internal(
                            "unexpected NULL in error side of error_if_null".into(),
                        ));
                    }
                    o => o.unwrap_str(),
                };
                Err(EvalError::IfNullError(err_msg.into()))
            }
            _ => Ok(first),
        }
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        input_types[0].scalar_type.clone().nullable(false)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct Greatest;

impl fmt::Display for Greatest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("greatest")
    }
}

impl LazyVariadicFunc for Greatest {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let datums = fallible_iterator::convert(exprs.iter().map(|e| e.eval(datums, temp_storage)));
        Ok(datums
            .filter(|d| Ok(!d.is_null()))
            .max()?
            .unwrap_or(Datum::Null))
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        SqlColumnType::union_many(input_types)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn could_error(&self) -> bool {
        false
    }

    fn is_monotone(&self) -> bool {
        true
    }

    fn is_associative(&self) -> bool {
        true
    }
}

#[sqlfunc(sqlname = "hmac")]
fn hmac_string(to_digest: &str, key: &str, typ: &str) -> Result<Vec<u8>, EvalError> {
    let to_digest = to_digest.as_bytes();
    let key = key.as_bytes();
    hmac_inner(to_digest, key, typ)
}

#[sqlfunc(sqlname = "hmac")]
fn hmac_bytes(to_digest: &[u8], key: &[u8], typ: &str) -> Result<Vec<u8>, EvalError> {
    hmac_inner(to_digest, key, typ)
}

pub fn hmac_inner(to_digest: &[u8], key: &[u8], typ: &str) -> Result<Vec<u8>, EvalError> {
    match typ {
        "md5" => {
            let mut mac = Hmac::<Md5>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        "sha1" => {
            let k = aws_hmac::Key::new(aws_hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, key);
            Ok(aws_hmac::sign(&k, to_digest).as_ref().to_vec())
        }
        "sha224" => {
            let k = aws_hmac::Key::new(aws_hmac::HMAC_SHA224, key);
            Ok(aws_hmac::sign(&k, to_digest).as_ref().to_vec())
        }
        "sha256" => {
            let k = aws_hmac::Key::new(aws_hmac::HMAC_SHA256, key);
            Ok(aws_hmac::sign(&k, to_digest).as_ref().to_vec())
        }
        "sha384" => {
            let k = aws_hmac::Key::new(aws_hmac::HMAC_SHA384, key);
            Ok(aws_hmac::sign(&k, to_digest).as_ref().to_vec())
        }
        "sha512" => {
            let k = aws_hmac::Key::new(aws_hmac::HMAC_SHA512, key);
            Ok(aws_hmac::sign(&k, to_digest).as_ref().to_vec())
        }
        other => Err(EvalError::InvalidHashAlgorithm(other.into())),
    }
}

#[sqlfunc]
fn jsonb_build_array<'a>(datums: Variadic<Datum<'a>>, temp_storage: &'a RowArena) -> JsonbRef<'a> {
    let datum = temp_storage.make_datum(|packer| {
        packer.push_list(datums.into_iter().map(|d| match d {
            Datum::Null => Datum::JsonNull,
            d => d,
        }))
    });
    JsonbRef::from_datum(datum)
}

#[sqlfunc]
fn jsonb_build_object<'a>(
    mut kvs: Variadic<(Datum<'a>, Datum<'a>)>,
    temp_storage: &'a RowArena,
) -> Result<JsonbRef<'a>, EvalError> {
    kvs.0.sort_by(|kv1, kv2| kv1.0.cmp(&kv2.0));
    kvs.0.dedup_by(|kv1, kv2| kv1.0 == kv2.0);
    let datum = temp_storage.try_make_datum(|packer| {
        packer.push_dict_with(|packer| {
            for (k, v) in kvs {
                if k.is_null() {
                    return Err(EvalError::KeyCannotBeNull);
                }
                let v = match v {
                    Datum::Null => Datum::JsonNull,
                    d => d,
                };
                packer.push(k);
                packer.push(v);
            }
            Ok(())
        })
    })?;
    Ok(JsonbRef::from_datum(datum))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct Least;

impl fmt::Display for Least {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("least")
    }
}

impl LazyVariadicFunc for Least {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let datums = fallible_iterator::convert(exprs.iter().map(|e| e.eval(datums, temp_storage)));
        Ok(datums
            .filter(|d| Ok(!d.is_null()))
            .min()?
            .unwrap_or(Datum::Null))
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        SqlColumnType::union_many(input_types)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn could_error(&self) -> bool {
        false
    }

    fn is_monotone(&self) -> bool {
        true
    }

    fn is_associative(&self) -> bool {
        true
    }
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct ListCreate {
    pub elem_type: SqlScalarType,
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::List { element_type: Box::new(self.elem_type.clone()), custom_id: None }.nullable(false)",
    introduces_nulls = false
)]
fn list_create<'a>(&self, datums: Variadic<Datum<'a>>, temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| packer.push_list(datums))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct RecordCreate {
    pub field_names: Vec<ColumnName>,
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Record { fields: self.field_names.clone().into_iter().zip_eq(input_types.iter().cloned()).collect(), custom_id: None }.nullable(false)",
    introduces_nulls = false
)]
fn record_create<'a>(&self, datums: Variadic<Datum<'a>>, temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| packer.push_list(datums.iter().copied()))
}

#[sqlfunc(
    output_type_expr = "input_types[0].scalar_type.unwrap_list_nth_layer_type(input_types.len() - 1).clone().nullable(true)",
    introduces_nulls = true
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn list_index<'a>(buf: DatumList<'a>, indices: Variadic<i64>) -> Datum<'a> {
    let mut buf = Datum::List(buf);
    for i in indices {
        if buf.is_null() {
            break;
        }
        if i < 1 {
            return Datum::Null;
        }

        buf = match buf.unwrap_list().iter().nth(i as usize - 1) {
            Some(datum) => datum,
            None => return Datum::Null,
        }
    }
    buf
}

#[sqlfunc(sqlname = "makeaclitem")]
fn make_acl_item(
    grantee_oid: u32,
    grantor_oid: u32,
    privileges: &str,
    is_grantable: bool,
) -> Result<AclItem, EvalError> {
    let grantee = Oid(grantee_oid);
    let grantor = Oid(grantor_oid);
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;
    if is_grantable {
        return Err(EvalError::Unsupported {
            feature: "GRANT OPTION".into(),
            discussion_no: None,
        });
    }

    Ok(AclItem {
        grantee,
        grantor,
        acl_mode,
    })
}

#[sqlfunc(sqlname = "make_mz_aclitem")]
fn make_mz_acl_item(
    grantee_str: &str,
    grantor_str: &str,
    privileges: &str,
) -> Result<MzAclItem, EvalError> {
    let grantee: RoleId = grantee_str
        .parse()
        .map_err(|e: anyhow::Error| EvalError::InvalidRoleId(e.to_string().into()))?;
    let grantor: RoleId = grantor_str
        .parse()
        .map_err(|e: anyhow::Error| EvalError::InvalidRoleId(e.to_string().into()))?;
    if grantor == RoleId::Public {
        return Err(EvalError::InvalidRoleId(
            "mz_aclitem grantor cannot be PUBLIC role".into(),
        ));
    }
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;

    Ok(MzAclItem {
        grantee,
        grantor,
        acl_mode,
    })
}

#[sqlfunc(sqlname = "makets")]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn make_timestamp(
    year: i64,
    month: i64,
    day: i64,
    hour: i64,
    minute: i64,
    second_float: f64,
) -> Result<Option<CheckedTimestamp<NaiveDateTime>>, EvalError> {
    let year: i32 = match year.try_into() {
        Ok(year) => year,
        Err(_) => return Ok(None),
    };
    let month: u32 = match month.try_into() {
        Ok(month) => month,
        Err(_) => return Ok(None),
    };
    let day: u32 = match day.try_into() {
        Ok(day) => day,
        Err(_) => return Ok(None),
    };
    let hour: u32 = match hour.try_into() {
        Ok(hour) => hour,
        Err(_) => return Ok(None),
    };
    let minute: u32 = match minute.try_into() {
        Ok(minute) => minute,
        Err(_) => return Ok(None),
    };
    let second = second_float as u32;
    let micros = ((second_float - second as f64) * 1_000_000.0) as u32;
    let date = match NaiveDate::from_ymd_opt(year, month, day) {
        Some(date) => date,
        None => return Ok(None),
    };
    let timestamp = match date.and_hms_micro_opt(hour, minute, second, micros) {
        Some(timestamp) => timestamp,
        None => return Ok(None),
    };
    Ok(Some(timestamp.try_into()?))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct MapBuild {
    pub value_type: SqlScalarType,
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Map { value_type: Box::new(self.value_type.clone()), custom_id: None }.nullable(false)",
    introduces_nulls = false
)]
fn map_build<'a>(
    &self,
    datums: Variadic<(Option<&str>, Datum<'a>)>,
    temp_storage: &'a RowArena,
) -> Datum<'a> {
    // Collect into a `BTreeMap` to provide the same semantics as it.
    let map: std::collections::BTreeMap<&str, _> = datums
        .into_iter()
        .filter_map(|(k, v)| k.map(|k| (k, v)))
        .collect();

    temp_storage.make_datum(|packer| packer.push_dict(map))
}

#[derive(
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    Serialize,
    Deserialize,
    Hash,
    MzReflect
)]
pub struct Or;

impl fmt::Display for Or {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("OR")
    }
}

impl LazyVariadicFunc for Or {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        // If any is true, then return true. Else, if any is null, then return null. Else, return false.
        let mut null = false;
        let mut err = None;
        for expr in exprs {
            match expr.eval(datums, temp_storage) {
                Ok(Datum::False) => {}
                Ok(Datum::True) => return Ok(Datum::True), // short-circuit
                // No return in these two cases, because we might still see a true
                Ok(Datum::Null) => null = true,
                Err(this_err) => err = std::cmp::max(err.take(), Some(this_err)),
                _ => unreachable!(),
            }
        }
        match (err, null) {
            (Some(err), _) => Err(err),
            (None, true) => Ok(Datum::Null),
            (None, false) => Ok(Datum::False),
        }
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        let in_nullable = input_types.iter().any(|t| t.nullable);
        SqlScalarType::Bool.nullable(in_nullable)
    }

    fn propagates_nulls(&self) -> bool {
        false
    }

    fn introduces_nulls(&self) -> bool {
        false
    }

    fn could_error(&self) -> bool {
        false
    }

    fn is_monotone(&self) -> bool {
        true
    }

    fn is_associative(&self) -> bool {
        true
    }

    fn is_infix_op(&self) -> bool {
        true
    }
}

#[sqlfunc(sqlname = "lpad")]
fn pad_leading(string: &str, raw_len: i32, pad: OptionalArg<&str>) -> Result<String, EvalError> {
    let len = match usize::try_from(raw_len) {
        Ok(len) => len,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(
                "length must be nonnegative".into(),
            ));
        }
    };
    if len > MAX_STRING_FUNC_RESULT_BYTES {
        return Err(EvalError::LengthTooLarge);
    }

    let pad_string = pad.unwrap_or(" ");

    let (end_char, end_char_byte_offset) = string
        .chars()
        .take(len)
        .fold((0, 0), |acc, char| (acc.0 + 1, acc.1 + char.len_utf8()));

    let mut buf = String::with_capacity(len);
    if len == end_char {
        buf.push_str(&string[0..end_char_byte_offset]);
    } else {
        buf.extend(pad_string.chars().cycle().take(len - end_char));
        buf.push_str(string);
    }

    Ok(buf)
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true)",
    introduces_nulls = true
)]
fn regexp_match<'a>(
    haystack: &'a str,
    needle: &str,
    flags: OptionalArg<&str>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let flags = flags.unwrap_or("");
    let needle = build_regex(needle, flags)?;
    regexp_match_static(Datum::String(haystack), temp_storage, &needle)
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false)",
    introduces_nulls = false
)]
fn regexp_split_to_array<'a>(
    text: &str,
    regexp_str: &str,
    flags: OptionalArg<&str>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let flags = flags.unwrap_or("");
    let regexp = build_regex(regexp_str, flags)?;
    regexp_split_to_array_re(text, &regexp, temp_storage)
}

#[sqlfunc]
fn regexp_replace<'a>(
    source: &'a str,
    pattern: &str,
    replacement: &str,
    flags_opt: OptionalArg<&str>,
) -> Result<Cow<'a, str>, EvalError> {
    let flags = flags_opt.0.unwrap_or("");
    let (limit, flags) = regexp_replace_parse_flags(flags);
    let regexp = build_regex(pattern, &flags)?;
    Ok(regexp.replacen(source, limit, replacement))
}

#[sqlfunc]
fn replace(text: &str, from: &str, to: &str) -> Result<String, EvalError> {
    // As a compromise to avoid always nearly duplicating the work of replace by doing size estimation,
    // we first check if it's possible for the fully replaced string to exceed the limit by assuming that
    // every possible substring is replaced.
    //
    // If that estimate exceeds the limit, we then do a more precise (and expensive) estimate by counting
    // the actual number of replacements that would occur, and using that to calculate the final size.
    let possible_size = text.len() * to.len();
    if possible_size > MAX_STRING_FUNC_RESULT_BYTES {
        let replacement_count = text.matches(from).count();
        let estimated_size = text.len() + replacement_count * (to.len().saturating_sub(from.len()));
        if estimated_size > MAX_STRING_FUNC_RESULT_BYTES {
            return Err(EvalError::LengthTooLarge);
        }
    }

    Ok(text.replace(from, to))
}

#[sqlfunc(
    output_type_expr = "SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(false)",
    introduces_nulls = false,
    propagates_nulls = false
)]
fn string_to_array<'a>(
    string: &'a str,
    delimiter: Option<&'a str>,
    null_string: OptionalArg<Option<&'a str>>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if string.is_empty() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.try_push_array(&[], std::iter::empty::<Datum>())?;

        return Ok(temp_storage.push_unary_row(row));
    }

    let Some(delimiter) = delimiter else {
        let split_all_chars_delimiter = "";
        return string_to_array_impl(
            string,
            split_all_chars_delimiter,
            null_string.flatten(),
            temp_storage,
        );
    };

    if delimiter.is_empty() {
        let mut row = Row::default();
        let mut packer = row.packer();
        let dims = &[ArrayDimension {
            lower_bound: 1,
            length: 1,
        }];
        match null_string.flatten() {
            Some(null_string) if null_string == string => {
                packer.try_push_array(dims, std::iter::once(Datum::Null))?;
            }
            _ => {
                packer.try_push_array(dims, vec![string].into_iter().map(Datum::String))?;
            }
        }
        Ok(temp_storage.push_unary_row(row))
    } else {
        string_to_array_impl(string, delimiter, null_string.flatten(), temp_storage)
    }
}

fn string_to_array_impl<'a>(
    string: &str,
    delimiter: &str,
    null_string: Option<&'a str>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut row = Row::default();
    let mut packer = row.packer();

    let result = string.split(delimiter);
    let found: Vec<&str> = if delimiter.is_empty() {
        result.filter(|s| !s.is_empty()).collect()
    } else {
        result.collect()
    };
    let array_dimensions = [ArrayDimension {
        lower_bound: 1,
        length: found.len(),
    }];

    if let Some(null_string) = null_string {
        let found_datums = found.into_iter().map(|chunk| {
            if chunk.eq(null_string) {
                Datum::Null
            } else {
                Datum::String(chunk)
            }
        });

        packer.try_push_array(&array_dimensions, found_datums)?;
    } else {
        packer.try_push_array(&array_dimensions, found.into_iter().map(Datum::String))?;
    }

    Ok(temp_storage.push_unary_row(row))
}

#[sqlfunc]
fn substr<'a>(s: &'a str, start: i32, length: OptionalArg<i32>) -> Result<&'a str, EvalError> {
    let raw_start_idx = i64::from(start) - 1;
    let start_idx = match usize::try_from(cmp::max(raw_start_idx, 0)) {
        Ok(i) => i,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(
                format!(
                    "substring starting index ({}) exceeds min/max position",
                    raw_start_idx
                )
                .into(),
            ));
        }
    };

    let mut char_indices = s.char_indices();
    let get_str_index = |(index, _char)| index;

    let str_len = s.len();
    let start_char_idx = char_indices.nth(start_idx).map_or(str_len, get_str_index);

    if let OptionalArg(Some(len)) = length {
        let end_idx = match i64::from(len) {
            e if e < 0 => {
                return Err(EvalError::InvalidParameterValue(
                    "negative substring length not allowed".into(),
                ));
            }
            e if e == 0 || e + raw_start_idx < 1 => return Ok(""),
            e => {
                let e = cmp::min(raw_start_idx + e - 1, e - 1);
                match usize::try_from(e) {
                    Ok(i) => i,
                    Err(_) => {
                        return Err(EvalError::InvalidParameterValue(
                            format!("substring length ({}) exceeds max position", e).into(),
                        ));
                    }
                }
            }
        };

        let end_char_idx = char_indices.nth(end_idx).map_or(str_len, get_str_index);

        Ok(&s[start_char_idx..end_char_idx])
    } else {
        Ok(&s[start_char_idx..])
    }
}

#[sqlfunc(sqlname = "split_string")]
fn split_part<'a>(string: &'a str, delimiter: &str, field: i32) -> Result<&'a str, EvalError> {
    let index = match usize::try_from(i64::from(field) - 1) {
        Ok(index) => index,
        Err(_) => {
            return Err(EvalError::InvalidParameterValue(
                "field position must be greater than zero".into(),
            ));
        }
    };

    // If the provided delimiter is the empty string,
    // PostgreSQL does not break the string into individual
    // characters. Instead, it generates the following parts: [string].
    if delimiter.is_empty() {
        if index == 0 {
            return Ok(string);
        } else {
            return Ok("");
        }
    }

    // If provided index is greater than the number of split parts,
    // return an empty string.
    Ok(string.split(delimiter).nth(index).unwrap_or(""))
}

#[sqlfunc(is_associative = true)]
fn concat(strs: Variadic<Option<&str>>) -> Result<String, EvalError> {
    let mut total_size = 0;
    for s in &strs {
        if let Some(s) = s {
            total_size += s.len();
            if total_size > MAX_STRING_FUNC_RESULT_BYTES {
                return Err(EvalError::LengthTooLarge);
            }
        }
    }
    let mut buf = String::with_capacity(total_size);
    for s in strs {
        if let Some(s) = s {
            buf.push_str(s);
        }
    }
    Ok(buf)
}

#[sqlfunc]
fn concat_ws(ws: &str, rest: Variadic<Option<&str>>) -> Result<String, EvalError> {
    let mut total_size = 0;
    for s in &rest {
        if let Some(s) = s {
            total_size += s.len();
            total_size += ws.len();
            if total_size > MAX_STRING_FUNC_RESULT_BYTES {
                return Err(EvalError::LengthTooLarge);
            }
        }
    }

    let buf = Itertools::join(&mut rest.into_iter().filter_map(|s| s), ws);

    Ok(buf)
}

#[sqlfunc]
fn translate(string: &str, from_str: &str, to_str: &str) -> String {
    let from = from_str.chars().collect::<Vec<_>>();
    let to = to_str.chars().collect::<Vec<_>>();

    string
        .chars()
        .filter_map(|c| match from.iter().position(|f| f == &c) {
            Some(idx) => to.get(idx).copied(),
            None => Some(c),
        })
        .collect()
}

#[sqlfunc(
    output_type_expr = "input_types[0].scalar_type.clone().nullable(false)",
    introduces_nulls = false
)]
// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn list_slice_linear<'a>(
    list: DatumList<'a>,
    first: (i64, i64),
    remainder: Variadic<(i64, i64)>,
    temp_storage: &'a RowArena,
) -> Datum<'a> {
    let mut start_idx = 0;
    let mut total_length = usize::MAX;

    for (start, end) in std::iter::once(first).chain(remainder) {
        let start = std::cmp::max(start, 1);

        // Result should be empty list.
        if start > end {
            start_idx = 0;
            total_length = 0;
            break;
        }

        let start_inner = start as usize - 1;
        // Start index only moves to geq positions.
        start_idx += start_inner;

        // Length index only moves to leq positions
        let length_inner = (end - start) as usize + 1;
        total_length = std::cmp::min(length_inner, total_length - start_inner);
    }

    let iter = list.iter().skip(start_idx).take(total_length);

    temp_storage.make_datum(|row| {
        row.push_list_with(|row| {
            // if iter is empty, will get the appropriate empty list.
            for d in iter {
                row.push(d);
            }
        });
    })
}

#[sqlfunc(sqlname = "timestamp_bin")]
fn date_bin_timestamp(
    stride: Interval,
    source: CheckedTimestamp<NaiveDateTime>,
    origin: CheckedTimestamp<NaiveDateTime>,
) -> Result<CheckedTimestamp<NaiveDateTime>, EvalError> {
    date_bin(stride, source, origin)
}

#[sqlfunc(sqlname = "timestamptz_bin")]
fn date_bin_timestamp_tz(
    stride: Interval,
    source: CheckedTimestamp<DateTime<Utc>>,
    origin: CheckedTimestamp<DateTime<Utc>>,
) -> Result<CheckedTimestamp<DateTime<Utc>>, EvalError> {
    date_bin(stride, source, origin)
}

#[sqlfunc(sqlname = "timezonet")]
fn timezone_time_variadic(
    tz_str: &str,
    time: NaiveTime,
    wall_time: CheckedTimestamp<DateTime<Utc>>,
) -> Result<NaiveTime, EvalError> {
    parse_timezone(tz_str, TimezoneSpec::Posix)
        .map(|tz| timezone_time(tz, time, &wall_time.naive_utc()))
}
pub(crate) trait LazyVariadicFunc: fmt::Display {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError>;

    /// The output SqlColumnType of this function.
    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType;

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool;

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        true
    }

    /// Returns true if the function is monotone.
    fn is_monotone(&self) -> bool {
        false
    }

    /// Returns true if the function is associative.
    fn is_associative(&self) -> bool {
        false
    }

    /// Returns true if the function is an infix operator.
    fn is_infix_op(&self) -> bool {
        false
    }
}

pub(crate) trait EagerVariadicFunc: fmt::Display {
    type Input<'a>: InputDatumType<'a, EvalError>;
    type Output<'a>: OutputDatumType<'a, EvalError>;

    fn call<'a>(&self, input: Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a>;

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType;

    fn propagates_nulls(&self) -> bool {
        !Self::Input::nullable()
    }

    fn introduces_nulls(&self) -> bool {
        Self::Output::nullable()
    }

    fn could_error(&self) -> bool {
        Self::Output::fallible()
    }

    fn is_monotone(&self) -> bool {
        false
    }

    fn is_associative(&self) -> bool {
        false
    }

    fn is_infix_op(&self) -> bool {
        false
    }
}

/// Blanket `LazyVariadicFunc` impl for each eager type, bridging
/// expression evaluation and null propagation via `InputDatumType::try_from_iter`.
impl<T: EagerVariadicFunc> LazyVariadicFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let mut datums = exprs.iter().map(|e| e.eval(datums, temp_storage));
        match T::Input::try_from_iter(&mut datums) {
            Ok(input) => self.call(input, temp_storage).into_result(temp_storage),
            Err(Ok(None)) => Err(EvalError::Internal("missing parameter".into())),
            Err(Ok(Some(datum))) if datum.is_null() => Ok(datum),
            Err(Ok(Some(_datum))) => {
                // datum is _not_ NULL
                Err(EvalError::Internal("invalid input type".into()))
            }
            Err(Err(res)) => Err(res),
        }
    }

    fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        self.output_type(input_types)
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn is_monotone(&self) -> bool {
        self.is_monotone()
    }

    fn is_associative(&self) -> bool {
        self.is_associative()
    }

    fn is_infix_op(&self) -> bool {
        self.is_infix_op()
    }
}

derive_variadic! {
    Coalesce(Coalesce),
    Greatest(Greatest),
    Least(Least),
    Concat(Concat),
    ConcatWs(ConcatWs),
    MakeTimestamp(MakeTimestamp),
    PadLeading(PadLeading),
    Substr(Substr),
    Replace(Replace),
    JsonbBuildArray(JsonbBuildArray),
    JsonbBuildObject(JsonbBuildObject),
    MapBuild(MapBuild),
    ArrayCreate(ArrayCreate),
    ArrayToString(ArrayToString),
    ArrayIndex(ArrayIndex),
    ListCreate(ListCreate),
    RecordCreate(RecordCreate),
    ListIndex(ListIndex),
    ListSliceLinear(ListSliceLinear),
    SplitPart(SplitPart),
    RegexpMatch(RegexpMatch),
    HmacString(HmacString),
    HmacBytes(HmacBytes),
    ErrorIfNull(ErrorIfNull),
    DateBinTimestamp(DateBinTimestamp),
    DateBinTimestampTz(DateBinTimestampTz),
    DateDiffTimestamp(DateDiffTimestamp),
    DateDiffTimestampTz(DateDiffTimestampTz),
    DateDiffDate(DateDiffDate),
    DateDiffTime(DateDiffTime),
    And(And),
    Or(Or),
    RangeCreate(RangeCreate),
    MakeAclItem(MakeAclItem),
    MakeMzAclItem(MakeMzAclItem),
    Translate(Translate),
    ArrayPosition(ArrayPosition),
    ArrayFill(ArrayFill),
    StringToArray(StringToArray),
    TimezoneTimeVariadic(TimezoneTimeVariadic),
    RegexpSplitToArray(RegexpSplitToArray),
    RegexpReplace(RegexpReplace),
    CaseLiteral(CaseLiteral),
}

impl VariadicFunc {
    pub fn switch_and_or(&self) -> Self {
        match self {
            VariadicFunc::And(_) => Or.into(),
            VariadicFunc::Or(_) => And.into(),
            _ => unreachable!(),
        }
    }

    /// Gives the unit (u) of OR or AND, such that `u AND/OR x == x`.
    /// Note that a 0-arg AND/OR evaluates to unit_of_and_or.
    pub fn unit_of_and_or(&self) -> MirScalarExpr {
        match self {
            VariadicFunc::And(_) => MirScalarExpr::literal_true(),
            VariadicFunc::Or(_) => MirScalarExpr::literal_false(),
            _ => unreachable!(),
        }
    }

    /// Gives the zero (z) of OR or AND, such that `z AND/OR x == z`.
    pub fn zero_of_and_or(&self) -> MirScalarExpr {
        match self {
            VariadicFunc::And(_) => MirScalarExpr::literal_false(),
            VariadicFunc::Or(_) => MirScalarExpr::literal_true(),
            _ => unreachable!(),
        }
    }
}
