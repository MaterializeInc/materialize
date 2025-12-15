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

use std::cmp;

use chrono::NaiveDate;
use fallible_iterator::FallibleIterator;
use hmac::{Hmac, Mac};
use itertools::Itertools;
use md5::Md5;
use mz_lowertest::MzReflect;
use mz_ore::cast::{CastFrom, ReinterpretCast};
use mz_pgtz::timezone::TimezoneSpec;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::mz_acl_item::{AclItem, AclMode, MzAclItem};
use mz_repr::adt::range::{InvalidRangeError, Range, RangeBound, parse_range_bound_flags};
use mz_repr::adt::system::Oid;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::role_id::RoleId;
use mz_repr::{
    ColumnName, Datum, DatumType, ReprScalarType, Row, RowArena, SqlColumnType, SqlScalarType,
};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

use crate::func::{
    MAX_STRING_FUNC_RESULT_BYTES, array_create_scalar, build_regex, date_bin, parse_timezone,
    regexp_match_static, regexp_replace_parse_flags, regexp_replace_static,
    regexp_split_to_array_re, stringify_datum, timezone_time,
};
use crate::{EvalError, MirScalarExpr};

pub fn and<'a>(
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
/// dimensional array. Otherwise the lower bound of the additional dimension is
/// one and the length of the new dimension is equal to `datums.len()`.
fn array_create_multidim<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    // Per PostgreSQL, if all input arrays are zero dimensional, so is the
    // output.
    if datums.iter().all(|d| d.unwrap_array().dims().is_empty()) {
        let dims = &[];
        let datums = &[];
        let datum = temp_storage.try_make_datum(|packer| packer.try_push_array(dims, datums))?;
        return Ok(datum);
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

fn array_fill<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    const MAX_SIZE: usize = 1 << 28 - 1;
    const NULL_ARR_ERR: &str = "dimension array or low bound array";
    const NULL_ELEM_ERR: &str = "dimension values";

    let fill = datums[0];
    if matches!(fill, Datum::Array(_)) {
        return Err(EvalError::Unsupported {
            feature: "array_fill with arrays".into(),
            discussion_no: None,
        });
    }

    let arr = match datums[1] {
        Datum::Null => return Err(EvalError::MustNotBeNull(NULL_ARR_ERR.into())),
        o => o.unwrap_array(),
    };

    let dimensions = arr
        .elements()
        .iter()
        .map(|d| match d {
            Datum::Null => Err(EvalError::MustNotBeNull(NULL_ELEM_ERR.into())),
            d => Ok(usize::cast_from(u32::reinterpret_cast(d.unwrap_int32()))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    let lower_bounds = match datums.get(2) {
        Some(d) => {
            let arr = match d {
                Datum::Null => return Err(EvalError::MustNotBeNull(NULL_ARR_ERR.into())),
                o => o.unwrap_array(),
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

fn array_index<'a>(datums: &[Datum<'a>], offset: i64) -> Datum<'a> {
    mz_ore::soft_assert_no_log!(offset == 0 || offset == 1, "offset must be either 0 or 1");

    let array = datums[0].unwrap_array();
    let dims = array.dims();
    if dims.len() != datums.len() - 1 {
        // You missed the datums "layer"
        return Datum::Null;
    }

    let mut final_idx = 0;

    for (d, idx) in dims.into_iter().zip_eq(datums[1..].iter()) {
        // Lower bound is written in terms of 1-based indexing, which offset accounts for.
        let idx = isize::cast_from(idx.unwrap_int64() + offset);

        let (lower, upper) = d.dimension_bounds();

        // This index missed all of the data at this layer. The dimension bounds are inclusive,
        // while range checks are exclusive, so adjust.
        if !(lower..upper + 1).contains(&idx) {
            return Datum::Null;
        }

        // We discover how many indices our last index represents physically.
        final_idx *= d.length;

        // Because both index and lower bound are handled in 1-based indexing, taking their
        // difference moves us back into 0-based indexing. Similarly, if the lower bound is
        // negative, subtracting a negative value >= to itself ensures its non-negativity.
        final_idx += usize::try_from(idx - d.lower_bound)
            .expect("previous bounds check ensures phsical index is at least 0");
    }

    array
        .elements()
        .iter()
        .nth(final_idx)
        .unwrap_or(Datum::Null)
}

fn array_position<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let array = match datums[0] {
        Datum::Null => return Ok(Datum::Null),
        o => o.unwrap_array(),
    };

    if array.dims().len() > 1 {
        return Err(EvalError::MultiDimensionalArraySearch);
    }

    let search = datums[1];
    if search == Datum::Null {
        return Ok(Datum::Null);
    }

    let skip: usize = match datums.get(2) {
        Some(Datum::Null) => return Err(EvalError::MustNotBeNull("initial position".into())),
        None => 0,
        Some(o) => usize::try_from(o.unwrap_int32())
            .unwrap_or(0)
            .saturating_sub(1),
    };

    let r = array.elements().iter().skip(skip).position(|d| d == search);

    Ok(Datum::from(r.map(|p| {
        // Adjust count for the amount we skipped, plus 1 for adjustng to PG indexing scheme.
        i32::try_from(p + skip + 1).expect("fewer than i32::MAX elements in array")
    })))
}

// WARNING: This function has potential OOM risk!
// It is very difficult to calculate the output size ahead of time without knowing how to
// calculate the stringified size of each element for all possible datatypes.
fn array_to_string<'a>(
    datums: &[Datum<'a>],
    elem_type: &SqlScalarType,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if datums[0].is_null() || datums[1].is_null() {
        return Ok(Datum::Null);
    }
    let array = datums[0].unwrap_array();
    let delimiter = datums[1].unwrap_str();
    let null_str = match datums.get(2) {
        None | Some(Datum::Null) => None,
        Some(d) => Some(d.unwrap_str()),
    };

    let mut out = String::new();
    for elem in array.elements().iter() {
        if elem.is_null() {
            if let Some(null_str) = null_str {
                out.push_str(null_str);
                out.push_str(delimiter);
            }
        } else {
            stringify_datum(&mut out, elem, elem_type)?;
            out.push_str(delimiter);
        }
    }
    if out.len() > 0 {
        // Lop off last delimiter only if string is not empty
        out.truncate(out.len() - delimiter.len());
    }
    Ok(Datum::String(temp_storage.push_string(out)))
}

fn coalesce<'a>(
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

fn create_range<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let flags = match datums[2] {
        Datum::Null => {
            return Err(EvalError::InvalidRange(
                InvalidRangeError::NullRangeBoundFlags,
            ));
        }
        o => o.unwrap_str(),
    };

    let (lower_inclusive, upper_inclusive) = parse_range_bound_flags(flags)?;

    let mut range = Range::new(Some((
        RangeBound::new(datums[0], lower_inclusive),
        RangeBound::new(datums[1], upper_inclusive),
    )));

    range.canonicalize()?;

    Ok(temp_storage.make_datum(|row| {
        row.push_range(range).expect("errors already handled");
    }))
}

fn date_diff_date<'a>(unit: Datum, a: Datum, b: Datum) -> Result<Datum<'a>, EvalError> {
    let unit = unit.unwrap_str();
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let a = a.unwrap_date();
    let b = b.unwrap_date();

    // Convert the Date into a timestamp so we can calculate age.
    let a_ts = CheckedTimestamp::try_from(NaiveDate::from(a).and_hms_opt(0, 0, 0).unwrap())?;
    let b_ts = CheckedTimestamp::try_from(NaiveDate::from(b).and_hms_opt(0, 0, 0).unwrap())?;
    let diff = b_ts.diff_as(&a_ts, unit)?;

    Ok(Datum::Int64(diff))
}

fn date_diff_time<'a>(unit: Datum, a: Datum, b: Datum) -> Result<Datum<'a>, EvalError> {
    let unit = unit.unwrap_str();
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let a = a.unwrap_time();
    let b = b.unwrap_time();

    // Convert the Time into a timestamp so we can calculate age.
    let a_ts =
        CheckedTimestamp::try_from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(a))?;
    let b_ts =
        CheckedTimestamp::try_from(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().and_time(b))?;
    let diff = b_ts.diff_as(&a_ts, unit)?;

    Ok(Datum::Int64(diff))
}

fn date_diff_timestamp<'a>(unit: Datum, a: Datum, b: Datum) -> Result<Datum<'a>, EvalError> {
    let unit = unit.unwrap_str();
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let a = a.unwrap_timestamp();
    let b = b.unwrap_timestamp();
    let diff = b.diff_as(&a, unit)?;

    Ok(Datum::Int64(diff))
}

fn date_diff_timestamptz<'a>(unit: Datum, a: Datum, b: Datum) -> Result<Datum<'a>, EvalError> {
    let unit = unit.unwrap_str();
    let unit = unit
        .parse()
        .map_err(|_| EvalError::InvalidDatePart(unit.into()))?;

    let a = a.unwrap_timestamptz();
    let b = b.unwrap_timestamptz();
    let diff = b.diff_as(&a, unit)?;

    Ok(Datum::Int64(diff))
}

fn error_if_null<'a>(
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

fn greatest<'a>(
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

pub fn hmac_string<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = datums[0].unwrap_str().as_bytes();
    let key = datums[1].unwrap_str().as_bytes();
    let typ = datums[2].unwrap_str();
    hmac_inner(to_digest, key, typ, temp_storage)
}

pub fn hmac_bytes<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let to_digest = datums[0].unwrap_bytes();
    let key = datums[1].unwrap_bytes();
    let typ = datums[2].unwrap_str();
    hmac_inner(to_digest, key, typ, temp_storage)
}

pub fn hmac_inner<'a>(
    to_digest: &[u8],
    key: &[u8],
    typ: &str,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = match typ {
        "md5" => {
            let mut mac = Hmac::<Md5>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha1" => {
            let mut mac = Hmac::<Sha1>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha224" => {
            let mut mac = Hmac::<Sha224>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha256" => {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha384" => {
            let mut mac = Hmac::<Sha384>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        "sha512" => {
            let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts any key size");
            mac.update(to_digest);
            mac.finalize().into_bytes().to_vec()
        }
        other => return Err(EvalError::InvalidHashAlgorithm(other.into())),
    };
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

fn jsonb_build_array<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| {
        packer.push_list(datums.into_iter().map(|d| match d {
            Datum::Null => Datum::JsonNull,
            d => *d,
        }))
    })
}

fn jsonb_build_object<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut kvs = datums.chunks(2).collect::<Vec<_>>();
    kvs.sort_by(|kv1, kv2| kv1[0].cmp(&kv2[0]));
    kvs.dedup_by(|kv1, kv2| kv1[0] == kv2[0]);
    temp_storage.try_make_datum(|packer| {
        packer.push_dict_with(|packer| {
            for kv in kvs {
                let k = kv[0];
                if k.is_null() {
                    return Err(EvalError::KeyCannotBeNull);
                };
                let v = match kv[1] {
                    Datum::Null => Datum::JsonNull,
                    d => d,
                };
                packer.push(k);
                packer.push(v);
            }
            Ok(())
        })
    })
}

fn least<'a>(
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

fn list_create<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    temp_storage.make_datum(|packer| packer.push_list(datums))
}

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn list_index<'a>(datums: &[Datum<'a>]) -> Datum<'a> {
    let mut buf = datums[0];

    for i in datums[1..].iter() {
        if buf.is_null() {
            break;
        }

        let i = i.unwrap_int64();
        if i < 1 {
            return Datum::Null;
        }

        buf = buf
            .unwrap_list()
            .iter()
            .nth(i as usize - 1)
            .unwrap_or(Datum::Null);
    }
    buf
}

fn make_acl_item<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let grantee = Oid(datums[0].unwrap_uint32());
    let grantor = Oid(datums[1].unwrap_uint32());
    let privileges = datums[2].unwrap_str();
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;
    let is_grantable = datums[3].unwrap_bool();
    if is_grantable {
        return Err(EvalError::Unsupported {
            feature: "GRANT OPTION".into(),
            discussion_no: None,
        });
    }

    Ok(Datum::AclItem(AclItem {
        grantee,
        grantor,
        acl_mode,
    }))
}

fn make_mz_acl_item<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let grantee: RoleId = datums[0]
        .unwrap_str()
        .parse()
        .map_err(|e: anyhow::Error| EvalError::InvalidRoleId(e.to_string().into()))?;
    let grantor: RoleId = datums[1]
        .unwrap_str()
        .parse()
        .map_err(|e: anyhow::Error| EvalError::InvalidRoleId(e.to_string().into()))?;
    if grantor == RoleId::Public {
        return Err(EvalError::InvalidRoleId(
            "mz_aclitem grantor cannot be PUBLIC role".into(),
        ));
    }
    let privileges = datums[2].unwrap_str();
    let acl_mode = AclMode::parse_multiple_privileges(privileges)
        .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(e.to_string().into()))?;

    Ok(Datum::MzAclItem(MzAclItem {
        grantee,
        grantor,
        acl_mode,
    }))
}

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn make_timestamp<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let year: i32 = match datums[0].unwrap_int64().try_into() {
        Ok(year) => year,
        Err(_) => return Ok(Datum::Null),
    };
    let month: u32 = match datums[1].unwrap_int64().try_into() {
        Ok(month) => month,
        Err(_) => return Ok(Datum::Null),
    };
    let day: u32 = match datums[2].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let hour: u32 = match datums[3].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let minute: u32 = match datums[4].unwrap_int64().try_into() {
        Ok(day) => day,
        Err(_) => return Ok(Datum::Null),
    };
    let second_float = datums[5].unwrap_float64();
    let second = second_float as u32;
    let micros = ((second_float - second as f64) * 1_000_000.0) as u32;
    let date = match NaiveDate::from_ymd_opt(year, month, day) {
        Some(date) => date,
        None => return Ok(Datum::Null),
    };
    let timestamp = match date.and_hms_micro_opt(hour, minute, second, micros) {
        Some(timestamp) => timestamp,
        None => return Ok(Datum::Null),
    };
    Ok(timestamp.try_into()?)
}

fn map_build<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    // Collect into a `BTreeMap` to provide the same semantics as it.
    let map: std::collections::BTreeMap<&str, _> = datums
        .into_iter()
        .tuples()
        .filter_map(|(k, v)| {
            if k.is_null() {
                None
            } else {
                Some((k.unwrap_str(), v))
            }
        })
        .collect();

    temp_storage.make_datum(|packer| packer.push_dict(map))
}

pub fn or<'a>(
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

fn pad_leading<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let string = datums[0].unwrap_str();

    let len = match usize::try_from(datums[1].unwrap_int32()) {
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

    let pad_string = if datums.len() == 3 {
        datums[2].unwrap_str()
    } else {
        " "
    };

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

    Ok(Datum::String(temp_storage.push_string(buf)))
}

fn regexp_match_dynamic<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let haystack = datums[0];
    let needle = datums[1].unwrap_str();
    let flags = match datums.get(2) {
        Some(d) => d.unwrap_str(),
        None => "",
    };
    let needle = build_regex(needle, flags)?;
    regexp_match_static(haystack, temp_storage, &needle)
}

fn regexp_split_to_array<'a>(
    text: Datum<'a>,
    regexp: Datum<'a>,
    flags: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let text = text.unwrap_str();
    let regexp = regexp.unwrap_str();
    let flags = flags.unwrap_str();
    let regexp = build_regex(regexp, flags)?;
    regexp_split_to_array_re(text, &regexp, temp_storage)
}

fn regexp_replace_dynamic<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let source = datums[0];
    let pattern = datums[1];
    let replacement = datums[2];
    let flags = match datums.get(3) {
        Some(d) => d.unwrap_str(),
        None => "",
    };
    let (limit, flags) = regexp_replace_parse_flags(flags);
    let regexp = build_regex(pattern.unwrap_str(), &flags)?;
    regexp_replace_static(source, replacement, &regexp, limit, temp_storage)
}

fn replace<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Result<Datum<'a>, EvalError> {
    // As a compromise to avoid always nearly duplicating the work of replace by doing size estimation,
    // we first check if its possible for the fully replaced string to exceed the limit by assuming that
    // every possible substring is replaced.
    //
    // If that estimate exceeds the limit, we then do a more precise (and expensive) estimate by counting
    // the actual number of replacements that would occur, and using that to calculate the final size.
    let text = datums[0].unwrap_str();
    let from = datums[1].unwrap_str();
    let to = datums[2].unwrap_str();
    let possible_size = text.len() * to.len();
    if possible_size > MAX_STRING_FUNC_RESULT_BYTES {
        let replacement_count = text.matches(from).count();
        let estimated_size = text.len() + replacement_count * (to.len().saturating_sub(from.len()));
        if estimated_size > MAX_STRING_FUNC_RESULT_BYTES {
            return Err(EvalError::LengthTooLarge);
        }
    }

    Ok(Datum::String(
        temp_storage.push_string(text.replace(from, to)),
    ))
}

fn string_to_array<'a>(
    string_datum: Datum<'a>,
    delimiter: Datum<'a>,
    null_string: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    if string_datum.is_null() {
        return Ok(Datum::Null);
    }

    let string = string_datum.unwrap_str();

    if string.is_empty() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.try_push_array(&[], std::iter::empty::<Datum>())?;

        return Ok(temp_storage.push_unary_row(row));
    }

    if delimiter.is_null() {
        let split_all_chars_delimiter = "";
        return string_to_array_impl(string, split_all_chars_delimiter, null_string, temp_storage);
    }

    let delimiter = delimiter.unwrap_str();

    if delimiter.is_empty() {
        let mut row = Row::default();
        let mut packer = row.packer();
        packer.try_push_array(
            &[ArrayDimension {
                lower_bound: 1,
                length: 1,
            }],
            vec![string].into_iter().map(Datum::String),
        )?;

        Ok(temp_storage.push_unary_row(row))
    } else {
        string_to_array_impl(string, delimiter, null_string, temp_storage)
    }
}

fn string_to_array_impl<'a>(
    string: &str,
    delimiter: &str,
    null_string: Datum<'a>,
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

    if null_string.is_null() {
        packer.try_push_array(&array_dimensions, found.into_iter().map(Datum::String))?;
    } else {
        let null_string = null_string.unwrap_str();
        let found_datums = found.into_iter().map(|chunk| {
            if chunk.eq(null_string) {
                Datum::Null
            } else {
                Datum::String(chunk)
            }
        });

        packer.try_push_array(&array_dimensions, found_datums)?;
    }

    Ok(temp_storage.push_unary_row(row))
}

fn substr<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let s: &'a str = datums[0].unwrap_str();

    let raw_start_idx = i64::from(datums[1].unwrap_int32()) - 1;
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

    if datums.len() == 3 {
        let end_idx = match i64::from(datums[2].unwrap_int32()) {
            e if e < 0 => {
                return Err(EvalError::InvalidParameterValue(
                    "negative substring length not allowed".into(),
                ));
            }
            e if e == 0 || e + raw_start_idx < 1 => return Ok(Datum::String("")),
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

        Ok(Datum::String(&s[start_char_idx..end_char_idx]))
    } else {
        Ok(Datum::String(&s[start_char_idx..]))
    }
}

fn split_part<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let string = datums[0].unwrap_str();
    let delimiter = datums[1].unwrap_str();

    // Provided index value begins at 1, not 0.
    let index = match usize::try_from(i64::from(datums[2].unwrap_int32()) - 1) {
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
            return Ok(datums[0]);
        } else {
            return Ok(Datum::String(""));
        }
    }

    // If provided index is greater than the number of split parts,
    // return an empty string.
    Ok(Datum::String(
        string.split(delimiter).nth(index).unwrap_or(""),
    ))
}

fn text_concat_variadic<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut total_size = 0;
    for d in datums {
        if !d.is_null() {
            total_size += d.unwrap_str().len();
            if total_size > MAX_STRING_FUNC_RESULT_BYTES {
                return Err(EvalError::LengthTooLarge);
            }
        }
    }
    let mut buf = String::new();
    for d in datums {
        if !d.is_null() {
            buf.push_str(d.unwrap_str());
        }
    }
    Ok(Datum::String(temp_storage.push_string(buf)))
}

fn text_concat_ws<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let ws = match datums[0] {
        Datum::Null => return Ok(Datum::Null),
        d => d.unwrap_str(),
    };

    let mut total_size = 0;
    for d in &datums[1..] {
        if !d.is_null() {
            total_size += d.unwrap_str().len();
            total_size += ws.len();
            if total_size > MAX_STRING_FUNC_RESULT_BYTES {
                return Err(EvalError::LengthTooLarge);
            }
        }
    }

    let buf = Itertools::join(
        &mut datums[1..].iter().filter_map(|d| match d {
            Datum::Null => None,
            d => Some(d.unwrap_str()),
        }),
        ws,
    );

    Ok(Datum::String(temp_storage.push_string(buf)))
}

fn translate<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    let string = datums[0].unwrap_str();
    let from = datums[1].unwrap_str().chars().collect::<Vec<_>>();
    let to = datums[2].unwrap_str().chars().collect::<Vec<_>>();

    Datum::String(
        temp_storage.push_string(
            string
                .chars()
                .filter_map(|c| match from.iter().position(|f| f == &c) {
                    Some(idx) => to.get(idx).copied(),
                    None => Some(c),
                })
                .collect(),
        ),
    )
}

// TODO ///

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn list_slice_linear<'a>(datums: &[Datum<'a>], temp_storage: &'a RowArena) -> Datum<'a> {
    assert_eq!(
        datums.len() % 2,
        1,
        "expr::scalar::func::list_slice expects an odd number of arguments; 1 for list + 2 \
        for each start-end pair"
    );
    assert!(
        datums.len() > 2,
        "expr::scalar::func::list_slice expects at least 3 arguments; 1 for list + at least \
        one start-end pair"
    );

    let mut start_idx = 0;
    let mut total_length = usize::MAX;

    for (start, end) in datums[1..].iter().tuples::<(_, _)>() {
        let start = std::cmp::max(start.unwrap_int64(), 1);
        let end = end.unwrap_int64();

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

    let iter = datums[0]
        .unwrap_list()
        .iter()
        .skip(start_idx)
        .take(total_length);

    temp_storage.make_datum(|row| {
        row.push_list_with(|row| {
            // if iter is empty, will get the appropriate empty list.
            for d in iter {
                row.push(d);
            }
        });
    })
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum VariadicFunc {
    Coalesce,
    Greatest,
    Least,
    Concat,
    ConcatWs,
    MakeTimestamp,
    PadLeading,
    Substr,
    Replace,
    JsonbBuildArray,
    JsonbBuildObject,
    MapBuild {
        value_type: SqlScalarType,
    },
    ArrayCreate {
        // We need to know the element type to type empty arrays.
        elem_type: SqlScalarType,
    },
    ArrayToString {
        elem_type: SqlScalarType,
    },
    ArrayIndex {
        // Adjusts the index by offset depending on whether being called on an array or an
        // Int2Vector.
        offset: i64,
    },
    ListCreate {
        // We need to know the element type to type empty lists.
        elem_type: SqlScalarType,
    },
    RecordCreate {
        field_names: Vec<ColumnName>,
    },
    ListIndex,
    ListSliceLinear,
    SplitPart,
    RegexpMatch,
    HmacString,
    HmacBytes,
    ErrorIfNull,
    DateBinTimestamp,
    DateBinTimestampTz,
    DateDiffTimestamp,
    DateDiffTimestampTz,
    DateDiffDate,
    DateDiffTime,
    And,
    Or,
    RangeCreate {
        elem_type: SqlScalarType,
    },
    MakeAclItem,
    MakeMzAclItem,
    Translate,
    ArrayPosition,
    ArrayFill {
        elem_type: SqlScalarType,
    },
    StringToArray,
    TimezoneTime,
    RegexpSplitToArray,
    RegexpReplace,
}

impl VariadicFunc {
    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        // Evaluate all non-eager functions directly
        match self {
            VariadicFunc::Coalesce => return coalesce(datums, temp_storage, exprs),
            VariadicFunc::Greatest => return greatest(datums, temp_storage, exprs),
            VariadicFunc::And => return and(datums, temp_storage, exprs),
            VariadicFunc::Or => return or(datums, temp_storage, exprs),
            VariadicFunc::ErrorIfNull => return error_if_null(datums, temp_storage, exprs),
            VariadicFunc::Least => return least(datums, temp_storage, exprs),
            _ => {}
        };

        // Compute parameters to eager functions
        let ds = exprs
            .iter()
            .map(|e| e.eval(datums, temp_storage))
            .collect::<Result<Vec<_>, _>>()?;
        // Check NULL propagation
        if self.propagates_nulls() && ds.iter().any(|d| d.is_null()) {
            return Ok(Datum::Null);
        }

        // Evaluate eager functions
        match self {
            VariadicFunc::Coalesce
            | VariadicFunc::Greatest
            | VariadicFunc::And
            | VariadicFunc::Or
            | VariadicFunc::ErrorIfNull
            | VariadicFunc::Least => unreachable!(),
            VariadicFunc::Concat => text_concat_variadic(&ds, temp_storage),
            VariadicFunc::ConcatWs => text_concat_ws(&ds, temp_storage),
            VariadicFunc::MakeTimestamp => make_timestamp(&ds),
            VariadicFunc::PadLeading => pad_leading(&ds, temp_storage),
            VariadicFunc::Substr => substr(&ds),
            VariadicFunc::Replace => replace(&ds, temp_storage),
            VariadicFunc::Translate => Ok(translate(&ds, temp_storage)),
            VariadicFunc::JsonbBuildArray => Ok(jsonb_build_array(&ds, temp_storage)),
            VariadicFunc::JsonbBuildObject => jsonb_build_object(&ds, temp_storage),
            VariadicFunc::MapBuild { .. } => Ok(map_build(&ds, temp_storage)),
            VariadicFunc::ArrayCreate {
                elem_type: SqlScalarType::Array(_),
            } => array_create_multidim(&ds, temp_storage),
            VariadicFunc::ArrayCreate { .. } => array_create_scalar(&ds, temp_storage),
            VariadicFunc::ArrayToString { elem_type } => {
                array_to_string(&ds, elem_type, temp_storage)
            }
            VariadicFunc::ArrayIndex { offset } => Ok(array_index(&ds, *offset)),

            VariadicFunc::ListCreate { .. } | VariadicFunc::RecordCreate { .. } => {
                Ok(list_create(&ds, temp_storage))
            }
            VariadicFunc::ListIndex => Ok(list_index(&ds)),
            VariadicFunc::ListSliceLinear => Ok(list_slice_linear(&ds, temp_storage)),
            VariadicFunc::SplitPart => split_part(&ds),
            VariadicFunc::RegexpMatch => regexp_match_dynamic(&ds, temp_storage),
            VariadicFunc::HmacString => hmac_string(&ds, temp_storage),
            VariadicFunc::HmacBytes => hmac_bytes(&ds, temp_storage),
            VariadicFunc::DateBinTimestamp => date_bin(
                ds[0].unwrap_interval(),
                ds[1].unwrap_timestamp(),
                ds[2].unwrap_timestamp(),
            )
            .into_result(temp_storage),
            VariadicFunc::DateBinTimestampTz => date_bin(
                ds[0].unwrap_interval(),
                ds[1].unwrap_timestamptz(),
                ds[2].unwrap_timestamptz(),
            )
            .into_result(temp_storage),
            VariadicFunc::DateDiffTimestamp => date_diff_timestamp(ds[0], ds[1], ds[2]),
            VariadicFunc::DateDiffTimestampTz => date_diff_timestamptz(ds[0], ds[1], ds[2]),
            VariadicFunc::DateDiffDate => date_diff_date(ds[0], ds[1], ds[2]),
            VariadicFunc::DateDiffTime => date_diff_time(ds[0], ds[1], ds[2]),
            VariadicFunc::RangeCreate { .. } => create_range(&ds, temp_storage),
            VariadicFunc::MakeAclItem => make_acl_item(&ds),
            VariadicFunc::MakeMzAclItem => make_mz_acl_item(&ds),
            VariadicFunc::ArrayPosition => array_position(&ds),
            VariadicFunc::ArrayFill { .. } => array_fill(&ds, temp_storage),
            VariadicFunc::TimezoneTime => parse_timezone(ds[0].unwrap_str(), TimezoneSpec::Posix)
                .map(|tz| {
                    timezone_time(
                        tz,
                        ds[1].unwrap_time(),
                        &ds[2].unwrap_timestamptz().naive_utc(),
                    )
                    .into()
                }),
            VariadicFunc::RegexpSplitToArray => {
                let flags = if ds.len() == 2 {
                    Datum::String("")
                } else {
                    ds[2]
                };
                regexp_split_to_array(ds[0], ds[1], flags, temp_storage)
            }
            VariadicFunc::RegexpReplace => regexp_replace_dynamic(&ds, temp_storage),
            VariadicFunc::StringToArray => {
                let null_string = if ds.len() == 2 { Datum::Null } else { ds[2] };

                string_to_array(ds[0], ds[1], null_string, temp_storage)
            }
        }
    }

    pub fn is_associative(&self) -> bool {
        match self {
            VariadicFunc::Coalesce
            | VariadicFunc::Greatest
            | VariadicFunc::Least
            | VariadicFunc::Concat
            | VariadicFunc::And
            | VariadicFunc::Or => true,

            VariadicFunc::MakeTimestamp
            | VariadicFunc::PadLeading
            | VariadicFunc::ConcatWs
            | VariadicFunc::Substr
            | VariadicFunc::Replace
            | VariadicFunc::Translate
            | VariadicFunc::JsonbBuildArray
            | VariadicFunc::JsonbBuildObject
            | VariadicFunc::MapBuild { value_type: _ }
            | VariadicFunc::ArrayCreate { elem_type: _ }
            | VariadicFunc::ArrayToString { elem_type: _ }
            | VariadicFunc::ArrayIndex { offset: _ }
            | VariadicFunc::ListCreate { elem_type: _ }
            | VariadicFunc::RecordCreate { field_names: _ }
            | VariadicFunc::ListIndex
            | VariadicFunc::ListSliceLinear
            | VariadicFunc::SplitPart
            | VariadicFunc::RegexpMatch
            | VariadicFunc::HmacString
            | VariadicFunc::HmacBytes
            | VariadicFunc::ErrorIfNull
            | VariadicFunc::DateBinTimestamp
            | VariadicFunc::DateBinTimestampTz
            | VariadicFunc::DateDiffTimestamp
            | VariadicFunc::DateDiffTimestampTz
            | VariadicFunc::DateDiffDate
            | VariadicFunc::DateDiffTime
            | VariadicFunc::RangeCreate { .. }
            | VariadicFunc::MakeAclItem
            | VariadicFunc::MakeMzAclItem
            | VariadicFunc::ArrayPosition
            | VariadicFunc::ArrayFill { .. }
            | VariadicFunc::TimezoneTime
            | VariadicFunc::RegexpSplitToArray
            | VariadicFunc::StringToArray
            | VariadicFunc::RegexpReplace => false,
        }
    }

    pub fn output_type(&self, input_types: Vec<SqlColumnType>) -> SqlColumnType {
        use VariadicFunc::*;
        let in_nullable = input_types.iter().any(|t| t.nullable);
        match self {
            Greatest | Least => input_types
                .into_iter()
                .reduce(|l, r| l.union(&r).unwrap())
                .unwrap(),
            Coalesce => {
                // Note that the parser doesn't allow empty argument lists for variadic functions
                // that use the standard function call syntax (ArrayCreate and co. are different
                // because of the special syntax for calling them).
                let nullable = input_types.iter().all(|typ| typ.nullable);
                input_types
                    .into_iter()
                    .reduce(|l, r| l.union(&r).unwrap())
                    .unwrap()
                    .nullable(nullable)
            }
            Concat | ConcatWs => SqlScalarType::String.nullable(in_nullable),
            MakeTimestamp => SqlScalarType::Timestamp { precision: None }.nullable(true),
            PadLeading => SqlScalarType::String.nullable(in_nullable),
            Substr => SqlScalarType::String.nullable(in_nullable),
            Replace => SqlScalarType::String.nullable(in_nullable),
            Translate => SqlScalarType::String.nullable(in_nullable),
            JsonbBuildArray | JsonbBuildObject => SqlScalarType::Jsonb.nullable(true),
            MapBuild { value_type } => SqlScalarType::Map {
                value_type: Box::new(value_type.clone()),
                custom_id: None,
            }
            .nullable(true),
            ArrayCreate { elem_type } => {
                debug_assert!(
                    input_types
                        .iter()
                        .all(|t| ReprScalarType::from(&t.scalar_type)
                            == ReprScalarType::from(elem_type)),
                    "Args to ArrayCreate should have types that are repr-compatible with the elem_type"
                );
                match elem_type {
                    SqlScalarType::Array(_) => elem_type.clone().nullable(false),
                    _ => SqlScalarType::Array(Box::new(elem_type.clone())).nullable(false),
                }
            }
            ArrayToString { .. } => SqlScalarType::String.nullable(in_nullable),
            ArrayIndex { .. } => input_types[0]
                .scalar_type
                .unwrap_array_element_type()
                .clone()
                .nullable(true),
            ListCreate { elem_type } => {
                // commented out to work around
                // https://github.com/MaterializeInc/database-issues/issues/2730
                // soft_assert!(
                //     input_types.iter().all(|t| t.scalar_type.base_eq(elem_type)),
                //     "{}", format!("Args to ListCreate should have types that are compatible with the elem_type.\nArgs:{:#?}\nelem_type:{:#?}", input_types, elem_type)
                // );
                SqlScalarType::List {
                    element_type: Box::new(elem_type.clone()),
                    custom_id: None,
                }
                .nullable(false)
            }
            ListIndex => input_types[0]
                .scalar_type
                .unwrap_list_nth_layer_type(input_types.len() - 1)
                .clone()
                .nullable(true),
            ListSliceLinear { .. } => input_types[0].scalar_type.clone().nullable(in_nullable),
            RecordCreate { field_names } => SqlScalarType::Record {
                fields: field_names
                    .clone()
                    .into_iter()
                    .zip_eq(input_types)
                    .collect(),
                custom_id: None,
            }
            .nullable(false),
            SplitPart => SqlScalarType::String.nullable(in_nullable),
            RegexpMatch => SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
            HmacString | HmacBytes => SqlScalarType::Bytes.nullable(in_nullable),
            ErrorIfNull => input_types[0].scalar_type.clone().nullable(false),
            DateBinTimestamp => SqlScalarType::Timestamp { precision: None }.nullable(in_nullable),
            DateBinTimestampTz => {
                SqlScalarType::TimestampTz { precision: None }.nullable(in_nullable)
            }
            DateDiffTimestamp => SqlScalarType::Int64.nullable(in_nullable),
            DateDiffTimestampTz => SqlScalarType::Int64.nullable(in_nullable),
            DateDiffDate => SqlScalarType::Int64.nullable(in_nullable),
            DateDiffTime => SqlScalarType::Int64.nullable(in_nullable),
            And | Or => SqlScalarType::Bool.nullable(in_nullable),
            RangeCreate { elem_type } => SqlScalarType::Range {
                element_type: Box::new(elem_type.clone()),
            }
            .nullable(false),
            MakeAclItem => SqlScalarType::AclItem.nullable(true),
            MakeMzAclItem => SqlScalarType::MzAclItem.nullable(true),
            ArrayPosition => SqlScalarType::Int32.nullable(true),
            ArrayFill { elem_type } => {
                SqlScalarType::Array(Box::new(elem_type.clone())).nullable(false)
            }
            TimezoneTime => SqlScalarType::Time.nullable(in_nullable),
            RegexpSplitToArray => {
                SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(in_nullable)
            }
            RegexpReplace => SqlScalarType::String.nullable(in_nullable),
            StringToArray => SqlScalarType::Array(Box::new(SqlScalarType::String)).nullable(true),
        }
    }

    /// Whether the function output is NULL if any of its inputs are NULL.
    ///
    /// NB: if any input is NULL the output will be returned as NULL without
    /// calling the function.
    pub fn propagates_nulls(&self) -> bool {
        // NOTE: The following is a list of the variadic functions
        // that **DO NOT** propagate nulls.
        !matches!(
            self,
            VariadicFunc::And
                | VariadicFunc::Or
                | VariadicFunc::Coalesce
                | VariadicFunc::Greatest
                | VariadicFunc::Least
                | VariadicFunc::Concat
                | VariadicFunc::ConcatWs
                | VariadicFunc::JsonbBuildArray
                | VariadicFunc::JsonbBuildObject
                | VariadicFunc::MapBuild { .. }
                | VariadicFunc::ListCreate { .. }
                | VariadicFunc::RecordCreate { .. }
                | VariadicFunc::ArrayCreate { .. }
                | VariadicFunc::ArrayToString { .. }
                | VariadicFunc::ErrorIfNull
                | VariadicFunc::RangeCreate { .. }
                | VariadicFunc::ArrayPosition
                | VariadicFunc::ArrayFill { .. }
                | VariadicFunc::StringToArray
        )
    }

    /// Whether the function might return NULL even if none of its inputs are
    /// NULL.
    ///
    /// This is presently conservative, and may indicate that a function
    /// introduces nulls even when it does not.
    pub fn introduces_nulls(&self) -> bool {
        use VariadicFunc::*;
        match self {
            Concat
            | ConcatWs
            | PadLeading
            | Substr
            | Replace
            | Translate
            | JsonbBuildArray
            | JsonbBuildObject
            | MapBuild { .. }
            | ArrayCreate { .. }
            | ArrayToString { .. }
            | ListCreate { .. }
            | RecordCreate { .. }
            | ListSliceLinear
            | SplitPart
            | HmacString
            | HmacBytes
            | ErrorIfNull
            | DateBinTimestamp
            | DateBinTimestampTz
            | DateDiffTimestamp
            | DateDiffTimestampTz
            | DateDiffDate
            | DateDiffTime
            | RangeCreate { .. }
            | And
            | Or
            | MakeAclItem
            | MakeMzAclItem
            | ArrayPosition
            | ArrayFill { .. }
            | TimezoneTime
            | RegexpSplitToArray
            | RegexpReplace => false,
            Coalesce
            | Greatest
            | Least
            | MakeTimestamp
            | ArrayIndex { .. }
            | StringToArray
            | ListIndex
            | RegexpMatch => true,
        }
    }

    pub fn switch_and_or(&self) -> Self {
        match self {
            VariadicFunc::And => VariadicFunc::Or,
            VariadicFunc::Or => VariadicFunc::And,
            _ => unreachable!(),
        }
    }

    pub fn is_infix_op(&self) -> bool {
        use VariadicFunc::*;
        matches!(self, And | Or)
    }

    /// Gives the unit (u) of OR or AND, such that `u AND/OR x == x`.
    /// Note that a 0-arg AND/OR evaluates to unit_of_and_or.
    pub fn unit_of_and_or(&self) -> MirScalarExpr {
        match self {
            VariadicFunc::And => MirScalarExpr::literal_true(),
            VariadicFunc::Or => MirScalarExpr::literal_false(),
            _ => unreachable!(),
        }
    }

    /// Gives the zero (z) of OR or AND, such that `z AND/OR x == z`.
    pub fn zero_of_and_or(&self) -> MirScalarExpr {
        match self {
            VariadicFunc::And => MirScalarExpr::literal_false(),
            VariadicFunc::Or => MirScalarExpr::literal_true(),
            _ => unreachable!(),
        }
    }

    /// Returns true if the function could introduce an error on non-error inputs.
    pub fn could_error(&self) -> bool {
        match self {
            VariadicFunc::And | VariadicFunc::Or => false,
            VariadicFunc::Coalesce => false,
            VariadicFunc::Greatest | VariadicFunc::Least => false,
            VariadicFunc::Concat | VariadicFunc::ConcatWs => false,
            VariadicFunc::Replace => false,
            VariadicFunc::Translate => false,
            VariadicFunc::ArrayIndex { .. } => false,
            VariadicFunc::ListCreate { .. } | VariadicFunc::RecordCreate { .. } => false,
            // All other cases are unknown
            _ => true,
        }
    }

    /// Returns true if the function is monotone. (Non-strict; either increasing or decreasing.)
    /// Monotone functions map ranges to ranges: ie. given a range of possible inputs, we can
    /// determine the range of possible outputs just by mapping the endpoints.
    ///
    /// This describes the *pointwise* behaviour of the function:
    /// ie. if more than one argument is provided, this describes the behaviour of
    /// any specific argument as the others are held constant. (For example, `COALESCE(a, b)` is
    /// monotone in `a` because for any particular value of `b`, increasing `a` will never
    /// cause the result to decrease.)
    ///
    /// This property describes the behaviour of the function over ranges where the function is defined:
    /// ie. the arguments and the result are non-error datums.
    pub fn is_monotone(&self) -> bool {
        match self {
            VariadicFunc::Coalesce
            | VariadicFunc::Greatest
            | VariadicFunc::Least
            | VariadicFunc::And
            | VariadicFunc::Or => true,
            VariadicFunc::Concat
            | VariadicFunc::ConcatWs
            | VariadicFunc::MakeTimestamp
            | VariadicFunc::PadLeading
            | VariadicFunc::Substr
            | VariadicFunc::Replace
            | VariadicFunc::JsonbBuildArray
            | VariadicFunc::JsonbBuildObject
            | VariadicFunc::MapBuild { .. }
            | VariadicFunc::ArrayCreate { .. }
            | VariadicFunc::ArrayToString { .. }
            | VariadicFunc::ArrayIndex { .. }
            | VariadicFunc::ListCreate { .. }
            | VariadicFunc::RecordCreate { .. }
            | VariadicFunc::ListIndex
            | VariadicFunc::ListSliceLinear
            | VariadicFunc::SplitPart
            | VariadicFunc::RegexpMatch
            | VariadicFunc::HmacString
            | VariadicFunc::HmacBytes
            | VariadicFunc::ErrorIfNull
            | VariadicFunc::DateBinTimestamp
            | VariadicFunc::DateBinTimestampTz
            | VariadicFunc::RangeCreate { .. }
            | VariadicFunc::MakeAclItem
            | VariadicFunc::MakeMzAclItem
            | VariadicFunc::Translate
            | VariadicFunc::ArrayPosition
            | VariadicFunc::ArrayFill { .. }
            | VariadicFunc::DateDiffTimestamp
            | VariadicFunc::DateDiffTimestampTz
            | VariadicFunc::DateDiffDate
            | VariadicFunc::DateDiffTime
            | VariadicFunc::TimezoneTime
            | VariadicFunc::RegexpSplitToArray
            | VariadicFunc::StringToArray
            | VariadicFunc::RegexpReplace => false,
        }
    }
}

impl std::fmt::Display for VariadicFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VariadicFunc::Coalesce => f.write_str("coalesce"),
            VariadicFunc::Greatest => f.write_str("greatest"),
            VariadicFunc::Least => f.write_str("least"),
            VariadicFunc::Concat => f.write_str("concat"),
            VariadicFunc::ConcatWs => f.write_str("concat_ws"),
            VariadicFunc::MakeTimestamp => f.write_str("makets"),
            VariadicFunc::PadLeading => f.write_str("lpad"),
            VariadicFunc::Substr => f.write_str("substr"),
            VariadicFunc::Replace => f.write_str("replace"),
            VariadicFunc::Translate => f.write_str("translate"),
            VariadicFunc::JsonbBuildArray => f.write_str("jsonb_build_array"),
            VariadicFunc::JsonbBuildObject => f.write_str("jsonb_build_object"),
            VariadicFunc::MapBuild { .. } => f.write_str("map_build"),
            VariadicFunc::ArrayCreate { .. } => f.write_str("array_create"),
            VariadicFunc::ArrayToString { .. } => f.write_str("array_to_string"),
            VariadicFunc::ArrayIndex { .. } => f.write_str("array_index"),
            VariadicFunc::ListCreate { .. } => f.write_str("list_create"),
            VariadicFunc::RecordCreate { .. } => f.write_str("record_create"),
            VariadicFunc::ListIndex => f.write_str("list_index"),
            VariadicFunc::ListSliceLinear => f.write_str("list_slice_linear"),
            VariadicFunc::SplitPart => f.write_str("split_string"),
            VariadicFunc::RegexpMatch => f.write_str("regexp_match"),
            VariadicFunc::HmacString | VariadicFunc::HmacBytes => f.write_str("hmac"),
            VariadicFunc::ErrorIfNull => f.write_str("error_if_null"),
            VariadicFunc::DateBinTimestamp => f.write_str("timestamp_bin"),
            VariadicFunc::DateBinTimestampTz => f.write_str("timestamptz_bin"),
            VariadicFunc::DateDiffTimestamp
            | VariadicFunc::DateDiffTimestampTz
            | VariadicFunc::DateDiffDate
            | VariadicFunc::DateDiffTime => f.write_str("datediff"),
            VariadicFunc::And => f.write_str("AND"),
            VariadicFunc::Or => f.write_str("OR"),
            VariadicFunc::RangeCreate {
                elem_type: element_type,
            } => f.write_str(match element_type {
                SqlScalarType::Int32 => "int4range",
                SqlScalarType::Int64 => "int8range",
                SqlScalarType::Date => "daterange",
                SqlScalarType::Numeric { .. } => "numrange",
                SqlScalarType::Timestamp { .. } => "tsrange",
                SqlScalarType::TimestampTz { .. } => "tstzrange",
                _ => unreachable!(),
            }),
            VariadicFunc::MakeAclItem => f.write_str("makeaclitem"),
            VariadicFunc::MakeMzAclItem => f.write_str("make_mz_aclitem"),
            VariadicFunc::ArrayPosition => f.write_str("array_position"),
            VariadicFunc::ArrayFill { .. } => f.write_str("array_fill"),
            VariadicFunc::TimezoneTime => f.write_str("timezonet"),
            VariadicFunc::RegexpSplitToArray => f.write_str("regexp_split_to_array"),
            VariadicFunc::RegexpReplace => f.write_str("regexp_replace"),
            VariadicFunc::StringToArray => f.write_str("string_to_array"),
        }
    }
}
