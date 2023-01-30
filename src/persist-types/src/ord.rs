// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ordered_float::OrderedFloat;
use std::cmp::Ordering;

use crate::columnar::{ColumnGet, Data};

#[allow(dead_code)]
#[derive(Debug)]
pub enum ColOrd<'a> {
    Bool(&'a <bool as Data>::Col),
    I8(&'a <i8 as Data>::Col),
    I16(&'a <i16 as Data>::Col),
    I32(&'a <i32 as Data>::Col),
    I64(&'a <i64 as Data>::Col),
    U8(&'a <u8 as Data>::Col),
    U16(&'a <u16 as Data>::Col),
    U32(&'a <u32 as Data>::Col),
    U64(&'a <u64 as Data>::Col),
    F32(&'a <f32 as Data>::Col),
    F64(&'a <f64 as Data>::Col),
    Bytes(&'a <Vec<u8> as Data>::Col),
    String(&'a <String as Data>::Col),
    OptBool(&'a <Option<bool> as Data>::Col),
    OptI8(&'a <Option<i8> as Data>::Col),
    OptI16(&'a <Option<i16> as Data>::Col),
    OptI32(&'a <Option<i32> as Data>::Col),
    OptI64(&'a <Option<i64> as Data>::Col),
    OptU8(&'a <Option<u8> as Data>::Col),
    OptU16(&'a <Option<u16> as Data>::Col),
    OptU32(&'a <Option<u32> as Data>::Col),
    OptU64(&'a <Option<u64> as Data>::Col),
    OptF32(&'a <Option<f32> as Data>::Col),
    OptF64(&'a <Option<f64> as Data>::Col),
    OptBytes(&'a <Option<Vec<u8>> as Data>::Col),
    OptString(&'a <Option<String> as Data>::Col),
}

impl<'a> ColOrd<'a> {
    /// Returns the index of the (min, max) values in the column.
    pub fn minmax(&self, len: usize) -> (usize, usize) {
        // TODO: This is an expensive operation. There are several ways to reduce the cost:
        // 1. Compare idx to idx+1, and then compare the smaller of the two to min and the
        // greater of the two to max. This reduces the # of comparisons to 1.5*n instead of 2*n.
        // 2. For the Arrow types that have the same total ordering as their Rust equivalents,
        // use arrow2's SIMD-backed aggregations. Note that arrow2's aggregations may handle
        // null values differently than in Rust.
        // 3. Parallelize the comparison, as each column's minmax can be computed independently.
        let mut min_idx = 0;
        let mut max_idx = 0;
        for idx in 0..len {
            match self.cmp(min_idx, idx) {
                Ordering::Less | Ordering::Equal => {}
                Ordering::Greater => min_idx = idx,
            }
            match self.cmp(max_idx, idx) {
                Ordering::Less => max_idx = idx,
                Ordering::Equal | Ordering::Greater => {}
            }
        }
        (min_idx, max_idx)
    }

    fn cmp(&self, idx0: usize, idx1: usize) -> Ordering {
        match *self {
            ColOrd::Bool(x) => ColumnGet::<bool>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::I8(x) => ColumnGet::<i8>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::I16(x) => ColumnGet::<i16>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::I32(x) => ColumnGet::<i32>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::I64(x) => ColumnGet::<i64>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::U8(x) => ColumnGet::<u8>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::U16(x) => ColumnGet::<u16>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::U32(x) => ColumnGet::<u32>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::U64(x) => ColumnGet::<u64>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::F32(x) => OrderedFloat(ColumnGet::<f32>::get(x, idx0))
                .cmp(&OrderedFloat(ColumnGet::get(x, idx1))),
            ColOrd::F64(x) => OrderedFloat(ColumnGet::<f64>::get(x, idx0))
                .cmp(&OrderedFloat(ColumnGet::get(x, idx1))),
            ColOrd::Bytes(x) => {
                ColumnGet::<Vec<u8>>::get(x, idx0).cmp(ColumnGet::<Vec<u8>>::get(x, idx1))
            }
            ColOrd::String(x) => {
                ColumnGet::<String>::get(x, idx0).cmp(ColumnGet::<String>::get(x, idx1))
            }
            ColOrd::OptBool(x) => {
                ColumnGet::<Option<bool>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptI8(x) => ColumnGet::<Option<i8>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::OptI16(x) => {
                ColumnGet::<Option<i16>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptI32(x) => {
                ColumnGet::<Option<i32>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptI64(x) => {
                ColumnGet::<Option<i64>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptU8(x) => ColumnGet::<Option<u8>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1)),
            ColOrd::OptU16(x) => {
                ColumnGet::<Option<u16>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptU32(x) => {
                ColumnGet::<Option<u32>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptU64(x) => {
                ColumnGet::<Option<u64>>::get(x, idx0).cmp(&ColumnGet::get(x, idx1))
            }
            ColOrd::OptF32(x) => ColumnGet::<Option<f32>>::get(x, idx0)
                .map(OrderedFloat)
                .cmp(&ColumnGet::get(x, idx1).map(OrderedFloat)),
            ColOrd::OptF64(x) => ColumnGet::<Option<f64>>::get(x, idx0)
                .map(OrderedFloat)
                .cmp(&ColumnGet::get(x, idx1).map(OrderedFloat)),
            ColOrd::OptBytes(x) => {
                ColumnGet::<Option<Vec<u8>>>::get(x, idx0)
                    .cmp(&ColumnGet::<Option<Vec<u8>>>::get(x, idx1))
            }
            ColOrd::OptString(x) => {
                ColumnGet::<Option<String>>::get(x, idx0)
                    .cmp(&ColumnGet::<Option<String>>::get(x, idx1))
            }
        }
    }
}

#[derive(Debug)]
pub struct ColsOrd<'a> {
    cols: &'a [ColOrd<'a>],
}

impl<'a> ColsOrd<'a> {
    pub fn new(cols: &'a [ColOrd<'a>]) -> Self {
        ColsOrd { cols }
    }

    pub fn key(&'a self, idx: usize) -> ColsOrdKey<'a> {
        ColsOrdKey {
            idx,
            cols_ord: self,
        }
    }

    fn cmp(&self, idx0: usize, idx1: usize) -> Ordering {
        for col in self.cols.iter() {
            let cmp = col.cmp(idx0, idx1);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

#[derive(Debug)]
pub struct ColsOrdKey<'a> {
    pub idx: usize,
    cols_ord: &'a ColsOrd<'a>,
}

impl<'a> PartialEq for ColsOrdKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<'a> Eq for ColsOrdKey<'a> {}

impl<'a> PartialOrd for ColsOrdKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ColsOrdKey<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Important! Make sure this is used correctly (only ColsOrdKeys issues by
        #![allow(clippy::as_conversions)]
        assert_eq!(self.cols_ord as *const _, other.cols_ord as *const _);
        self.cols_ord.cmp(self.idx, other.idx)
    }
}
