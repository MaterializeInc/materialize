// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use crate::columnar::Data;

// WIP we might be able to replace this with `Box<dyn ColumnGet>` if we added an
// Ord bound to ColumnGet.
#[derive(Debug)]
pub enum ColOrd<'a> {
    I64(&'a <i64 as Data>::Col),
    String(&'a <String as Data>::Col),
}

impl<'a> ColOrd<'a> {
    fn cmp(&self, idx0: usize, idx1: usize) -> Ordering {
        match self {
            ColOrd::I64(x) => x[idx0].cmp(&x[idx1]),
            ColOrd::String(x) => x.value(idx0).cmp(x.value(idx1)),
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
        assert_eq!(self.cols_ord as *const _, other.cols_ord as *const _);
        self.cols_ord.cmp(self.idx, other.idx)
    }
}
