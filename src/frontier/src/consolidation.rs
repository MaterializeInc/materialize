// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Consolidation of `(data, diff)` and `(data, time, diff)` collections.

use crate::difference::Semigroup;

pub fn consolidate<D: Ord, R: Semigroup>(vec: &mut Vec<(D, R)>) {
    let len = consolidate_slice(&mut vec[..]);
    vec.truncate(len);
}

pub fn consolidate_slice<D: Ord, R: Semigroup>(slice: &mut [(D, R)]) -> usize {
    if slice.len() <= 1 {
        return if slice.len() == 1 && slice[0].1.is_zero() { 0 } else { slice.len() };
    }
    slice.sort_by(|a, b| a.0.cmp(&b.0));
    let mut w = 0;
    for r in 1..slice.len() {
        if slice[w].0 == slice[r].0 {
            let d = slice[r].1.clone();
            slice[w].1.plus_equals(&d);
        } else {
            if !slice[w].1.is_zero() { w += 1; }
            slice.swap(w, r);
        }
    }
    if !slice[w].1.is_zero() { w += 1; }
    w
}

pub fn consolidate_updates<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>) {
    let len = consolidate_updates_slice(&mut vec[..]);
    vec.truncate(len);
}

pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {
    if slice.len() <= 1 {
        return if slice.len() == 1 && slice[0].2.is_zero() { 0 } else { slice.len() };
    }
    slice.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
    let mut w = 0;
    for r in 1..slice.len() {
        if slice[w].0 == slice[r].0 && slice[w].1 == slice[r].1 {
            let d = slice[r].2.clone();
            slice[w].2.plus_equals(&d);
        } else {
            if !slice[w].2.is_zero() { w += 1; }
            slice.swap(w, r);
        }
    }
    if !slice[w].2.is_zero() { w += 1; }
    w
}
