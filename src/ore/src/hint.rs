// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the criterion project.
// The original source code was retrieved on February 19, 2020 from:
//
//     https://github.com/bheisler/criterion.rs/blob/76061c756347c0575bcfd044a9027dcf66f85a3e/src/lib.rs
//
// The original source code is dual-licensed under the Apache 2.0 and MIT
// licenses, copies of which can be found in the LICENSE file at the root of
// this repository.

//! Extensions to `std::hint`.

/// A function that is opaque to the optimizer, used to prevent the compiler
/// from optimizing away computations in a benchmark.
///
/// This variant is stable-compatible, but it may cause some performance
/// overhead or fail to prevent code from being eliminated.
///
/// When `std::hint::black_box` is stabilized, this function can be removed.
pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = std::ptr::read_volatile(&dummy);
        std::mem::forget(dummy);
        ret
    }
}
