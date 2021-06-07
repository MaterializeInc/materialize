// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
