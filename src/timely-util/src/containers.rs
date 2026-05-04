// Copyright Materialize, Inc. and contributors. All rights reserved.
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

//! Reusable containers.

pub mod stack;

pub use alloc::{enable_columnar_lgalloc, set_enable_columnar_lgalloc};

mod alloc {
    thread_local! {
        static ENABLE_COLUMNAR_LGALLOC: std::cell::Cell<bool> =
            const { std::cell::Cell::new(false) };
    }

    /// Returns `true` if columnar allocations should come from lgalloc.
    ///
    /// Currently a no-op: `Column::Align` is now backed by a plain `Vec<u64>` rather than an
    /// lgalloc `Region`, so this flag has no effect. Kept for source compatibility with callers
    /// in `mz-compute` that flip it via dyncfg.
    #[inline]
    pub fn enable_columnar_lgalloc() -> bool {
        ENABLE_COLUMNAR_LGALLOC.get()
    }

    /// Set whether columnar allocations should come from lgalloc. See
    /// [`enable_columnar_lgalloc`] — currently has no effect on allocation behavior.
    pub fn set_enable_columnar_lgalloc(enabled: bool) {
        ENABLE_COLUMNAR_LGALLOC.set(enabled);
    }
}
