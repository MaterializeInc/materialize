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

//! Internal utility proc-macros for Materialize.
//!
//! Note: This is separate from the `mz_ore` crate because `proc-macro` crates are only allowed
//! to export procedural macros and nothing else.

use proc_macro::TokenStream;

mod instrument;
mod static_list;
mod test;

/// A macro that collects all of the static objects of a specific type in the annotated module into
/// a single list.
///
/// ```
/// use mz_ore_proc::static_list;
///
/// #[static_list(ty = "usize", name = "ALL_NUMS", expected_count = 3)]
/// pub mod my_items {
///     pub static FOO: usize = 1;
///
///     // Won't get included in the list, because it's not a usize.
///     pub static NON_USIZE: u8 = 10;
///
///     // Nested modules also work.
///     pub mod inner {
///         pub static BAR: usize = 5;
///         pub static BAZ: usize = 11;
///     }
/// }
///
/// assert_eq!(ALL_NUMS, &[&1usize, &5, &11]);
/// ```
///
/// ### Wrong expected count.
///
/// You're required to specify an expected count of items as a smoke test to make sure all of the
/// items you think should get included, are included.
///
/// ```compile_fail
/// # use mz_ore_proc::static_list;
/// #[static_list(ty = "usize", name = "ALL_NUMS", expected_count = 2)]
/// pub mod items {
///     pub static A: usize = 10;
///     pub static B: isize = 20;
/// }
/// ```
#[proc_macro_attribute]
pub fn static_list(args: TokenStream, item: TokenStream) -> TokenStream {
    static_list::static_list_impl(args, item)
}

/// Materialize wrapper around the `#[tracing::insrument]` macro.
///
/// We wrap the `tracing::instrument` macro to skip tracing all arguments by default, this prevents
/// noisy or actively harmful things (e.g. the entire Catalog) from accidentally getting traced. If
/// you would like to include a function's argument in the traced span, you can use the
/// `fields(...)` syntax.
#[proc_macro_attribute]
pub fn instrument(attr: TokenStream, item: TokenStream) -> TokenStream {
    instrument::instrument_impl(attr, item)
}

/// Materialize wrapper around the `test` macro.
///
/// The wrapper automatically initializes our logging infrastructure.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::test_impl(attr, item)
}
