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

//! Internal utility proc-macros for persist.
//!
//! Note: This is separate from the `mz_persist_client` crate because
//! `proc-macro` crates are only allowed to export procedural macros and nothing
//! else.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{ItemFn, ReturnType, parse_macro_input};

/// Persist wrapper around the `test` macro.
///
/// The wrapper automatically runs the test with various interesting
/// configurations.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test_impl(attr, item)
}

/// Implementation for the `#[mz_persist_proc::test]` macro.
fn test_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = TokenStream2::from(attr);
    let item = parse_macro_input!(item as ItemFn);

    let attrs = &item.attrs;
    let async_ = &item.sig.asyncness;
    let await_ = if async_.is_some() {
        quote! {.await}
    } else {
        quote! {}
    };
    let inputs = &item.sig.inputs;
    let body = &item.block;
    let test_name = &item.sig.ident;

    // Note that Rust does not allow us to have a test function with
    // #[should_panic] that has a non-unit return value.
    let ret = match &item.sig.output {
        ReturnType::Default => quote! {},
        ReturnType::Type(_, type_) => quote! {-> #type_},
    };

    quote! {
        #[::mz_ore::test(
            #args
        )]
        #(#attrs)*
        #async_ fn #test_name() #ret {
            #async_ fn test_impl(#inputs) #ret {
              #body
            }

            let dyncfgs = [
                {
                    // Inline writes disabled
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_inline_writes_single_max_bytes", ::mz_dyncfg::ConfigVal::Usize(0));
                    x.add_dynamic("persist_inline_writes_total_max_bytes", ::mz_dyncfg::ConfigVal::Usize(0));
                    x
                },
                {
                    // Inline writes enabled
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_inline_writes_single_max_bytes", ::mz_dyncfg::ConfigVal::Usize(4 * 1024));
                    x.add_dynamic("persist_inline_writes_total_max_bytes", ::mz_dyncfg::ConfigVal::Usize(1024 * 1024));
                    x
                },
                {
                    // Stress inline writes backpressure
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_inline_writes_single_max_bytes", ::mz_dyncfg::ConfigVal::Usize(4 * 1024));
                    x.add_dynamic("persist_inline_writes_total_max_bytes", ::mz_dyncfg::ConfigVal::Usize(0));
                    x
                },
                {
                    // Enable new compaction tracking / claiming
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_claim_unclaimed_compactions", ::mz_dyncfg::ConfigVal::Bool(true));
                    x
                },
                {
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_record_schema_id", ::mz_dyncfg::ConfigVal::Bool(true));
                    x
                },
                {
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_encoding_enable_dictionary", ::mz_dyncfg::ConfigVal::Bool(true));
                    x
                },
                {
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_batch_max_run_len", ::mz_dyncfg::ConfigVal::Usize(4));
                    x
                },

                 {
                    let mut x = ::mz_dyncfg::ConfigUpdates::default();
                    x.add_dynamic("persist_enable_incremental_compaction", ::mz_dyncfg::ConfigVal::Bool(true));
                    x
                },
            ];

            for (idx, dyncfgs) in dyncfgs.into_iter().enumerate() {
                let debug = dyncfgs.updates.iter().map(|(name, val)| {
                    format!(" {}={:?}", name, val)
                }).collect::<String>();
                eprintln!("mz_persist_proc::test {}{}", idx, debug);
                test_impl(dyncfgs)#await_
            }
          }
    }
    .into()
}
