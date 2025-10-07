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

//! test macro with auto-initialized logging

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{ItemFn, Meta, ReturnType, Token, parse_macro_input, parse_quote};

/// Based on <https://github.com/d-e-s-o/test-log>
/// Copyright (C) 2019-2022 Daniel Mueller <deso@posteo.net>
/// SPDX-License-Identifier: (Apache-2.0 OR MIT)
///
/// Implementation for the `test` macro.
pub fn test_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args: Vec<_> =
        parse_macro_input!(attr with Punctuated::<Meta, Token![,]>::parse_terminated)
            .into_iter()
            .collect();
    let input = parse_macro_input!(item as ItemFn);

    let inner_test = match args.as_slice() {
        [] => parse_quote! { ::core::prelude::v1::test },
        [Meta::Path(path)] => quote! { #path },
        [Meta::List(list)] => quote! { #list },
        _ => panic!("unsupported attributes supplied: {:?}", args),
    };

    expand_wrapper(&inner_test, &input)
}

fn expand_logging_init() -> TokenStream2 {
    let crate_name = std::env::var("CARGO_PKG_NAME").unwrap();
    if crate_name == "mz-ore" {
        quote! {
          {
            use crate::test;
            let _ = test::init_logging();
          }
        }
    } else {
        quote! {
          {
            let _ = ::mz_ore::test::init_logging();
          }
        }
    }
}

/// Emit code for a wrapper function around a test function.
fn expand_wrapper(inner_test: &TokenStream2, wrappee: &ItemFn) -> TokenStream {
    let attrs = &wrappee.attrs;
    let async_ = &wrappee.sig.asyncness;
    let await_ = if async_.is_some() {
        quote! {.await}
    } else {
        quote! {}
    };
    let body = &wrappee.block;
    let test_name = &wrappee.sig.ident;

    // Note that Rust does not allow us to have a test function with
    // #[should_panic] that has a non-unit return value.
    let ret = match &wrappee.sig.output {
        ReturnType::Default => quote! {},
        ReturnType::Type(_, type_) => quote! {-> #type_},
    };

    let logging_init = expand_logging_init();

    let result = quote! {
      #[#inner_test]
      #(#attrs)*
      #async_ fn #test_name() #ret {
        #async_ fn test_impl() #ret {
          #body
        }

        #logging_init

        test_impl()#await_
      }
    };
    result.into()
}
