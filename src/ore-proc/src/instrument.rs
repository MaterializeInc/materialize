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

//! instrument macro with improved default safety
//!
//! This wraps the `tracing::instrument` macro and:
//! - adds `skip_all`
//! - errors on `skip`
//!
//! Its purpose is to prevent accidentally including large function arguments in tracing spans. By
//! enforcing the use of skip_all, users must use the `fields` argument of the `tracing::instrument`
//! macro to manually select their desired fields.

use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2, TokenTree};
use quote::quote;

/// Implementation for the `#[instrument]` macro.
pub fn instrument_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = TokenStream2::from(attr);
    let item = TokenStream2::from(item);

    // syn appears to not be able to parse the `%` part of things like `#[instrument(fields(shard =
    // %id))]`, so we use the more naive proc_macro crate and look for strings.
    let mut iter = attr.into_iter();
    let mut args: TokenStream2 = quote! { skip_all };
    while let Some(tok) = iter.next() {
        match &tok {
            TokenTree::Ident(ident) => match ident.to_string().as_str() {
                "skip_all" => panic!("skip_all already included; remove it"),
                "skip" => panic!("skip prohibited; use fields"),
                _ => {}
            },
            _ => {}
        }
        args.extend([tok])
    }
    quote! {
        #[allow(clippy::disallowed_macros)]
        #[::tracing::instrument(
            #args
        )]
        #item
    }
    .into()
}
