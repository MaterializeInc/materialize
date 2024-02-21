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

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;
use syn::AttributeArgs;
use syn::ItemFn;
use syn::Meta;
use syn::NestedMeta;

#[proc_macro_attribute]
pub fn instrument(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as AttributeArgs);
    let input = parse_macro_input!(item as ItemFn);

    let mut output = Vec::new();
    for arg in &args {
        match arg {
            NestedMeta::Meta(Meta::Path(path)) => {
                if let Some(ident) = path.get_ident() {
                    // Forcibly included, so remove.
                    if *ident == "skip_all" {
                        continue;
                    }
                }
            }
            NestedMeta::Meta(Meta::List(list)) => {
                if let Some(ident) = list.path.get_ident() {
                    if *ident == "skip" {
                        panic!("skip prohibited; use fields");
                    }
                }
            }
            _ => {}
        }
        output.push(arg);
    }

    let result = quote! {
      #[tracing::instrument(skip_all, #(#output),*)]
      #input
    };
    result.into()
}
