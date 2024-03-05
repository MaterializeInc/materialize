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

use std::collections::VecDeque;

use proc_macro::TokenStream;
use proc_macro2::{Punct, Spacing, TokenStream as TokenStream2};
use quote::{quote, ToTokens, TokenStreamExt};
use syn::parse::Parse;
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, Error, Ident, Item, ItemMod, LitInt, LitStr, Token, Type, Visibility,
};

/// Implementation for the `#[static_list]` macro.
pub fn static_list_impl(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as StaticListArgs);
    let item = parse_macro_input!(item as ItemMod);

    let static_items = match collect_items(&item, &args.ty) {
        Ok(items) => items,
        Err(e) => return e.to_compile_error().into(),
    };

    // Make sure our expected count matches how many items we actually collected.
    let expected_count = match args.expected_count.base10_parse::<usize>() {
        Ok(c) => c,
        Err(e) => return e.to_compile_error().into(),
    };
    if static_items.len() != expected_count {
        let msg = format!(
            "Expected {} items, static list would contain {}",
            expected_count,
            static_items.len()
        );
        let err = syn::Error::new(item.span(), &msg);
        return err.to_compile_error().into();
    }

    let name = syn::Ident::new(&args.name.value(), args.name.span());
    let ty = syn::Ident::new(&args.ty.value(), args.ty.span());

    let expanded = quote! {
        pub static #name : &[ &'static #ty ] = &[
            #static_items
        ];

        #item
    };

    expanded.into()
}

#[derive(Debug)]
struct StaticItem<'i>(VecDeque<&'i Ident>);

impl<'i> StaticItem<'i> {
    pub fn new(ident: &'i Ident) -> Self {
        StaticItem(VecDeque::from([ident]))
    }

    pub fn to_path(&self) -> syn::Path {
        syn::Path {
            leading_colon: None,
            segments: self
                .0
                .iter()
                .copied()
                .cloned()
                .map(|i| syn::PathSegment {
                    ident: i,
                    arguments: syn::PathArguments::None,
                })
                .collect(),
        }
    }
}

#[derive(Debug)]
struct StaticItems<'a>(Vec<StaticItem<'a>>);

impl<'a> StaticItems<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a> IntoIterator for StaticItems<'a> {
    type Item = StaticItem<'a>;
    type IntoIter = <Vec<StaticItem<'a>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> ToTokens for StaticItems<'a> {
    fn to_tokens(&self, tokens: &mut TokenStream2) {
        for path in self.0.iter().map(|item| item.to_path()) {
            tokens.append(Punct::new('&', Spacing::Joint));
            path.to_tokens(tokens);
            tokens.append(Punct::new(',', Spacing::Joint));
        }
    }
}

fn collect_items<'i, 't>(
    item_mod: &'i ItemMod,
    expected_ty: &'t LitStr,
) -> syn::Result<StaticItems<'i>> {
    let items = item_mod.content.as_ref().map(|c| &c.1[..]).unwrap_or(&[]);

    // TODO(parkmycar): Support ignoring modules.
    let Visibility::Public(_) = item_mod.vis else {
        return Err(Error::new_spanned(
            item_mod,
            "Modules in a #[static_list] must be `pub`",
        ));
    };

    let mut static_items = Vec::new();
    for item in items {
        match item {
            Item::Static(item_static) if type_matches(&item_static.ty, expected_ty) => {
                // For ease of use, we error if a static item isn't public.
                //
                // TODO(parkmycar): Support ignoring items.
                let Visibility::Public(_) = item_static.vis else {
                    return Err(Error::new_spanned(
                        item_static,
                        "All items in a #[static_list] must be `pub`",
                    ));
                };

                static_items.push(StaticItem::new(&item_static.ident));
            }
            Item::Mod(nested_item_mod) => {
                let nested_items = collect_items(nested_item_mod, expected_ty)?;
                static_items.extend(nested_items.into_iter());
            }
            _ => (),
        }
    }

    for static_item in &mut static_items {
        static_item.0.push_front(&item_mod.ident);
    }

    Ok(StaticItems(static_items))
}

#[derive(Debug)]
struct StaticListArgs {
    /// Type of static objects we should use to form the list.
    pub ty: LitStr,
    /// Name we should use for the list.
    ///
    /// Note: Requring the exact name, instead of some smart default, makes it a lot easier to
    /// discover where the list is defined using tools like grep.
    pub name: LitStr,
    /// Expected count of items, used as a smoke test.
    pub expected_count: LitInt,
}

impl Parse for StaticListArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut ty: Option<LitStr> = None;
        let mut name: Option<LitStr> = None;
        let mut expected_count: Option<LitInt> = None;

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(keywords::ty) {
                let res = input.parse::<KeyValueArg<keywords::ty, LitStr>>()?;
                ty = Some(res.val);
            } else if lookahead.peek(keywords::name) {
                let res = input.parse::<KeyValueArg<keywords::name, LitStr>>()?;
                name = Some(res.val);
            } else if lookahead.peek(keywords::expected_count) {
                let res = input.parse::<KeyValueArg<keywords::expected_count, LitInt>>()?;
                expected_count = Some(res.val);
            } else if lookahead.peek(Token![,]) {
                let _ = input.parse::<Token![,]>()?;
            } else {
                return Err(input.error("Unexpected argument"));
            }
        }

        let mut missing_args = Vec::new();
        if ty.is_none() {
            missing_args.push("ty");
        }
        if name.is_none() {
            missing_args.push("name");
        }
        if expected_count.is_none() {
            missing_args.push("expected_count");
        }

        if !missing_args.is_empty() {
            input.error(format!("Missing arguments {:?}", missing_args));
        }

        Ok(StaticListArgs {
            ty: ty.expect("checked above"),
            name: name.expect("checked above"),
            expected_count: expected_count.expect("checked above"),
        })
    }
}

fn type_matches(static_ty: &Type, expected_ty: &LitStr) -> bool {
    // Note: This probably isn't super accurrate, but it's easy.
    static_ty
        .into_token_stream()
        .to_string()
        .ends_with(&expected_ty.value())
}

struct KeyValueArg<K, V> {
    _key: K,
    val: V,
}

impl<K: Parse, V: Parse> Parse for KeyValueArg<K, V> {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let key = input.parse::<K>()?;
        let _ = input.parse::<Token![=]>()?;
        let val = input.parse::<V>()?;

        Ok(Self { _key: key, val })
    }
}

mod keywords {
    syn::custom_keyword!(ty);
    syn::custom_keyword!(name);
    syn::custom_keyword!(expected_count);
}
