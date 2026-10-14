// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! The `#[sqlfunc]` attribute macro's entry point.
//!
//! Parses the attribute, classifies the annotated function's arity, and hands the
//! matching [`crate::shape::Shape`] to [`crate::generate::generate`].

use darling::FromMeta;
use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::generate::generate;
use crate::modifiers::Modifiers;
use crate::shape::Shape;
use crate::signature;

/// Implementation for the `#[sqlfunc]` macro. The first parameter is the attribute
/// arguments, the second is the function body. The third parameter indicates
/// whether to include the test function in the output.
///
/// The feature `test` must be enabled to include the test function.
pub fn sqlfunc(
    attr: TokenStream,
    item: TokenStream,
    include_test: bool,
) -> darling::Result<TokenStream> {
    let mut attr_args = darling::ast::NestedMeta::parse_meta_list(attr.clone())?;

    // Check if the first attribute arg is a bare Path (struct name).
    let struct_ty = match attr_args.first() {
        Some(darling::ast::NestedMeta::Meta(syn::Meta::Path(_))) => {
            let darling::ast::NestedMeta::Meta(syn::Meta::Path(path)) = attr_args.remove(0) else {
                unreachable!()
            };
            Some(path)
        }
        _ => None,
    };

    let modifiers = Modifiers::from_list(&attr_args)?;
    let generate_tests = modifiers.generates_test();
    let func = syn::parse2::<syn::ItemFn>(item.clone())?;

    let has_self = signature::has_receiver(&func);
    let Some(shape) = determine_shape(&func) else {
        return Err(darling::Error::custom(
            "nullary functions are not supported",
        ));
    };
    let tokens = generate(shape, &func, modifiers, struct_ty, has_self, &attr)?;

    let test = (generate_tests && include_test).then(|| generate_test(attr, item, &func.sig.ident));

    Ok(quote! {
        #tokens
        #test
    })
}

#[cfg(any(feature = "test", test))]
fn generate_test(attr: TokenStream, item: TokenStream, name: &Ident) -> TokenStream {
    let attr = attr.to_string();
    let item = item.to_string();
    let test_name = Ident::new(&format!("test_{}", name), name.span());
    let fn_name = name.to_string();

    quote! {
        #[cfg(test)]
        #[cfg_attr(miri, ignore)] // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
        #[mz_ore::test]
        fn #test_name() {
            let (output, input) = mz_expr_derive_impl::test_sqlfunc_str(#attr, #item);
            insta::assert_snapshot!(#fn_name, output, &input);
        }
    }
}

#[cfg(not(any(feature = "test", test)))]
fn generate_test(_attr: TokenStream, _item: TokenStream, _name: &Ident) -> TokenStream {
    quote! {}
}

/// Checks whether a parameter's type is `Variadic<...>` or `OptionalArg<...>`,
/// which indicates the function should be treated as variadic regardless of
/// parameter count.
fn is_variadic_arg(arg: &syn::FnArg) -> bool {
    if let syn::FnArg::Typed(pat) = arg {
        if let syn::Type::Path(path) = &*pat.ty {
            if let Some(segment) = path.path.segments.last() {
                let ident = segment.ident.to_string();
                return ident == "Variadic" || ident == "OptionalArg";
            }
        }
    }
    false
}

/// The shape of a function annotated with `#[sqlfunc]`, or `None` for a function
/// without SQL arguments.
///
/// Only the SQL arguments count, so a `&self` receiver and a trailing `&RowArena` are
/// skipped. A `Variadic<T>` or `OptionalArg<T>` argument makes the function variadic
/// regardless of how many arguments it has.
fn determine_shape(func: &syn::ItemFn) -> Option<Shape> {
    let args = signature::sql_args(func);
    if args.iter().any(|arg| is_variadic_arg(arg)) {
        return Some(Shape::Variadic);
    }
    match args.len() {
        0 => None,
        1 => Some(Shape::Unary),
        2 => Some(Shape::Binary),
        _ => Some(Shape::Variadic),
    }
}
