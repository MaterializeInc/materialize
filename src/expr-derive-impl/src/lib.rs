// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of proc macros to derive SQL function traits.
//!
//! This is separate from the actual proc macro to allow exporting the
//! function defining the proc macro itself. Proc macro crates cannot
//! export anything but proc macros.

mod sqlfunc;

pub use sqlfunc::sqlfunc;

/// Non-exported version of `sqlfunc` for testing purposes, accepting proc_macro2 token streams.
#[cfg(any(feature = "test", test))]
fn sqlfunc_for_test(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> darling::Result<proc_macro2::TokenStream> {
    sqlfunc(attr, item, false)
}

#[cfg(any(feature = "test", test))]
pub fn test_sqlfunc(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> (String, String) {
    fn rust_fmt(input: &str) -> String {
        let file = syn::parse_file(input).unwrap();
        prettyplease::unparse(&file)
    }

    let input = rust_fmt(&format!("#[sqlfunc({attr})]\n{item}"));
    let output = rust_fmt(
        &sqlfunc_for_test(attr, item)
            .unwrap_or_else(|err| err.write_errors())
            .to_string(),
    );
    (output, input)
}

#[cfg(any(feature = "test", test))]
pub fn test_sqlfunc_str(attr: &str, item: &str) -> (String, String) {
    test_sqlfunc(attr.parse().unwrap(), item.parse().unwrap())
}

#[cfg(test)]
mod test {
    use quote::quote;

    #[mz_ore::test]
    fn insta_test_add_int16() {
        let attr = quote! {
                is_monotone = (true, true),
                output_type = i16,
                is_infix_op = true,
                sqlname = "+",
                propagates_nulls = true
        };
        let item = quote! {
            fn add_int16<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
                a.unwrap_int16()
                    .checked_add(b.unwrap_int16())
                    .ok_or(EvalError::NumericFieldOverflow)
                    .map(Datum::from)
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("add_int16", output, &input);
    }

    #[mz_ore::test]
    fn insta_test_unary() {
        let attr = quote! {};
        let item = quote! {
            fn unary_fn<'a>(a: Datum<'a>) -> bool {
                unimplemented!()
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("unary_fn", output, &input);
    }

    #[mz_ore::test]
    fn insta_test_unary_arena() {
        let attr = quote! {};
        let item = quote! {
            fn unary_fn<'a>(a: Datum<'a>, temp_storage: &RowArena) -> bool {
                unimplemented!()
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("unary_arena_fn", output, &input);
    }

    #[mz_ore::test]
    fn insta_test_unary_ref() {
        let attr = quote! {};
        let item = quote! {
            fn unary_fn<'a>(a: &i16) -> bool {
                unimplemented!()
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("unary_ref", output, &input);
    }

    #[mz_ore::test]
    fn insta_test_complex_output_type() {
        let attr = quote! {
                is_monotone = (true, true),
                output_type = "Option<bool>",
                is_infix_op = true,
                sqlname = "test",
                propagates_nulls = true
        };
        let item = quote! {
            fn complex_output_type_fn<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
                unimplemented!()
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("complex_type", output, &input);
    }

    #[mz_ore::test]
    fn insta_test_binary_arena() {
        let attr = quote! {};
        let item = quote! {
            fn unary_fn<'a>(a: Datum<'a>, b: u16, temp_storage: &RowArena) -> bool {
                unimplemented!()
            }
        };
        let (output, input) = super::test_sqlfunc(attr, item);
        insta::assert_snapshot!("binary_arena_fn", output, &input);
    }
}
