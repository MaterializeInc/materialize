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

mod generate;
mod modifiers;
mod shape;
mod signature;
mod source;
mod sqlfunc;

pub use sqlfunc::sqlfunc;

/// Expands `#[sqlfunc(attr)]` on `item` and returns the formatted expansion, or the
/// formatted errors, along with the formatted input.
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
        &sqlfunc(attr, item, false)
            .unwrap_or_else(|err| err.write_errors())
            .to_string(),
    );
    (output, input)
}

/// [`test_sqlfunc`] over source text, as the generated snapshot tests pass it.
#[cfg(any(feature = "test", test))]
pub fn test_sqlfunc_str(attr: &str, item: &str) -> (String, String) {
    test_sqlfunc(attr.parse().unwrap(), item.parse().unwrap())
}

#[cfg(test)]
mod test;
