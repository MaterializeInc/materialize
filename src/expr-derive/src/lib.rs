// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Proc macros to derive SQL function traits.

/// Derive function traits for SQL functions.
///
/// The `sqlfunc` attribute macro is used to derive SQL function traits for unary, binary,
/// and variadic functions. Depending on the kind of function, it will implement the
/// appropriate traits: `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc`.
///
/// `doc/developer/sqlfunc.md` documents the modifiers, their defaults, and which arities
/// accept them.
///
/// # Limitations
/// * The input and output types can contain lifetime parameters, as long as they are `'a`.
///
/// # Examples
/// ```ignore
/// use mz_expr_derive::sqlfunc;
/// #[sqlfunc(sqlname = "!")]
/// fn negate(a: bool) -> bool {
///    !a
/// }
/// ```
#[proc_macro_attribute]
pub fn sqlfunc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    mz_expr_derive_impl::sqlfunc(attr.into(), item.into(), true)
        .unwrap_or_else(|err| err.write_errors())
        .into()
}
