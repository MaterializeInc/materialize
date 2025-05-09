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
/// The `sqlfunc` attribute macro is used to derive SQL function traits for unary and binary
/// functions. Depending on the kind of function, it will implement the appropriate traits.
/// For unary functions, it implements the `EagerUnaryFunc` trait, and for binary functions,
/// it implements the `EagerBinaryFunc` trait.
///
/// The macro takes the following arguments:
/// * `is_monotone`: An expression indicating whether the function is monotone. For unary functions,
///   it should be an expression that evaluates to a boolean. For binary functions, it should be a
///   tuple of two expressions, each evaluating to a boolean. See `LazyBinaryFunc` for details.
/// * `sqlname`: The SQL name of the function.
/// * `preserves_uniqueness`: A boolean indicating whether the function preserves uniqueness.
///   Unary functions only.
/// * `inverse`: A function that is the inverse of the function. Applies to unary functions only.
/// * `negate`: A function that negates the function. Applies to binary functions only. For example,
///   the `!=` operator is the negation of the `=` operator, and we'd mark the `Eq` function with
///   `negate = Some(BinaryFunc::NotEq)`.
/// * `is_infix_op`: A boolean indicating whether the function is an infix operator. Applies to
///   binary functions only.
/// * `output_type`: The output type of the function.
/// * `output_type_expr`: An expression that evaluates to the output type. Applies to binary
///   functions only. The expression has access to the `input_type_a` and `input_type_b` variables,
///   and should evaluate to a `ColumnType` value. Requires `introduces_nulls`, and conflicts with
///   `output_type`.
/// * `could_error`: A boolean indicating whether the function could error.
/// * `propagate_nulls`: A boolean indicating whether the function propagates nulls. Applies to
///   binary functions only.
/// * `introduces_nulls`: A boolean indicating whether the function introduces nulls. Applies to
///   all functions.
///
/// # Limitations
/// * The input and output types can contain lifetime parameters, as long as they are `'a`.
/// * Unary functions cannot yet receive a `&RowArena` as an argument.
/// * The `output_type`
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
