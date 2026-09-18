// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! Per-arity descriptions of the three scalar function shapes.
//!
//! A [`Shape`] answers the questions that differ between `EagerUnaryFunc`,
//! `EagerBinaryFunc`, and `EagerVariadicFunc`. Adding a modifier to an arity means
//! adding a row to that arity's table here, not writing expansion code.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;

/// The three scalar function arities the macro generates for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    Unary,
    Binary,
    Variadic,
}

/// A modifier that maps directly onto one optional trait method.
///
/// Modifiers that do not produce a trait method, such as `sqlname`, `output_type`,
/// `output_type_expr`, `test`, and `skip_display`, are absent: `crate::generate::generate`
/// handles those explicitly because they feed `Display`, the output-type body, or the
/// expansion decision instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Modifier {
    CouldError,
    IntroducesNulls,
    IsMonotone,
    IsInfixOp,
    PropagatesNulls,
    Inverse,
    PreservesUniqueness,
    IsEliminableCast,
    Negate,
    IsInfinityMonotone,
    IsAssociative,
}

impl Modifier {
    /// The attribute key, which is also the generated method name.
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Modifier::CouldError => "could_error",
            Modifier::IntroducesNulls => "introduces_nulls",
            Modifier::IsMonotone => "is_monotone",
            Modifier::IsInfixOp => "is_infix_op",
            Modifier::PropagatesNulls => "propagates_nulls",
            Modifier::Inverse => "inverse",
            Modifier::PreservesUniqueness => "preserves_uniqueness",
            Modifier::IsEliminableCast => "is_eliminable_cast",
            Modifier::Negate => "negate",
            Modifier::IsInfinityMonotone => "is_infinity_monotone",
            Modifier::IsAssociative => "is_associative",
        }
    }
}

/// The return type of a generated override method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReturnTy {
    Bool,
    BoolPair,
    OptUnaryFunc,
    OptBinaryFunc,
}

impl ReturnTy {
    pub(crate) fn to_tokens(&self) -> TokenStream {
        match self {
            ReturnTy::Bool => quote! { bool },
            ReturnTy::BoolPair => quote! { (bool, bool) },
            ReturnTy::OptUnaryFunc => quote! { Option<crate::UnaryFunc> },
            ReturnTy::OptBinaryFunc => quote! { Option<crate::BinaryFunc> },
        }
    }
}

// `crate::generate::override_methods` walks these tables in order to generate each
// arity's override methods. The order here therefore determines the method order
// in the generated code, which the insta snapshots under `sqlfunc.rs` pin exactly,
// so it is load-bearing, not cosmetic.
const UNARY_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::Inverse, ReturnTy::OptUnaryFunc),
    (Modifier::IsMonotone, ReturnTy::Bool),
    (Modifier::PreservesUniqueness, ReturnTy::Bool),
    (Modifier::IsEliminableCast, ReturnTy::Bool),
];

const BINARY_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::IsInfixOp, ReturnTy::Bool),
    (Modifier::IsMonotone, ReturnTy::BoolPair),
    (Modifier::IsInfinityMonotone, ReturnTy::Bool),
    (Modifier::Negate, ReturnTy::OptBinaryFunc),
    (Modifier::PropagatesNulls, ReturnTy::Bool),
];

const VARIADIC_MODIFIERS: &[(Modifier, ReturnTy)] = &[
    (Modifier::CouldError, ReturnTy::Bool),
    (Modifier::IntroducesNulls, ReturnTy::Bool),
    (Modifier::IsInfixOp, ReturnTy::Bool),
    (Modifier::IsMonotone, ReturnTy::Bool),
    (Modifier::IsAssociative, ReturnTy::Bool),
    (Modifier::PropagatesNulls, ReturnTy::Bool),
];

impl Shape {
    /// The modifiers this arity accepts, with the return type of each generated
    /// method. A modifier absent from this table is rejected.
    pub(crate) fn modifiers(&self) -> &'static [(Modifier, ReturnTy)] {
        match self {
            Shape::Unary => UNARY_MODIFIERS,
            Shape::Binary => BINARY_MODIFIERS,
            Shape::Variadic => VARIADIC_MODIFIERS,
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Shape::Unary => "unary",
            Shape::Binary => "binary",
            Shape::Variadic => "variadic",
        }
    }

    /// The trait the generated impl implements.
    pub(crate) fn trait_path(&self) -> TokenStream {
        match self {
            Shape::Unary => quote! { crate::func::EagerUnaryFunc },
            Shape::Binary => quote! { crate::func::binary::EagerBinaryFunc },
            Shape::Variadic => quote! { crate::func::variadic::EagerVariadicFunc },
        }
    }

    /// The name and sole parameter of the trait method that computes the output
    /// `SqlColumnType`.
    ///
    /// `EagerVariadicFunc` declares no `output_sql_type`. Its core method is named
    /// `output_type` and takes the SQL column types directly, where the unary and
    /// binary traits declare `output_sql_type` and keep `output_type` as a wrapper
    /// over `ReprColumnType`.
    pub(crate) fn output_method(&self) -> (Ident, TokenStream) {
        let span = Span::call_site();
        match self {
            Shape::Unary => (
                Ident::new("output_sql_type", span),
                quote! { input_type: mz_repr::SqlColumnType },
            ),
            Shape::Binary => (
                Ident::new("output_sql_type", span),
                quote! { input_types: &[mz_repr::SqlColumnType] },
            ),
            Shape::Variadic => (
                Ident::new("output_type", span),
                quote! { input_types: &[mz_repr::SqlColumnType] },
            ),
        }
    }

    /// The tail of the output-type method, which sets the nullability of the
    /// `output` column type the caller computed.
    ///
    /// The caller binds `output`, `nullable`, and `propagates_nulls`, and names the
    /// input parameter as [`Shape::output_method`] spells it. `checks` are
    /// per-position nullability checks over `input_types`. Unary receives a single
    /// column type, so it has no position to check and ignores them.
    ///
    /// The result is nullable when the function itself introduces nulls, when a
    /// parameter that rejects NULL is handed a nullable input, because the runtime
    /// conversion then rejects the NULL, or when the function propagates nulls and
    /// some input is nullable, because the optimizer short-circuits an all-NULL call.
    pub(crate) fn nullability(&self, checks: &[TokenStream]) -> TokenStream {
        match self {
            Shape::Unary => quote! {
                output.nullable(nullable || (propagates_nulls && input_type.nullable))
            },
            Shape::Binary => quote! {
                let non_nullable_input_is_nullable = false #(#checks)*;
                let inputs_nullable = input_types.iter().any(|it| it.nullable);
                let is_null = nullable
                    || non_nullable_input_is_nullable
                    || (propagates_nulls && inputs_nullable);
                output.nullable(is_null)
            },
            Shape::Variadic => quote! {
                let non_nullable_input_is_nullable = false #(#checks)*;
                let inputs_nullable = input_types.iter().any(|it| it.nullable);
                output.nullable(
                    nullable
                    || non_nullable_input_is_nullable
                    || (propagates_nulls && inputs_nullable)
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Modifier, Shape};

    fn has(shape: Shape, m: Modifier) -> bool {
        shape
            .modifiers()
            .iter()
            .any(|(candidate, _)| *candidate == m)
    }

    #[mz_ore::test]
    fn unary_accepts_inverse_and_rejects_negate() {
        assert!(has(Shape::Unary, Modifier::Inverse));
        assert!(has(Shape::Unary, Modifier::PreservesUniqueness));
        assert!(has(Shape::Unary, Modifier::IsEliminableCast));
        assert!(!has(Shape::Unary, Modifier::Negate));
        assert!(!has(Shape::Unary, Modifier::IsInfixOp));
        assert!(!has(Shape::Unary, Modifier::PropagatesNulls));
        assert!(!has(Shape::Unary, Modifier::IsAssociative));
    }
}
