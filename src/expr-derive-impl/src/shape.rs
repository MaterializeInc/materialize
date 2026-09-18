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

use proc_macro2::TokenStream;
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
/// `output_type_expr`, and `test`, are absent: `crate::sqlfunc`'s generator arms
/// handle those explicitly because they feed `Display`, the output-type body, or
/// the expansion decision instead.
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
}

#[cfg(test)]
mod tests {
    use super::{Modifier, ReturnTy, Shape};

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

    #[mz_ore::test]
    fn binary_accepts_negate_and_infinity_monotone() {
        assert!(has(Shape::Binary, Modifier::Negate));
        assert!(has(Shape::Binary, Modifier::IsInfinityMonotone));
        assert!(has(Shape::Binary, Modifier::IsInfixOp));
        assert!(!has(Shape::Binary, Modifier::Inverse));
        assert!(!has(Shape::Binary, Modifier::IsAssociative));
    }

    #[mz_ore::test]
    fn variadic_accepts_associative_only_among_the_exclusives() {
        assert!(has(Shape::Variadic, Modifier::IsAssociative));
        assert!(has(Shape::Variadic, Modifier::IsInfixOp));
        assert!(!has(Shape::Variadic, Modifier::Negate));
        assert!(!has(Shape::Variadic, Modifier::Inverse));
        assert!(!has(Shape::Variadic, Modifier::IsInfinityMonotone));
    }

    #[mz_ore::test]
    fn could_error_introduces_nulls_and_is_monotone_are_universal() {
        for shape in [Shape::Unary, Shape::Binary, Shape::Variadic] {
            assert!(has(shape, Modifier::CouldError), "{}", shape.label());
            assert!(has(shape, Modifier::IntroducesNulls), "{}", shape.label());
            assert!(has(shape, Modifier::IsMonotone), "{}", shape.label());
        }
    }

    #[mz_ore::test]
    fn is_monotone_returns_a_pair_only_for_binary() {
        let ret = |shape: Shape| {
            shape
                .modifiers()
                .iter()
                .find(|(m, _)| *m == Modifier::IsMonotone)
                .map(|(_, r)| *r)
                .expect("is_monotone is universal")
        };
        assert_eq!(ret(Shape::Unary), ReturnTy::Bool);
        assert_eq!(ret(Shape::Binary), ReturnTy::BoolPair);
        assert_eq!(ret(Shape::Variadic), ReturnTy::Bool);
    }

    #[mz_ore::test]
    fn modifier_table_order_matches_todays_generated_code() {
        // Order is load-bearing: `crate::generate::override_methods` walks these
        // tables to generate each arity's override methods in this order. The insta
        // snapshots under `sqlfunc.rs` require that generated code to stay
        // byte-identical, so this test pins the sequence they depend on.
        let names = |shape: Shape| -> Vec<&'static str> {
            shape.modifiers().iter().map(|(m, _)| m.name()).collect()
        };
        assert_eq!(
            names(Shape::Unary),
            vec![
                "could_error",
                "introduces_nulls",
                "inverse",
                "is_monotone",
                "preserves_uniqueness",
                "is_eliminable_cast",
            ]
        );
        assert_eq!(
            names(Shape::Binary),
            vec![
                "could_error",
                "introduces_nulls",
                "is_infix_op",
                "is_monotone",
                "is_infinity_monotone",
                "negate",
                "propagates_nulls",
            ]
        );
        assert_eq!(
            names(Shape::Variadic),
            vec![
                "could_error",
                "introduces_nulls",
                "is_infix_op",
                "is_monotone",
                "is_associative",
                "propagates_nulls",
            ]
        );
    }
}
