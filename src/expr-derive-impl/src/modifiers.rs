// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//! The `#[sqlfunc]` attribute's key-value modifiers.
//!
//! [`Modifiers`] is the parsed attribute and [`reject_inapplicable`] gates it against
//! the per-arity tables in [`crate::shape`]. Modifiers that feed `Display` or the
//! output-type body are read by name, and the ones that map onto a trait method are
//! reached through [`Modifiers::iter`].

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, Lit};

use crate::shape::{Modifier, Shape};

/// Modifiers passed as key-value pairs to the `#[sqlfunc]` macro.
///
/// Which arities accept which modifier is declared by the tables in [`crate::shape`],
/// not here. [`reject_inapplicable`] enforces it.
#[derive(Debug, Default, darling::FromMeta)]
pub(crate) struct Modifiers {
    /// Whether the function is monotone with respect to its arguments. Binary functions
    /// take a pair of expressions, one per argument.
    is_monotone: Option<Expr>,
    /// Whether `is_monotone`'s endpoint-sampling guarantee still holds when an operand
    /// may be infinite. Set `false` for multiplication and division.
    is_infinity_monotone: Option<Expr>,
    /// The SQL name for the function.
    pub(crate) sqlname: Option<SqlName>,
    /// Whether the function preserves uniqueness.
    preserves_uniqueness: Option<Expr>,
    /// The inverse of the function, if it exists.
    inverse: Option<Expr>,
    /// The negated function, if it exists.
    negate: Option<Expr>,
    /// Whether the function is an infix operator.
    is_infix_op: Option<Expr>,
    /// The output type of the function, if it cannot be inferred.
    pub(crate) output_type: Option<syn::Path>,
    /// The output type of the function as an expression.
    pub(crate) output_type_expr: Option<Expr>,
    /// Whether the function could error.
    could_error: Option<Expr>,
    /// Whether the function propagates nulls.
    propagates_nulls: Option<Expr>,
    /// Whether the function introduces nulls.
    pub(crate) introduces_nulls: Option<Expr>,
    /// Whether the function is associative.
    is_associative: Option<Expr>,
    /// Whether the function is a noop cast.
    is_eliminable_cast: Option<Expr>,
    /// Whether to generate a snapshot test for the function. Defaults to false.
    test: Option<bool>,
}

impl Modifiers {
    /// Whether the call site asked for a generated snapshot test.
    pub(crate) fn generates_test(&self) -> bool {
        self.test.unwrap_or(false)
    }

    /// The method-producing modifiers that are present, in this method's own fixed
    /// order, which does not match any of the three per-arity tables in
    /// `crate::shape`. A caller that needs table order joins against
    /// `Shape::modifiers()` by `Modifier` instead.
    ///
    /// Modifiers that do not produce a trait method are excluded, because
    /// `crate::generate::generate` consumes those by name.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (Modifier, &Expr)> + '_ {
        [
            (Modifier::CouldError, self.could_error.as_ref()),
            (Modifier::IntroducesNulls, self.introduces_nulls.as_ref()),
            (Modifier::IsMonotone, self.is_monotone.as_ref()),
            (Modifier::IsInfixOp, self.is_infix_op.as_ref()),
            (Modifier::PropagatesNulls, self.propagates_nulls.as_ref()),
            (Modifier::Inverse, self.inverse.as_ref()),
            (
                Modifier::PreservesUniqueness,
                self.preserves_uniqueness.as_ref(),
            ),
            (Modifier::IsEliminableCast, self.is_eliminable_cast.as_ref()),
            (Modifier::Negate, self.negate.as_ref()),
            (
                Modifier::IsInfinityMonotone,
                self.is_infinity_monotone.as_ref(),
            ),
            (Modifier::IsAssociative, self.is_associative.as_ref()),
        ]
        .into_iter()
        .filter_map(|(modifier, expr)| expr.map(|expr| (modifier, expr)))
    }
}

#[cfg(test)]
impl Modifiers {
    /// Parses modifiers from attribute tokens. Test helper for `crate::generate`.
    pub(crate) fn from_tokens(tokens: TokenStream) -> darling::Result<Self> {
        let args = darling::ast::NestedMeta::parse_meta_list(tokens)?;
        <Self as darling::FromMeta>::from_list(&args)
    }
}

/// Errors if `mods` carries a method-producing modifier `shape` does not accept.
pub(crate) fn reject_inapplicable(shape: Shape, mods: &Modifiers) -> darling::Result<()> {
    for (modifier, _) in mods.iter() {
        let accepted = shape
            .modifiers()
            .iter()
            .any(|(candidate, _)| *candidate == modifier);
        if !accepted {
            return Err(darling::Error::custom(format!(
                "`{}` is not supported for {} functions",
                modifier.name(),
                shape.label(),
            )));
        }
    }
    Ok(())
}

/// A name for the SQL function. It can be either a literal or a macro, thus we
/// can't use `String` or `syn::Expr` directly.
#[derive(Debug)]
pub(crate) enum SqlName {
    /// A literal string.
    Literal(syn::Lit),
    /// A macro expression.
    Macro(syn::ExprMacro),
}

impl quote::ToTokens for SqlName {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let name = match self {
            SqlName::Literal(lit) => quote! { #lit },
            SqlName::Macro(mac) => quote! { #mac },
        };
        tokens.extend(name);
    }
}

impl darling::FromMeta for SqlName {
    fn from_value(value: &Lit) -> darling::Result<Self> {
        Ok(Self::Literal(value.clone()))
    }
    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Lit(lit) => Self::from_value(&lit.lit),
            Expr::Macro(mac) => Ok(Self::Macro(mac.clone())),
            // Syn sometimes inserts groups, see `FromMeta::from_expr` for
            // details.
            Expr::Group(mac) => Self::from_expr(&mac.expr),
            _ => Err(darling::Error::unexpected_expr_type(expr)),
        }
    }
}
