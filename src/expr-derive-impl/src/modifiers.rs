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
//! [`Modifiers`] is the parsed attribute. [`Modifiers::validate`] checks it before
//! anything is generated. Modifiers that feed `Display` or the output-type body have
//! their own accessors, and the ones that map onto a trait method are reached through
//! [`Modifiers::get`].

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Expr, Lit};

use crate::shape::{Modifier, Shape};

/// Modifiers passed as key-value pairs to the `#[sqlfunc]` macro.
#[derive(Debug, Default, darling::FromMeta)]
pub(crate) struct Modifiers {
    /// Whether the function is monotone with respect to its arguments. Binary functions
    /// take a pair of expressions, one per argument.
    is_monotone: Option<Expr>,
    /// Whether `is_monotone`'s endpoint-sampling guarantee still holds when an operand
    /// may be infinite. Set `false` for multiplication and division.
    is_infinity_monotone: Option<Expr>,
    /// The SQL name for the function.
    sqlname: Option<SqlName>,
    /// Whether the function preserves uniqueness.
    preserves_uniqueness: Option<Expr>,
    /// The inverse of the function, if it exists.
    inverse: Option<Expr>,
    /// The negated function, if it exists.
    negate: Option<Expr>,
    /// Whether the function is an infix operator.
    is_infix_op: Option<Expr>,
    /// The output type of the function, if it cannot be inferred.
    output_type: Option<syn::Path>,
    /// The output type of the function as an expression.
    output_type_expr: Option<Expr>,
    /// Whether the function could error.
    could_error: Option<Expr>,
    /// Whether the function propagates nulls.
    propagates_nulls: Option<Expr>,
    /// Whether the function introduces nulls.
    introduces_nulls: Option<Expr>,
    /// Whether the function is associative.
    is_associative: Option<Expr>,
    /// Whether the function is a noop cast.
    is_eliminable_cast: Option<Expr>,
    /// Whether to generate a snapshot test for the function. Defaults to false.
    test: Option<bool>,
    /// Suppresses the generated `fmt::Display`. Set this when the struct's name
    /// depends on its state, so the call site keeps a hand-written impl.
    skip_display: Option<bool>,
}

impl Modifiers {
    /// Whether the call site asked for a generated snapshot test.
    pub(crate) fn generates_test(&self) -> bool {
        self.test.unwrap_or(false)
    }

    /// The SQL name the generated `Display` writes, if the call site overrides the
    /// function's name.
    pub(crate) fn sqlname(&self) -> Option<&SqlName> {
        self.sqlname.as_ref()
    }

    /// The type whose column type is the function's output type, if the call site
    /// overrides the one derived from the return type.
    pub(crate) fn output_type(&self) -> Option<&syn::Path> {
        self.output_type.as_ref()
    }

    /// An expression computing the function's output column type, if the call site
    /// overrides the one derived from the return type.
    pub(crate) fn output_type_expr(&self) -> Option<&Expr> {
        self.output_type_expr.as_ref()
    }

    /// Whether the call site asked to suppress the generated `Display` impl.
    pub(crate) fn skip_display(&self) -> bool {
        self.skip_display.unwrap_or(false)
    }

    /// The expression the call site gave a method-producing modifier, if any.
    pub(crate) fn get(&self, modifier: Modifier) -> Option<&Expr> {
        match modifier {
            Modifier::CouldError => self.could_error.as_ref(),
            Modifier::IntroducesNulls => self.introduces_nulls.as_ref(),
            Modifier::IsMonotone => self.is_monotone.as_ref(),
            Modifier::IsInfixOp => self.is_infix_op.as_ref(),
            Modifier::PropagatesNulls => self.propagates_nulls.as_ref(),
            Modifier::Inverse => self.inverse.as_ref(),
            Modifier::PreservesUniqueness => self.preserves_uniqueness.as_ref(),
            Modifier::IsEliminableCast => self.is_eliminable_cast.as_ref(),
            Modifier::Negate => self.negate.as_ref(),
            Modifier::IsInfinityMonotone => self.is_infinity_monotone.as_ref(),
            Modifier::IsAssociative => self.is_associative.as_ref(),
        }
    }

    /// Errors if a modifier does not apply to `shape`, or conflicts with another
    /// modifier or with the function's receiver. Reports every problem, each at the
    /// modifier it concerns.
    pub(crate) fn validate(&self, shape: Shape, has_self: bool) -> darling::Result<()> {
        let mut errors = darling::Error::accumulator();
        for modifier in Modifier::ALL {
            let Some(expr) = self.get(modifier) else {
                continue;
            };
            if !shape.accepts(modifier) {
                errors.push(
                    darling::Error::custom(format!(
                        "`{}` is not supported for {} functions",
                        modifier.name(),
                        shape.label(),
                    ))
                    .with_span(expr),
                );
            }
        }
        if let (Some(_), Some(expr)) = (&self.output_type, &self.output_type_expr) {
            errors.push(
                darling::Error::custom("output_type and output_type_expr cannot be used together")
                    .with_span(expr),
            );
        }
        if self.skip_display() {
            if let Some(sqlname) = &self.sqlname {
                errors.push(
                    darling::Error::custom(
                        "sqlname has no effect with skip_display, which suppresses the only impl that reads it",
                    )
                    .with_span(sqlname),
                );
            }
            // A hand-written `Display` is only needed when the name depends on struct
            // state, and without `&self` the macro defines a unit struct that has none.
            if !has_self {
                errors.push(darling::Error::custom(
                    "skip_display requires a &self receiver, since a unit struct has no state for a hand-written Display to read",
                ));
            }
        }
        errors.finish()
    }
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
