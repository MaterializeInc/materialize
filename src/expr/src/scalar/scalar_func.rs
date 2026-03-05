// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unified scalar function traits.
//!
//! [`LazyScalarFunc`] and [`EagerScalarFunc`] provide a single trait interface
//! for all scalar functions, regardless of arity. Functions that previously
//! implemented `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc`
//! can be migrated to implement [`EagerScalarFunc`] instead.
//!
//! A blanket implementation bridges [`EagerScalarFunc`] to [`LazyScalarFunc`],
//! handling expression evaluation and null propagation automatically.

use std::fmt;

use mz_repr::{
    Datum, InputDatumType, OutputDatumType, ReprColumnType, RowArena, SqlColumnType,
};

use crate::{EvalError, MirScalarExpr};

/// A scalar SQL function that lazily evaluates its arguments.
///
/// This is the unified replacement for `LazyUnaryFunc`, `LazyBinaryFunc`, and
/// `LazyVariadicFunc`. All scalar functions should eventually implement this
/// trait (either directly for lazy functions, or indirectly via
/// [`EagerScalarFunc`]).
pub trait LazyScalarFunc: fmt::Display {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError>;

    /// The output [`SqlColumnType`] of this function given the input types.
    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType;

    /// The output [`ReprColumnType`] of this function given the input types.
    fn output_type(&self, input_types: &[ReprColumnType]) -> ReprColumnType {
        ReprColumnType::from(
            &self.output_sql_type(
                &input_types
                    .iter()
                    .map(SqlColumnType::from_repr)
                    .collect::<Vec<_>>(),
            ),
        )
    }

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool;

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool;

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        true
    }

    /// Whether the function is monotone with respect to the argument at the
    /// given index.
    ///
    /// Monotone functions map ranges to ranges: given a range of possible
    /// inputs, we can determine the range of possible outputs just by mapping
    /// the endpoints.
    ///
    /// For multi-argument functions, this describes *pointwise* behaviour:
    /// the behaviour of any specific argument as the others are held constant.
    fn arg_is_monotone(&self, _index: usize) -> bool {
        false
    }

    /// Whether this function preserves uniqueness.
    ///
    /// Uniqueness is preserved when `if f(x) = f(y) then x = y` is true.
    /// Only meaningful for unary functions.
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    /// The inverse of this function, if it exists. Only meaningful for unary
    /// functions.
    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    /// The negation of this function, if it exists. Only meaningful for binary
    /// functions.
    fn negate(&self) -> Option<crate::BinaryFunc> {
        None
    }

    /// Whether this function is an infix operator.
    fn is_infix_op(&self) -> bool {
        false
    }

    /// Whether this function is associative. Only meaningful for variadic
    /// functions.
    fn is_associative(&self) -> bool {
        false
    }
}

/// A scalar SQL function that eagerly evaluates its arguments.
///
/// This is the unified replacement for `EagerUnaryFunc`, `EagerBinaryFunc`, and
/// `EagerVariadicFunc`. The `Input` and `Output` associated types drive
/// automatic null propagation and error handling through [`InputDatumType`] and
/// [`OutputDatumType`].
///
/// A blanket implementation of [`LazyScalarFunc`] is provided for all types
/// that implement this trait.
pub trait EagerScalarFunc: fmt::Display {
    type Input<'a>: InputDatumType<'a, EvalError>;
    type Output<'a>: OutputDatumType<'a, EvalError>;

    fn call<'a>(&self, input: Self::Input<'a>, temp_storage: &'a RowArena) -> Self::Output<'a>;

    /// The output [`SqlColumnType`] of this function given the input types.
    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType;

    /// Whether this function will produce NULL on NULL input.
    fn propagates_nulls(&self) -> bool {
        !Self::Input::<'_>::nullable()
    }

    /// Whether this function will produce NULL on non-NULL input.
    fn introduces_nulls(&self) -> bool {
        Self::Output::<'_>::nullable()
    }

    /// Whether this function might error on non-error input.
    fn could_error(&self) -> bool {
        Self::Output::<'_>::fallible()
    }

    /// Whether the function is monotone with respect to the argument at the
    /// given index.
    fn arg_is_monotone(&self, _index: usize) -> bool {
        false
    }

    /// Whether this function preserves uniqueness (unary only).
    fn preserves_uniqueness(&self) -> bool {
        false
    }

    /// The inverse of this function (unary only).
    fn inverse(&self) -> Option<crate::UnaryFunc> {
        None
    }

    /// The negation of this function (binary only).
    fn negate(&self) -> Option<crate::BinaryFunc> {
        None
    }

    /// Whether this function is an infix operator.
    fn is_infix_op(&self) -> bool {
        false
    }

    /// Whether this function is associative (variadic only).
    fn is_associative(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Adapter impls: bridge the existing arity-specific enums to LazyScalarFunc
// ---------------------------------------------------------------------------

impl LazyScalarFunc for crate::UnaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        self.eval(datums, temp_storage, &exprs[0])
    }

    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        self.output_sql_type(input_types[0].clone())
    }

    fn output_type(&self, input_types: &[ReprColumnType]) -> ReprColumnType {
        self.output_type(input_types[0].clone())
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn arg_is_monotone(&self, _index: usize) -> bool {
        self.is_monotone()
    }

    fn preserves_uniqueness(&self) -> bool {
        self.preserves_uniqueness()
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        self.inverse()
    }
}

impl LazyScalarFunc for crate::BinaryFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        self.eval(datums, temp_storage, &[&exprs[0], &exprs[1]])
    }

    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        self.output_sql_type(input_types)
    }

    fn output_type(&self, input_types: &[ReprColumnType]) -> ReprColumnType {
        self.output_type(input_types)
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn arg_is_monotone(&self, index: usize) -> bool {
        let (a, b) = self.is_monotone();
        match index {
            0 => a,
            1 => b,
            _ => false,
        }
    }

    fn negate(&self) -> Option<crate::BinaryFunc> {
        self.negate()
    }

    fn is_infix_op(&self) -> bool {
        self.is_infix_op()
    }
}

impl LazyScalarFunc for crate::VariadicFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        self.eval(datums, temp_storage, exprs)
    }

    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        self.output_sql_type(input_types.to_vec())
    }

    fn output_type(&self, input_types: &[ReprColumnType]) -> ReprColumnType {
        self.output_type(input_types.to_vec())
    }

    fn propagates_nulls(&self) -> bool {
        self.propagates_nulls()
    }

    fn introduces_nulls(&self) -> bool {
        self.introduces_nulls()
    }

    fn could_error(&self) -> bool {
        self.could_error()
    }

    fn arg_is_monotone(&self, _index: usize) -> bool {
        self.is_monotone()
    }

    fn is_infix_op(&self) -> bool {
        self.is_infix_op()
    }

    fn is_associative(&self) -> bool {
        self.is_associative()
    }
}

/// Blanket [`LazyScalarFunc`] implementation for all [`EagerScalarFunc`] types.
///
/// Evaluates argument expressions, converts them to the expected input type
/// via [`InputDatumType::try_from_iter`], calls the function, and converts
/// the output via [`OutputDatumType::into_result`].
impl<T: EagerScalarFunc> LazyScalarFunc for T {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [MirScalarExpr],
    ) -> Result<Datum<'a>, EvalError> {
        let mut results = exprs.iter().map(|e| e.eval(datums, temp_storage));
        match T::Input::try_from_iter(&mut results) {
            Ok(input) => self.call(input, temp_storage).into_result(temp_storage),
            Err(Ok(None)) => Err(EvalError::Internal("missing parameter".into())),
            Err(Ok(Some(datum))) if datum.is_null() => Ok(datum),
            Err(Ok(Some(_))) => Err(EvalError::Internal("invalid input type".into())),
            Err(Err(err)) => Err(err),
        }
    }

    fn output_sql_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
        EagerScalarFunc::output_sql_type(self, input_types)
    }

    fn propagates_nulls(&self) -> bool {
        EagerScalarFunc::propagates_nulls(self)
    }

    fn introduces_nulls(&self) -> bool {
        EagerScalarFunc::introduces_nulls(self)
    }

    fn could_error(&self) -> bool {
        EagerScalarFunc::could_error(self)
    }

    fn arg_is_monotone(&self, index: usize) -> bool {
        EagerScalarFunc::arg_is_monotone(self, index)
    }

    fn preserves_uniqueness(&self) -> bool {
        EagerScalarFunc::preserves_uniqueness(self)
    }

    fn inverse(&self) -> Option<crate::UnaryFunc> {
        EagerScalarFunc::inverse(self)
    }

    fn negate(&self) -> Option<crate::BinaryFunc> {
        EagerScalarFunc::negate(self)
    }

    fn is_infix_op(&self) -> bool {
        EagerScalarFunc::is_infix_op(self)
    }

    fn is_associative(&self) -> bool {
        EagerScalarFunc::is_associative(self)
    }
}
