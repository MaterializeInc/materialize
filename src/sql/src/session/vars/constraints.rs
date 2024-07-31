// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines constraints that can be imposed on variables.

use std::fmt::Debug;
use std::ops::{RangeBounds, RangeFrom, RangeInclusive};

use mz_repr::adt::numeric::Numeric;
use mz_repr::bytes::ByteSize;

use super::{Value, Var, VarError};

pub static NUMERIC_NON_NEGATIVE: NumericNonNegNonNan = NumericNonNegNonNan;

pub static NUMERIC_BOUNDED_0_1_INCLUSIVE: NumericInRange<RangeInclusive<f64>> =
    NumericInRange(0.0f64..=1.0);

pub static BYTESIZE_AT_LEAST_1MB: ByteSizeInRange<RangeFrom<ByteSize>> =
    ByteSizeInRange(ByteSize::mb(1)..);

#[derive(Debug)]
pub enum ValueConstraint {
    /// Variable is read-only and cannot be updated.
    ReadOnly,
    /// The variables value can be updated, but only to a fixed value.
    Fixed,
    // Arbitrary constraints over values.
    Domain(&'static dyn DynDomainConstraint),
}

impl ValueConstraint {
    pub fn check_constraint(
        &self,
        var: &dyn Var,
        cur_value: &dyn Value,
        new_value: &dyn Value,
    ) -> Result<(), VarError> {
        match self {
            ValueConstraint::ReadOnly => return Err(VarError::ReadOnlyParameter(var.name())),
            ValueConstraint::Fixed => {
                if cur_value != new_value {
                    return Err(VarError::FixedValueParameter {
                        name: var.name(),
                        value: cur_value.format(),
                    });
                }
            }
            ValueConstraint::Domain(check) => check.check(var, new_value)?,
        }

        Ok(())
    }
}

impl Clone for ValueConstraint {
    fn clone(&self) -> Self {
        match self {
            ValueConstraint::Fixed => ValueConstraint::Fixed,
            ValueConstraint::ReadOnly => ValueConstraint::ReadOnly,
            ValueConstraint::Domain(c) => ValueConstraint::Domain(*c),
        }
    }
}

/// A type erased version of [`DomainConstraint`] that we can reference on a [`VarDefinition`].
///
/// [`VarDefinition`]: crate::session::vars::definitions::VarDefinition
pub trait DynDomainConstraint: Debug + Send + Sync + 'static {
    fn check(&self, var: &dyn Var, v: &dyn Value) -> Result<(), VarError>;
}

impl<D> DynDomainConstraint for D
where
    D: DomainConstraint + Send + Sync + 'static,
    D::Value: Value,
{
    fn check(&self, var: &dyn Var, v: &dyn Value) -> Result<(), VarError> {
        let val = v
            .as_any()
            .downcast_ref::<D::Value>()
            .expect("type should match");
        self.check(var, val)
    }
}
pub trait DomainConstraint: Debug + Send + Sync + 'static {
    type Value;

    fn check(&self, var: &dyn Var, v: &Self::Value) -> Result<(), VarError>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NumericNonNegNonNan;

impl DomainConstraint for NumericNonNegNonNan {
    type Value = Numeric;

    fn check(&self, var: &dyn Var, n: &Numeric) -> Result<(), VarError> {
        if n.is_nan() || n.is_negative() {
            Err(VarError::InvalidParameterValue {
                name: var.name(),
                invalid_values: vec![n.to_string()],
                reason: "only supports non-negative, non-NaN numeric values".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NumericInRange<R>(pub R);

impl<R> DomainConstraint for NumericInRange<R>
where
    R: RangeBounds<f64> + std::fmt::Debug + Send + Sync + 'static,
{
    type Value = Numeric;

    fn check(&self, var: &dyn Var, n: &Numeric) -> Result<(), VarError> {
        let n: f64 = (*n)
            .try_into()
            .map_err(|_e| VarError::InvalidParameterValue {
                name: var.name(),
                invalid_values: vec![n.to_string()],
                // This first check can fail if the value is NaN, out of range,
                // OR if it underflows (i.e. is very close to 0 without actually being 0, and the closest
                // representable float is 0).
                //
                // The underflow case is very unlikely to be accidentally hit by a user, so let's
                // not make the error message more confusing by talking about it, even though that makes
                // the error message slightly inaccurate.
                //
                // If the user tries to set the paramater to 0.000<hundreds more zeros>001
                // and gets the message "only supports values in range [0.0..=1.0]", I think they will
                // understand, or at least accept, what's going on.
                reason: format!("only supports values in range {:?}", self.0),
            })?;
        if !self.0.contains(&n) {
            Err(VarError::InvalidParameterValue {
                name: var.name(),
                invalid_values: vec![n.to_string()],
                reason: format!("only supports values in range {:?}", self.0),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ByteSizeInRange<R>(pub R);

impl<R> DomainConstraint for ByteSizeInRange<R>
where
    R: RangeBounds<ByteSize> + std::fmt::Debug + Send + Sync + 'static,
{
    type Value = ByteSize;

    fn check(&self, var: &dyn Var, size: &ByteSize) -> Result<(), VarError> {
        if self.0.contains(size) {
            Ok(())
        } else {
            Err(VarError::InvalidParameterValue {
                name: var.name(),
                invalid_values: vec![size.to_string()],
                reason: format!("only supports values in range {:?}", self.0),
            })
        }
    }
}
