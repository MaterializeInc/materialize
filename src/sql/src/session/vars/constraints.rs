// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines constraints that can be imposed on variables.

use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::RangeBounds;

use mz_repr::adt::numeric::Numeric;

use super::{Value, Var, VarError};

#[derive(Debug)]
pub enum ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    /// Variable is read-only and cannot be updated.
    ReadOnly,
    /// The variables value can be updated, but only to a fixed value.
    Fixed,
    // Arbitrary constraints over values.
    Domain(&'static dyn DomainConstraint<V>),
}

impl<V> ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    pub fn check_constraint(
        &self,
        var: &(dyn Var + Send + Sync),
        cur_value: &V,
        new_value: &V::Owned,
    ) -> Result<(), VarError> {
        match self {
            ValueConstraint::ReadOnly => return Err(VarError::ReadOnlyParameter(var.name())),
            ValueConstraint::Fixed => {
                if cur_value != new_value.borrow() {
                    return Err(VarError::FixedValueParameter(var.into()));
                }
            }
            ValueConstraint::Domain(check) => check.check(var, new_value)?,
        }

        Ok(())
    }
}

impl<V> Clone for ValueConstraint<V>
where
    V: Value + ToOwned + Debug + PartialEq + 'static,
{
    fn clone(&self) -> Self {
        match self {
            ValueConstraint::Fixed => ValueConstraint::Fixed,
            ValueConstraint::ReadOnly => ValueConstraint::ReadOnly,
            ValueConstraint::Domain(c) => ValueConstraint::Domain(*c),
        }
    }
}

pub trait DomainConstraint<V>: Debug + Send + Sync
where
    V: Value + Debug + PartialEq + 'static,
{
    // `self` is make a trait object
    fn check(&self, var: &(dyn Var + Send + Sync), v: &V::Owned) -> Result<(), VarError>;
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NumericNonNegNonNan;

impl DomainConstraint<Numeric> for NumericNonNegNonNan {
    fn check(&self, var: &(dyn Var + Send + Sync), n: &Numeric) -> Result<(), VarError> {
        if n.is_nan() || n.is_negative() {
            Err(VarError::InvalidParameterValue {
                parameter: var.into(),
                values: vec![n.to_string()],
                reason: "only supports non-negative, non-NaN numeric values".to_string(),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NumericInRange<R>(pub R);

impl<R> DomainConstraint<Numeric> for NumericInRange<R>
where
    R: RangeBounds<f64> + std::fmt::Debug + Send + Sync,
{
    fn check(&self, var: &(dyn Var + Send + Sync), n: &Numeric) -> Result<(), VarError> {
        let n: f64 = (*n)
            .try_into()
            .map_err(|_e| VarError::InvalidParameterValue {
                parameter: var.into(),
                values: vec![n.to_string()],
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
                parameter: var.into(),
                values: vec![n.to_string()],
                reason: format!("only supports values in range {:?}", self.0),
            })
        } else {
            Ok(())
        }
    }
}
