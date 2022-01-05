// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for validating query graph models.
//!
//! The public interface consists of the following items:
//! - the [`Model::validate`] method,
//! - the [`ValidationResult`] type alias,
//! - the [`ValidationError`] type.

mod quantifier;

use crate::query_model::*;
use quantifier::*;

impl Model {
    /// Validate a model with a default validator chain.
    pub fn validate(&self) -> ValidationResult {
        let chain = ValidatorChain { validators: vec![] };
        chain.validate(self)
    }
}

/// The [`Result`] type returned by [`Model::validate`].
pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// An enum consisting of variants corresponding to the different types
/// of constraint violations that can be detected as part of model
/// validation.
#[derive(Debug, Eq, Hash, PartialEq)]
pub enum ValidationError {
    InvalidEnforcedDistinct(BoxId, DistinctOperation),
    InvalidInputQuantifiers(BoxId, QuantifierConstraint),
    InvalidRangingQuantifiers(BoxId, QuantifierConstraint),
}

/// Consumes and merges two `ValidationResult` instances, reusing memory
/// if possible.
///
/// - If both results are `Ok(())` variants, the left one is returned.
/// - If one result is `Err(errors)` and the other is `Ok(())`, the `Err`
///   is returned.
/// - If both results are `Err(errors)` variants, a new `Err` with the
///   concatenated `errors` is returned.
fn merge(lhs: ValidationResult, rhs: ValidationResult) -> ValidationResult {
    match (lhs, rhs) {
        (lhs @ Ok(..), Ok(..)) => lhs,
        (Ok(..), rhs @ Err(..)) => rhs,
        (lhs @ Err(..), Ok(..)) => lhs,
        (Err(mut lhs), Err(rhs)) => {
            lhs.extend(rhs);
            Err(lhs)
        }
    }
}

/// A common trait shared by all validators.
trait Validator {
    /// Perform the validation logic of this [`Validator`] instance
    /// against the given query graph model.
    ///
    /// Return a [`Result`] of type `()` if the model is valid, or
    /// a `Vec<ValidationError>` consisting of validation errors
    /// discovered by this validator.
    fn validate(&self, model: &Model) -> ValidationResult;
}

/// A composite [`Validator`] that applies a chain of child [`Validator`] instances
/// and concatenates their reported [`ValidationError`] vectors.
///
/// Returns [`Ok`] iff all child validators return [`Ok`].
struct ValidatorChain {
    validators: Vec<Box<dyn Validator>>,
}

impl Validator for ValidatorChain {
    fn validate(&self, model: &Model) -> Result<(), Vec<validator::ValidationError>> {
        let mut result = Ok(());

        for validator in &self.validators {
            result = merge(result, validator.validate(model))
        }

        result
    }
}
