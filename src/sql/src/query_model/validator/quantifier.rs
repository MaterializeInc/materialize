// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines [`QuantifierConstraint`] and [`QuantifierConstraintValidator`].

use super::{ValidationError, ValidationResult, Validator};
use crate::query_model::*;
use std::ops::{Bound, RangeBounds};

/// A model for constraints imposed on the input or ranging quantifiers of a query box.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QuantifierConstraint {
    /// Lower bound for the number of allowed quantifiers that match the `allowed_types` bitmask.
    min: Bound<usize>,
    /// Upper bound for the number of allowed quantifiers that match the `allowed_types` bitmask.
    max: Bound<usize>,
    /// A bitmask of allowed [`QuantifierType`] variant discriminants.
    allowed_types: usize,
    /// Indicates whether the input of a quantifier with an `allowed_type` must be a select box.
    select_input: bool,
    /// Indicates whether the parent of a quantifier with an `allowed_type` must be a select box.
    select_parent: bool,
}

impl QuantifierConstraint {
    /// Check if the given [`Quantifier`] satisfy the `allowed_types` of the this constraint.
    fn satisfies_q_type<'a>(&self, quantifier: &'a Quantifier) -> bool {
        (quantifier.quantifier_type as usize) & self.allowed_types != 0
    }
    /// Check if the boxes referenced by the given [`Quantifier`] satisfy the `input_is_select`
    /// and `parent_is_select` of the this constraint.
    fn satisfies_b_type<'a>(&self, quantifier: &'a Quantifier, model: &'a Model) -> bool {
        let input_select_satisfied =
            !self.select_input || model.get_box(quantifier.input_box).is_select();
        let parent_select_satisfied =
            !self.select_parent || model.get_box(quantifier.parent_box).is_select();
        input_select_satisfied && parent_select_satisfied
    }
}

impl RangeBounds<usize> for QuantifierConstraint {
    fn start_bound(&self) -> Bound<&usize> {
        match self.min {
            Bound::Included(ref value) => Bound::Included(value),
            Bound::Excluded(ref value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&usize> {
        match self.max {
            Bound::Included(ref value) => Bound::Included(value),
            Bound::Excluded(ref value) => Bound::Excluded(value),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

/// Check a const-sized array of `constraints` against a sequence of `quantifiers`,
/// and call `on_failure` for constraints that are not satisfied by the sequence.
///
/// A `constraint[i]` is violated iff one of the following conditions are met:
/// - The number of `quantifiers` with satisfied type is within the given range.
/// - All `quantifiers` with satisfied type also satisfy the quantifier box constraints.
fn check_constraints<'a, const N: usize>(
    constraints: &[QuantifierConstraint; N],
    quantifiers: impl Iterator<Item = Ref<'a, Quantifier>>,
    model: &'a Model,
    mut on_failure: impl FnMut(&QuantifierConstraint) -> (),
) {
    // count quantifiers with satisfied type and associated box types
    let mut q_type_counts = [0; N];
    let mut b_type_counts = [0; N];
    for q in quantifiers {
        for i in 0..N {
            if constraints[i].satisfies_q_type(&q) {
                q_type_counts[i] += 1;
                b_type_counts[i] += constraints[i].satisfies_b_type(&q, model) as usize;
            }
        }
    }
    // call on_failure for violated constraints
    for i in 0..N {
        let q_type_count_ok = constraints[i].contains(&q_type_counts[i]);
        let b_type_count_ok = q_type_counts[i] == b_type_counts[i];
        if !(q_type_count_ok && b_type_count_ok) {
            on_failure(&constraints[i])
        }
    }
}

/// [`QuantifierConstraint`] constants used by the [`QuantifierConstraintValidator`].
mod constants {
    use super::*;
}

/// A [`Validator`] that checks the following quantifier constraints:
/// - `Get`, `TableFunction` and `Values` boxes cannot have input
///    quantifiers.
#[derive(Default)]
pub(crate) struct QuantifierConstraintValidator;

impl Validator for QuantifierConstraintValidator {
    fn validate(&self, model: &Model) -> ValidationResult {
        let mut errors = vec![];

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::constants::*;
    use super::*;

    fn get_user_id(id: u64) -> Get {
        Get {
            id: expr::GlobalId::User(id),
        }
    }
}
