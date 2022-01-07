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
/// A `constraint[i]` is violoated iff one of the following conditions are met:
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

    pub(crate) const ZERO_ARBITRARY: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(0),
        allowed_types: ARBITRARY_QUANTIFIER,
        select_input: false,
        select_parent: false,
    };

    pub(crate) const ZERO_NOT_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(0),
        allowed_types: ARBITRARY_QUANTIFIER & !(QuantifierType::Foreach as usize),
        select_input: false,
        select_parent: false,
    };

    pub(crate) const ONE_OR_MORE_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(1),
        max: Bound::Unbounded,
        allowed_types: QuantifierType::Foreach as usize,
        select_input: false,
        select_parent: false,
    };
}

/// A [`Validator`] that checks the following quantifier constraints:
/// - `Get`, `TableFunction` and `Values` boxes cannot have input
///    quantifiers.
/// - `Union`, `Except` and `Intersect` boxes can only have input
///    quantifiers of type `Foreach`.
#[derive(Default)]
pub(crate) struct QuantifierConstraintValidator;

impl Validator for QuantifierConstraintValidator {
    fn validate(&self, model: &Model) -> ValidationResult {
        let mut errors = vec![];

        for b in model.boxes.values().map(|b| b.borrow()) {
            use constants::*;
            use BoxType::*;
            match b.box_type {
                Get(..) | TableFunction(..) | Values(..) => {
                    let constraints = [ZERO_ARBITRARY];
                    check_constraints(&constraints, b.input_quantifiers(model), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    })
                }
                Union | Except | Intersect => {
                    let constraints = [ONE_OR_MORE_FOREACH, ZERO_NOT_FOREACH];
                    check_constraints(&constraints, b.input_quantifiers(model), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    })
                }
                _ => { /* everything is OK with the current box */ }
            }
        }

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

    #[test]
    fn test_invalid_get_table_fn_values() {
        let mut model = Model::new();

        let box_src = model.make_box(get_user_id(0).into());
        let box_get = model.make_box(get_user_id(1).into());
        let box_table_fn = model.make_box(TableFunction::default().into());
        let box_values = model.make_box(Values::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_src, box_get);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_table_fn);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_values);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_get, ZERO_ARBITRARY),
            ValidationError::InvalidInputQuantifiers(box_table_fn, ZERO_ARBITRARY),
            ValidationError::InvalidInputQuantifiers(box_values, ZERO_ARBITRARY),
        ]);
        assert_eq!(errors_act, errors_exp);
    }

    /// Tests the "box types that can only have input quantifiers of type Foreach" case.
    #[test]
    fn test_invalid_union_except_intersect() {
        let mut model = Model::new();

        let box_src = model.make_box(get_user_id(0).into());
        let box_union = model.make_box(BoxType::Union);
        let box_except = model.make_box(BoxType::Except);
        let box_intersect = model.make_box(BoxType::Intersect);

        model.make_quantifier(QuantifierType::PreservedForeach, box_src, box_union);
        model.make_quantifier(QuantifierType::PreservedForeach, box_src, box_except);
        model.make_quantifier(QuantifierType::PreservedForeach, box_src, box_intersect);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_union, ONE_OR_MORE_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_union, ZERO_NOT_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_except, ONE_OR_MORE_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_except, ZERO_NOT_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_intersect, ONE_OR_MORE_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_intersect, ZERO_NOT_FOREACH),
        ]);
        assert_eq!(errors_act, errors_exp);
    }

    fn get_user_id(id: u64) -> Get {
        Get {
            id: expr::GlobalId::User(id),
        }
    }
}
