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
use crate::query_model::model::*;
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
    quantifiers: impl Iterator<Item = BoundRef<'a, Quantifier>>,
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

    pub(crate) const ZERO_NOT_SUBQUERY: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(0),
        allowed_types: ARBITRARY_QUANTIFIER & !(SUBQUERY_QUANTIFIER as usize),
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

    pub(crate) const ZERO_PRESERVED_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(0),
        allowed_types: QuantifierType::PreservedForeach as usize,
        select_input: false,
        select_parent: false,
    };

    pub(crate) const ONE_OR_MORE_NOT_PRES_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(1),
        max: Bound::Unbounded,
        allowed_types: ARBITRARY_QUANTIFIER & !(QuantifierType::PreservedForeach as usize),
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

    pub(crate) const ONE_FOREACH_INPUT_SELECT: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(1),
        max: Bound::Included(1),
        allowed_types: QuantifierType::Foreach as usize,
        select_input: true,
        select_parent: false,
    };

    pub(crate) const ONE_FOREACH_PARENT_SELECT: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(1),
        max: Bound::Included(1),
        allowed_types: QuantifierType::Foreach as usize,
        select_input: false,
        select_parent: true,
    };

    pub(crate) const ONE_OR_TWO_PRES_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(1),
        max: Bound::Included(2),
        allowed_types: QuantifierType::PreservedForeach as usize,
        select_input: false,
        select_parent: false,
    };

    pub(crate) const ZERO_OR_ONE_FOREACH: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(1),
        allowed_types: QuantifierType::Foreach as usize,
        select_input: false,
        select_parent: false,
    };

    pub(crate) const TWO_OUTER_JOIN_INPUTS: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(2),
        max: Bound::Included(2),
        allowed_types: QuantifierType::PreservedForeach as usize | QuantifierType::Foreach as usize,
        select_input: false,
        select_parent: false,
    };

    pub(crate) const ZERO_NOT_OUTER_JOIN_INPUTS: QuantifierConstraint = QuantifierConstraint {
        min: Bound::Included(0),
        max: Bound::Included(0),
        allowed_types: !TWO_OUTER_JOIN_INPUTS.allowed_types,
        select_input: false,
        select_parent: false,
    };
}

/// A [`Validator`] that checks the following quantifier constraints:
/// - `Get` and `Values` boxes cannot have input
///    quantifiers.
/// - `TableFunction` boxes can only have subquery input quantifiers.
/// - `Union`, `Except` and `Intersect` boxes can only have input
///    quantifiers of type `Foreach`.
/// - A `Grouping` box must have a single input quantifier of type
///   `Foreach` ranging over the contents of a `Select` box.
/// - A `Grouping` box must have a single ranging quantifier of type
///   `Foreach` that has a `Select` box as its parent.
/// - A `Select` box must have one or more input quantifiers of
///   arbitrary type (except `PreservedForeach`).
/// - An `OuterJoin` box must have one or two input quantifiers of
///   type `PreservedForeach`.
/// - An `OuterJoin` box must have at most one input quantifier of
///   type `Foreach`.
/// - An `OuterJoin` box must have exactly two input quantifiers of
///   type `Foreach` or `PreservedForeach`.
#[derive(Default)]
pub(crate) struct QuantifierConstraintValidator;

impl Validator for QuantifierConstraintValidator {
    fn validate(&self, model: &Model) -> ValidationResult {
        let mut errors = vec![];

        for b in model.boxes_iter() {
            use constants::*;
            use BoxType::*;
            match b.box_type {
                Get(..) | Values(..) => {
                    let constraints = [ZERO_ARBITRARY];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    })
                }
                CallTable(..) => {
                    let constraints = [ZERO_NOT_SUBQUERY];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    })
                }
                Union | Except | Intersect => {
                    let constraints = [ONE_OR_MORE_FOREACH, ZERO_NOT_FOREACH];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    })
                }
                Grouping(..) => {
                    let constraints = [ONE_FOREACH_INPUT_SELECT, ZERO_NOT_FOREACH];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    });
                    let constraints = [ONE_FOREACH_PARENT_SELECT, ZERO_NOT_FOREACH];
                    check_constraints(&constraints, b.ranging_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidRangingQuantifiers(b.id, c.clone()))
                    });
                }
                Select(..) => {
                    let constraints = [ONE_OR_MORE_NOT_PRES_FOREACH, ZERO_PRESERVED_FOREACH];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    });
                }
                OuterJoin(..) => {
                    let constraints = [
                        ONE_OR_TWO_PRES_FOREACH,
                        ZERO_OR_ONE_FOREACH,
                        TWO_OUTER_JOIN_INPUTS,
                        ZERO_NOT_OUTER_JOIN_INPUTS,
                    ];
                    check_constraints(&constraints, b.input_quantifiers(), model, |c| {
                        errors.push(ValidationError::InvalidInputQuantifiers(b.id, c.clone()))
                    });
                }
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
    use mz_expr::TableFunc;

    use super::constants::*;
    use super::*;
    use crate::query_model::test::util::*;
    use std::collections::HashSet;

    /// Tests constraints for quantifiers incident with [`BoxType::Get`]
    /// and [`BoxType::Values`] boxes (happy case).
    #[test]
    fn test_invalid_get_values_ok() {
        let mut model = Model::default();

        let box_get = model.make_box(qgm::get(1).into());
        let box_values = model.make_box(Values::default().into());
        let box_tgt = model.make_box(Select::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_get, box_tgt);
        model.make_quantifier(QuantifierType::Foreach, box_values, box_tgt);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Get`]
    /// and [`BoxType::Values`] boxes (bad case).
    #[test]
    fn test_invalid_get_values_not_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_get = model.make_box(qgm::get(1).into());
        let box_values = model.make_box(Values::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_src, box_get);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_values);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_get, ZERO_ARBITRARY),
            ValidationError::InvalidInputQuantifiers(box_values, ZERO_ARBITRARY),
        ]);
        assert_eq!(errors_act, errors_exp);
    }

    /// Tests constraints for quantifiers incident with [`BoxType::CallTable`]
    /// boxes (happy case).
    #[test]
    fn test_invalid_call_table_ok() {
        use TableFunc::GenerateSeriesInt32;

        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let call_table = CallTable::new(GenerateSeriesInt32, vec![]);
        let box_table_fn = model.make_box(call_table.into());
        let box_tgt = model.make_box(Select::default().into());

        model.make_quantifier(QuantifierType::Scalar, box_src, box_table_fn);
        model.make_quantifier(QuantifierType::Foreach, box_table_fn, box_tgt);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::CallTable`]
    /// boxes (bad case).
    #[test]
    fn test_invalid_call_table_not_ok() {
        use TableFunc::GenerateSeriesInt32;

        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let call_table = CallTable::new(GenerateSeriesInt32, vec![]);
        let box_table_fn = model.make_box(call_table.into());

        model.make_quantifier(QuantifierType::Scalar, box_src, box_table_fn);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_table_fn);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([ValidationError::InvalidInputQuantifiers(
            box_table_fn,
            ZERO_NOT_SUBQUERY,
        )]);
        assert_eq!(errors_act, errors_exp);
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Grouping`],
    /// [`BoxType::Except`], and [`BoxType::Intersect`] boxes (happy case).
    #[test]
    fn test_invalid_union_except_intersect_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_union = model.make_box(BoxType::Union);
        let box_except = model.make_box(BoxType::Except);
        let box_intersect = model.make_box(BoxType::Intersect);

        model.make_quantifier(QuantifierType::Foreach, box_src, box_union);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_union);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_except);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_except);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_intersect);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_intersect);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Grouping`],
    /// [`BoxType::Except`], and [`BoxType::Intersect`] boxes (bad case).
    #[test]
    fn test_invalid_union_except_intersect_not_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
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

    /// Tests constraints for quantifiers incident with [`BoxType::Grouping`] boxes (happy case).
    #[test]
    fn test_invalid_grouping_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_select = model.make_box(Select::default().into());
        let box_grouping = model.make_box(Grouping::default().into());
        let box_dst = model.make_box(Select::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_src, box_select);
        model.make_quantifier(QuantifierType::Foreach, box_select, box_grouping);
        model.make_quantifier(QuantifierType::Foreach, box_grouping, box_dst);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Grouping`] boxes (bad case).
    #[test]
    fn test_invalid_grouping_not_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_grouping = model.make_box(Grouping::default().into());
        let box_dst = model.make_box(BoxType::Union);

        model.make_quantifier(QuantifierType::Existential, box_src, box_grouping);
        model.make_quantifier(QuantifierType::Foreach, box_src, box_grouping);
        model.make_quantifier(QuantifierType::Foreach, box_grouping, box_dst);
        model.make_quantifier(QuantifierType::Foreach, box_grouping, box_dst);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_grouping, ZERO_NOT_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_grouping, ONE_FOREACH_INPUT_SELECT),
            ValidationError::InvalidRangingQuantifiers(box_grouping, ONE_FOREACH_PARENT_SELECT),
        ]);
        assert_eq!(errors_act, errors_exp);
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Select`] boxes (happy case).
    #[test]
    fn test_invalid_select_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_select = model.make_box(Select::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_src, box_select);
        model.make_quantifier(QuantifierType::Existential, box_src, box_select);
        model.make_quantifier(QuantifierType::Scalar, box_src, box_select);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::Select`] boxes (bad case).
    #[test]
    fn test_invalid_select_not_ok() {
        let mut model = Model::default();

        let box_src = model.make_box(qgm::get(0).into());
        let box_select = model.make_box(Select::default().into());

        model.make_quantifier(QuantifierType::PreservedForeach, box_src, box_select);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_select, ONE_OR_MORE_NOT_PRES_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_select, ZERO_PRESERVED_FOREACH),
        ]);
        assert_eq!(errors_act, errors_exp);
    }

    /// Tests constraints for quantifiers incident with [`BoxType::OuterJoin`] boxes (happy case).
    #[test]
    fn test_outer_join_ok() {
        let mut model = Model::default();

        let box_lhs = model.make_box(qgm::get(0).into());
        let box_rhs = model.make_box(qgm::get(1).into());
        let box_outer_join_1 = model.make_box(OuterJoin::default().into());
        let box_outer_join_2 = model.make_box(OuterJoin::default().into());

        model.make_quantifier(QuantifierType::Foreach, box_lhs, box_outer_join_1);
        model.make_quantifier(QuantifierType::PreservedForeach, box_rhs, box_outer_join_1);
        model.make_quantifier(QuantifierType::PreservedForeach, box_lhs, box_outer_join_2);
        model.make_quantifier(QuantifierType::PreservedForeach, box_rhs, box_outer_join_2);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_ok());
    }

    /// Tests constraints for quantifiers incident with [`BoxType::OuterJoin`] boxes (bad case).
    #[test]
    fn test_outer_join_not_ok() {
        let mut model = Model::default();

        let box_lhs = model.make_box(qgm::get(0).into());
        let box_rhs = model.make_box(qgm::get(1).into());
        let box_outer_join_1 = model.make_box(OuterJoin::default().into());
        let box_outer_join_2 = model.make_box(OuterJoin::default().into());

        model.make_quantifier(QuantifierType::Existential, box_lhs, box_outer_join_1);
        model.make_quantifier(QuantifierType::Scalar, box_rhs, box_outer_join_1);
        model.make_quantifier(QuantifierType::Foreach, box_lhs, box_outer_join_2);
        model.make_quantifier(QuantifierType::Foreach, box_rhs, box_outer_join_2);

        let result = QuantifierConstraintValidator::default().validate(&model);
        assert!(result.is_err());

        let errors_act: HashSet<_> = result.unwrap_err().drain(..).collect();
        let errors_exp = HashSet::from([
            ValidationError::InvalidInputQuantifiers(box_outer_join_1, ONE_OR_TWO_PRES_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_outer_join_1, TWO_OUTER_JOIN_INPUTS),
            ValidationError::InvalidInputQuantifiers(box_outer_join_1, ZERO_NOT_OUTER_JOIN_INPUTS),
            ValidationError::InvalidInputQuantifiers(box_outer_join_2, ONE_OR_TWO_PRES_FOREACH),
            ValidationError::InvalidInputQuantifiers(box_outer_join_2, ZERO_OR_ONE_FOREACH),
        ]);
        assert_eq!(errors_act, errors_exp);
    }
}
