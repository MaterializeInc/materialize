// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines the [`RelationType`] attribute.
//!
//! The attribute value is a vector of [`ColumnType`] associated with
//! the output boxes of each query box.
//!
//! To derive [`Model::cols_type`] we need to derive [`Model::expr_type`] for
//! each column expression in that box. To derive the [`Model::expr_type`],
//! we need to derive [`Model::cref_type`], which in turn requires deriving
//! [`Model::cols_type`] on the input box of the quantifier associated with
//! the [`Model::cref_type`] argument.
//!
//! Derivation therefore must proceed in a bottom-up manner.
//!
//! The implementation makes use of the derived attributes
//! [`RejectedNulls`] as follows: If a [`ColumnReference`]
//! cannot be null if the enclosing box rejects nulls for it.

use mz_repr::{ColumnType, ScalarType};

use crate::query_model::attribute::core::{Attribute, AttributeKey};
use crate::query_model::attribute::rejected_nulls::RejectedNulls;
use crate::query_model::model::{
    BoundRef, BoxId, BoxScalarExpr, BoxType, ColumnReference, Model, QuantifierType, QueryBox,
};

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct RelationType;

impl AttributeKey for RelationType {
    type Value = Vec<ColumnType>;
}

impl Attribute for RelationType {
    fn attr_id(&self) -> &'static str {
        "RelationType"
    }

    fn requires(&self) -> Vec<Box<dyn Attribute>> {
        vec![Box::new(RejectedNulls) as Box<dyn Attribute>]
    }

    #[allow(unused_variables)]
    fn derive(&self, model: &mut Model, box_id: BoxId) {
        let value = model.cols_type(box_id);
        let mut r#box = model.get_mut_box(box_id);
        r#box.attributes.set::<RelationType>(value);
    }
}

/// Helper methods for the derivation of the `RelationType` attribute.
impl Model {
    /// Infers a vector of [`ColumnType`] values for the `columns`
    /// of the given [`BoxId`].
    ///
    /// *Note*: this method should be used only internally in order
    /// to derive the [`RelationType`] attribute of a [`Model`].
    fn cols_type(&self, box_id: BoxId) -> Vec<ColumnType> {
        self.get_box(box_id)
            .columns
            .iter()
            .map(|x| self.expr_type(&x.expr, box_id))
            .collect::<Vec<_>>()
    }

    /// Infers a [`ColumnType`] corresponding to the given
    /// [`BoxScalarExpr`] used within the context of an
    /// enclosing [`BoxId`].
    ///
    /// *Note*: this method should be used only internally in order
    /// to derive the [`RelationType`] attribute of a [`Model`].
    fn expr_type(&self, expr: &BoxScalarExpr, enclosing_box_id: BoxId) -> ColumnType {
        use BoxScalarExpr::*;

        // a result stack for storing intermediate types
        let mut results = Vec::<ColumnType>::new();

        expr.visit_post(&mut |expr| match expr {
            BaseColumn(c) => results.push(c.column_type.clone()),
            ColumnReference(c) => results.push(self.cref_type(c, enclosing_box_id)),
            Literal(_, typ) => results.push(typ.clone()),
            CallUnmaterializable(func) => results.push(func.output_type()),
            CallUnary { func, .. } => {
                let input_type = results.pop().unwrap();
                results.push(func.output_type(input_type));
            }
            CallBinary { func, .. } => {
                let input2_type = results.pop().unwrap();
                let input1_type = results.pop().unwrap();
                results.push(func.output_type(input1_type, input2_type));
            }
            CallVariadic { exprs, func } => {
                let input_types = exprs.iter().map(|_| results.pop().unwrap()).rev().collect();
                results.push(func.output_type(input_types));
            }
            If { .. } => {
                let else_type = results.pop().unwrap();
                let then_type = results.pop().unwrap();
                let cond_type = results.pop().unwrap();
                debug_assert!(then_type.scalar_type.base_eq(&else_type.scalar_type));
                debug_assert!(cond_type.scalar_type.base_eq(&ScalarType::Bool));
                results.push(ColumnType {
                    scalar_type: then_type.scalar_type,
                    nullable: then_type.nullable || else_type.nullable,
                });
            }
            Aggregate { func, .. } => {
                let input_type = results.pop().unwrap();
                results.push(func.output_type(input_type));
            }
        });

        debug_assert!(
            results.len() == 1,
            "expr_type: unexpected results stack size (expected 1, got {})",
            results.len()
        );

        results.pop().unwrap()
    }

    /// Looks up a [`ColumnType`] corresponding to the given
    /// [`ColumnReference`] from the [`RelationType`] attribute
    /// of the input box, assuming the [`ColumnReference`]
    /// occurs within the context of the enclosing [`BoxId`].
    ///
    /// *Note*: this method should be used only internally in order
    /// to derive the [`RelationType`] attribute of a [`Model`].
    fn cref_type(&self, cref: &ColumnReference, enclosing_box_id: BoxId) -> ColumnType {
        let quantifier = self.get_quantifier(cref.quantifier_id);

        // the type of All and Existential quantifiers is always BOOLEAN NOT NULL
        if matches!(
            quantifier.quantifier_type,
            QuantifierType::Existential | QuantifierType::All
        ) {
            return ColumnType {
                scalar_type: ScalarType::Bool,
                nullable: false,
            };
        }

        let input_box = quantifier.input_box();
        let column_type = &input_box.attributes.get::<RelationType>()[cref.position];

        let parent_box = self.get_box(quantifier.parent_box);
        let nullable = if enclosing_box_id == parent_box.id {
            if quantifier.quantifier_type == QuantifierType::Scalar {
                // scalar quantifiers can always be NULL if the subquery is empty
                true
            } else {
                use BoxType::*;
                // In general, nullability is influenced by the parent box of the quantifier.
                match &parent_box.box_type {
                    // The type can be restricted to NOT NULL if it is used in a Select
                    // box with a predicate that rejects nulls in that ColumnReference.
                    Select(..) if select_rejects_nulls(&parent_box, cref) => false,
                    // The type should be widened to NULL if it is used in an OuterJoin
                    // box where the opposite side is preserving.
                    OuterJoin(..) if outer_join_introduces_nulls(&parent_box, cref) => true,
                    // The type of aggregates in grouping boxes without keys (a.k.a.
                    // global aggregates) can always be NULL. This works correctly only
                    // under the assumption that the output columns of a Grouping box
                    // are always trivial ColumnReference expressions.
                    Grouping(grouping) if grouping.key.is_empty() => true,
                    // The type should be widened to NULL if it is used in a Union
                    // box with at least one quantifier in the same position being NULL.
                    Union if union_introduces_nulls(&parent_box, cref) => true,
                    // The type should be restricted to NOT NULL if it is used in an
                    // Intersect box with at least one quantifier in the same position
                    // being NOT NULL.
                    Intersect if intersect_rejects_nulls(&parent_box, cref) => false,
                    // otherwise, just inherit the nullable information from the input
                    _ => column_type.nullable.clone(),
                }
            }
        } else {
            // If the enclosing box is different from the parent box (which
            // can happen in a graph derived from a correlated subquery), the
            // RelationType of the latter might not be fully derived. In this
            // case, we therefore just inherit the nullability of the input.
            column_type.nullable.clone()
        };

        ColumnType {
            scalar_type: column_type.scalar_type.clone(),
            nullable,
        }
    }
}

/// A [`ColumnReference`] rejects nulls if:
/// 1. the enclosing box is of type [`BoxType::Select`] (asserted), and
/// 2. the [`RejectedNulls`] attribute rejects nulls in the [`ColumnReference`].
#[inline]
fn select_rejects_nulls(r#box: &BoundRef<'_, QueryBox>, cref: &ColumnReference) -> bool {
    debug_assert!(matches!(r#box.box_type, BoxType::Select(..)));
    r#box.attributes.get::<RejectedNulls>().contains(cref)
}

/// A quantifier referenced the given [`ColumnReference`] introduces nulls if:
/// 1. the enclosing box is of type [`BoxType::OuterJoin`] (asserted), and
/// 2. the other quantifier is preserving.
#[inline]
fn outer_join_introduces_nulls(r#box: &BoundRef<'_, QueryBox>, cref: &ColumnReference) -> bool {
    debug_assert!(matches!(r#box.box_type, BoxType::OuterJoin(..)));
    r#box
        .input_quantifiers()
        .filter(|q| q.id != cref.quantifier_id)
        .any(|q| q.quantifier_type == QuantifierType::PreservedForeach)
}

/// A quantifier referenced the given [`ColumnReference`] introduces nulls if:
/// 1. the enclosing box is of type [`BoxType::Union`] (asserted), and
/// 2. at least one of the input quantifiers for the given position is nullable.
#[inline]
fn union_introduces_nulls(r#box: &BoundRef<'_, QueryBox>, cref: &ColumnReference) -> bool {
    debug_assert!(matches!(r#box.box_type, BoxType::Union));
    r#box
        .input_quantifiers()
        .any(|q| q.input_box().attributes.get::<RelationType>()[cref.position].nullable)
}

/// A quantifier referenced the given [`ColumnReference`] introduces nulls if:
/// 1. the enclosing box is of type [`BoxType::Intersect`] (asserted), and
/// 2. at least one of the input quantifiers for the given position is not nullable.
#[inline]
fn intersect_rejects_nulls(r#box: &BoundRef<'_, QueryBox>, cref: &ColumnReference) -> bool {
    debug_assert!(matches!(r#box.box_type, BoxType::Intersect));
    r#box
        .input_quantifiers()
        .any(|q| !q.input_box().attributes.get::<RelationType>()[cref.position].nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::model::*;
    use crate::query_model::test::util::*;

    #[test]
    fn test_select() {
        let mut model = Model::default();
        let g_id = model.make_box(qgm::get(0).into());
        {
            let mut b = model.get_mut_box(g_id);
            b.add_column(exp::base(0, typ::int32(false)));
            b.add_column(exp::base(1, typ::int32(true)));
            b.add_column(exp::base(2, typ::int32(true)));
            b.add_column(exp::base(3, typ::int32(true)));
            b.add_column(exp::base(4, typ::int32(false)));
        }

        let s_id = model.make_box(Select::default().into());
        let q_id = model.make_quantifier(QuantifierType::Foreach, g_id, s_id);
        {
            let mut b = model.get_mut_box(s_id);
            // C0: (#0 + #1)
            b.add_column(exp::add(exp::cref(q_id, 0), exp::cref(q_id, 1)));
            // C0: (#1 - #2)
            b.add_column(exp::sub(exp::cref(q_id, 1), exp::cref(q_id, 2)));
            // C0: (#2 * #3)
            b.add_column(exp::mul(exp::cref(q_id, 2), exp::cref(q_id, 3)));
            // C0: (#3 / #4)
            b.add_column(exp::div(exp::cref(q_id, 3), exp::cref(q_id, 4)));

            if let BoxType::Select(s) = &mut b.box_type {
                s.predicates.extend_from_slice(&[
                    exp::gt(exp::cref(q_id, 1), exp::lit::int32(5)),
                    exp::not(exp::isnull(exp::cref(q_id, 2))),
                ]);
            }
        }

        // derive Get box (#0)
        RejectedNulls.derive(&mut model, g_id);
        RelationType.derive(&mut model, g_id);
        // derive Select box (#1)
        RejectedNulls.derive(&mut model, s_id);
        RelationType.derive(&mut model, s_id);

        {
            let b = model.get_box(s_id);

            let act_value = b.attributes.get::<RelationType>();
            let exp_value = &vec![
                typ::int32(false),
                typ::int32(false),
                typ::int32(true),
                typ::int32(true),
            ];

            assert_eq!(act_value, exp_value);
        }
    }

    #[test]
    fn test_outer_join() {
        let mut model = Model::default();
        let g_id = model.make_box(qgm::get(0).into());
        {
            let mut b = model.get_mut_box(g_id);
            b.add_column(exp::base(0, typ::int32(false)));
            b.add_column(exp::base(1, typ::int32(false)));
            b.add_column(exp::base(2, typ::int32(false)));
            b.add_column(exp::base(3, typ::int32(false)));
            b.add_column(exp::base(4, typ::int32(false)));
        }

        let o_id = model.make_box(OuterJoin::default().into());
        let l_id = model.make_quantifier(QuantifierType::PreservedForeach, g_id, o_id);
        let r_id = model.make_quantifier(QuantifierType::Foreach, g_id, o_id);
        {
            let mut b = model.get_mut_box(o_id);
            // C0: Q0.0
            b.add_column(exp::cref(l_id, 0));
            // C1: Q0.1
            b.add_column(exp::cref(l_id, 1));
            // C2: Q1.3
            b.add_column(exp::cref(r_id, 3));
            // C3: Q1.4
            b.add_column(exp::cref(r_id, 4));
        }

        // derive Get box (#0)
        RejectedNulls.derive(&mut model, g_id);
        RelationType.derive(&mut model, g_id);
        // derive OuterJoin box (#1)
        RejectedNulls.derive(&mut model, o_id);
        RelationType.derive(&mut model, o_id);

        {
            let b = model.get_box(o_id);

            let act_value = b.attributes.get::<RelationType>();
            let exp_value = &vec![
                typ::int32(false),
                typ::int32(false),
                typ::int32(true),
                typ::int32(true),
            ];

            assert_eq!(act_value, exp_value);
        }
    }

    #[test]
    fn test_union() {
        let mut model = Model::default();

        let g_ids = (0..=2)
            .map(|id| {
                let g_id = model.make_box(qgm::get(id).into());
                let mut b = model.get_mut_box(g_id);
                b.add_column(exp::base(0, typ::int32(false)));
                b.add_column(exp::base(1, typ::int32(id == 1)));
                b.add_column(exp::base(2, typ::int32(id == 2)));
                b.add_column(exp::base(3, typ::int32(id == 1)));
                b.add_column(exp::base(4, typ::int32(id == 2)));
                g_id
            })
            .collect::<Vec<_>>();

        let u_id = model.make_box(BoxType::Union);
        let q_ids = g_ids
            .iter()
            .map(|g_id| model.make_quantifier(QuantifierType::Foreach, *g_id, u_id))
            .collect::<Vec<_>>();

        {
            let mut b = model.get_mut_box(u_id);
            // C0: Q0.0
            b.add_column(exp::cref(q_ids[0], 0));
            // C1: Q0.1
            b.add_column(exp::cref(q_ids[0], 1));
            // C2: Q0.2
            b.add_column(exp::cref(q_ids[0], 2));
            // C3: Q0.3
            b.add_column(exp::cref(q_ids[0], 3));
            // C4: Q0.4
            b.add_column(exp::cref(q_ids[0], 4));
        }

        // derive Get box (#i)
        for g_id in g_ids {
            RejectedNulls.derive(&mut model, g_id);
            RelationType.derive(&mut model, g_id);
        }
        // derive OuterJoin box (#1)
        RejectedNulls.derive(&mut model, u_id);
        RelationType.derive(&mut model, u_id);

        {
            let b = model.get_box(u_id);

            let act_value = b.attributes.get::<RelationType>();
            let exp_value = &vec![
                typ::int32(false),
                typ::int32(true),
                typ::int32(true),
                typ::int32(true),
                typ::int32(true),
            ];

            assert_eq!(act_value, exp_value);
        }
    }

    #[test]
    fn test_exists_subquery() {
        let mut model = Model::default();

        let g_ids = (0..=1)
            .map(|id| {
                let g_id = model.make_box(qgm::get(id).into());
                let mut b = model.get_mut_box(g_id);
                b.add_column(exp::base(0, typ::int32(true)));
                g_id
            })
            .collect::<Vec<_>>();

        let s_id = model.make_box(Select::default().into());
        let q_ids = vec![
            model.make_quantifier(QuantifierType::Foreach, g_ids[0], s_id),
            model.make_quantifier(QuantifierType::Existential, g_ids[1], s_id),
        ];
        {
            let mut b = model.get_mut_box(s_id);
            // C0: Q0.0
            b.add_column(exp::cref(q_ids[0], 0));
            // C0: Q1.0
            b.add_column(exp::cref(q_ids[1], 0));
        }

        // derive Get box (#i)
        for g_id in g_ids {
            RejectedNulls.derive(&mut model, g_id);
            RelationType.derive(&mut model, g_id);
        }
        // derive Select box (#1)
        RejectedNulls.derive(&mut model, s_id);
        RelationType.derive(&mut model, s_id);

        {
            let b = model.get_box(s_id);

            let act_value = b.attributes.get::<RelationType>();
            let exp_value = &vec![typ::int32(true), typ::bool(false)];

            assert_eq!(act_value, exp_value);
        }
    }

    #[test]
    fn test_scalar_subquery() {
        let mut model = Model::default();

        let g_ids = (0..=1)
            .map(|id| {
                let g_id = model.make_box(qgm::get(id).into());
                let mut b = model.get_mut_box(g_id);
                b.add_column(exp::base(0, typ::int32(false)));
                b.add_column(exp::base(0, typ::bool(false)));
                g_id
            })
            .collect::<Vec<_>>();

        let s_id = model.make_box(Select::default().into());
        let q_ids = vec![
            model.make_quantifier(QuantifierType::Foreach, g_ids[0], s_id),
            model.make_quantifier(QuantifierType::Scalar, g_ids[1], s_id),
        ];
        {
            let mut b = model.get_mut_box(s_id);
            // C0: Q0.0
            b.add_column(exp::cref(q_ids[0], 0));
            // C0: Q0.1
            b.add_column(exp::cref(q_ids[0], 1));
            // C0: Q1.0
            b.add_column(exp::cref(q_ids[1], 0));
            // C0: Q1.1
            b.add_column(exp::cref(q_ids[1], 1));
        }

        // derive Get box (#i)
        for g_id in g_ids {
            RejectedNulls.derive(&mut model, g_id);
            RelationType.derive(&mut model, g_id);
        }
        // derive Select box (#1)
        RejectedNulls.derive(&mut model, s_id);
        RelationType.derive(&mut model, s_id);

        {
            let b = model.get_box(s_id);

            let act_value = b.attributes.get::<RelationType>();
            let exp_value = &vec![
                typ::int32(false),
                typ::bool(false),
                typ::int32(true),
                typ::bool(true),
            ];

            assert_eq!(act_value, exp_value);
        }
    }
}
