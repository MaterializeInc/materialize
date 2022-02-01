// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines the [`NonNullRequirements`] attribute.
//!
//! The attribute value is a vector of column references corresponding
//! to the columns of the associated `QueryBox`.
//!
//! If any of the references is `NULL`, the corresponding column will
//! also be `NULL`.

use crate::query_model::attribute::core::{Attribute, AttributeKey};
use crate::query_model::model::{BoxId, BoxScalarExpr, ColumnReference, Model};
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub(crate) struct NonNullRequirements;

impl AttributeKey for NonNullRequirements {
    type Value = Vec<HashSet<ColumnReference>>;
}

impl Attribute for NonNullRequirements {
    fn attr_id(&self) -> &'static str {
        "NonNullRequirements"
    }

    fn requires(&self) -> Vec<Box<dyn Attribute>> {
        vec![]
    }

    fn derive(&self, model: &mut Model, box_id: BoxId) {
        let mut r#box = model.get_mut_box(box_id);

        let value = r#box
            .columns
            .iter()
            .map(|x| non_null_requirements(&x.expr))
            .collect::<Vec<_>>();

        // TODO: remove this
        // println!("|box[{}].columns| = {:?}", box_id, r#box.columns.len());
        // println!("attr[{}] = {:?}", box_id, value);

        r#box.attributes.set::<NonNullRequirements>(value);
    }
}

/// Returns all columns that *must* be non-Null for the `expr` to be non-Null.
pub(crate) fn non_null_requirements(expr: &BoxScalarExpr) -> HashSet<ColumnReference> {
    use BoxScalarExpr::*;
    let mut result = HashSet::new();

    expr.try_visit_pre_post(
        &mut |expr| {
            match expr {
                ColumnReference(col) => {
                    result.insert(col.clone());
                    None
                }
                BaseColumn(..) | Literal(..) | CallNullary(_) => None,
                CallUnary { func, .. } => {
                    if func.propagates_nulls() {
                        None
                    } else {
                        Some(vec![])
                    }
                }
                CallBinary { func, .. } => {
                    if func.propagates_nulls() {
                        None
                    } else {
                        Some(vec![])
                    }
                }
                CallVariadic { func, .. } => {
                    if func.propagates_nulls() {
                        None
                    } else {
                        Some(vec![])
                    }
                }
                // The branches of an if are computed lazily, but the condition is not
                // so we can descend into it safely
                If { cond, .. } => Some(vec![cond]),
                // TODO the non-null requeriments of an aggregate expression can
                // be pused down to, for example, convert an outer join into an
                // inner join
                Aggregate { .. } => Some(vec![]),
            }
        },
        &mut |_| (),
    );

    return result;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::model::*;
    use crate::query_model::test::util::*;

    #[test]
    fn test_derivation() {
        let mut model = Model::new();
        let g_id = model.make_box(qgm::get(0).into());
        {
            let mut b = model.get_mut_box(g_id);
            b.add_column(exp::base(0, typ::int32(true)));
            b.add_column(exp::base(1, typ::int32(true)));
            b.add_column(exp::base(2, typ::int32(true)));
            b.add_column(exp::base(3, typ::int32(true)));
        }

        let s_id = model.make_box(Select::default().into());
        let q_id = model.make_quantifier(QuantifierType::Foreach, g_id, s_id);
        {
            let mut b = model.get_mut_box(s_id);
            // C0: (#0 - #1) + (#2 - #3)
            b.add_column(exp::add(
                exp::sub(exp::cref(q_id, 0), exp::cref(q_id, 1)),
                exp::sub(exp::cref(q_id, 2), exp::cref(q_id, 3)),
            ));
            // C1: (#0 > #1) || (#2 > #3)
            b.add_column(exp::or(
                exp::gt(exp::cref(q_id, 0), exp::cref(q_id, 1)),
                exp::gt(exp::cref(q_id, 2), exp::cref(q_id, 3)),
            ));
            // C1: (#0 > #1) && isnull(#1)
            b.add_column(exp::and(
                exp::gt(exp::cref(q_id, 0), exp::cref(q_id, 1)),
                exp::not(exp::isnull(exp::cref(q_id, 1))),
            ));
        }

        NonNullRequirements.derive(&mut model, s_id);

        {
            let s_box = model.get_box(s_id);

            let act_value = s_box.attributes.get::<NonNullRequirements>();
            let exp_value = &vec![
                HashSet::from([cref(q_id, 0), cref(q_id, 1), cref(q_id, 2), cref(q_id, 3)]),
                HashSet::from([]),
                HashSet::from([]),
            ];

            assert_eq!(act_value, exp_value);
        }
    }
}
