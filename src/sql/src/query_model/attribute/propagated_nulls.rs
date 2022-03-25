// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines the [`PropagatedNulls`] attribute.
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
pub(crate) struct PropagatedNulls;

impl AttributeKey for PropagatedNulls {
    type Value = Vec<HashSet<ColumnReference>>;
}

impl Attribute for PropagatedNulls {
    fn attr_id(&self) -> &'static str {
        "PropagatedNulls"
    }

    fn requires(&self) -> Vec<Box<dyn Attribute>> {
        vec![]
    }

    fn derive(&self, model: &mut Model, box_id: BoxId) {
        let mut r#box = model.get_mut_box(box_id);

        let value = r#box
            .columns
            .iter()
            .map(|x| propagated_nulls(&x.expr))
            .collect::<Vec<_>>();

        // TODO: remove this
        // println!("|box[{}].columns| = {:?}", box_id, r#box.columns.len());
        // println!("attr[{}] = {:?}", box_id, value);

        r#box.attributes.set::<PropagatedNulls>(value);
    }
}

/// Returns all columns that *must* be non-Null for the `expr` to be non-Null.
pub(crate) fn propagated_nulls(expr: &BoxScalarExpr) -> HashSet<ColumnReference> {
    use BoxScalarExpr::*;
    let mut result = HashSet::new();

    expr.try_visit_pre_post(
        &mut |expr| {
            match expr {
                ColumnReference(col) => {
                    result.insert(col.clone());
                    None
                }
                BaseColumn(..) | Literal(..) | CallUnmaterializable(_) => None,
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
                // The branches of an if are computed lazily, but the condition is not.
                // However, nulls propagate to the condition are cast to false.
                // Consequently, we currently don't do anything here.
                // TODO: I think we might be able to take use the intersection of the
                // results in the two branches.
                If { .. } => Some(vec![]),
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
        let mut model = Model::default();
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
            // C2: (#0 > #1) && isnull(#1)
            b.add_column(exp::and(
                exp::gt(exp::cref(q_id, 0), exp::cref(q_id, 1)),
                exp::not(exp::isnull(exp::cref(q_id, 1))),
            ));
        }

        PropagatedNulls.derive(&mut model, s_id);

        {
            let s_box = model.get_box(s_id);

            let act_value = s_box.attributes.get::<PropagatedNulls>();
            let exp_value = &vec![
                HashSet::from([cref(q_id, 0), cref(q_id, 1), cref(q_id, 2), cref(q_id, 3)]),
                HashSet::from([]),
                HashSet::from([]),
            ];

            assert_eq!(act_value, exp_value);
        }
    }
}
