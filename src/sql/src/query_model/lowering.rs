// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::{
    BoxId, BoxScalarExpr, BoxType, ColumnReference, DistinctOperation, Model, QuantifierSet,
    QuantifierType,
};
use itertools::Itertools;
use repr::RelationType;
use std::collections::HashMap;

/// Maps a column reference to a specific column position.
///
/// This is used for resolving column references when lowering expressions
/// on top of a `MirRelationExpr`, where it maps each column reference with
/// with a column position within the projection of that `MirRelationExpr`.
type ColumnMap = HashMap<ColumnReference, usize>;

impl Model {
    pub fn lower(&self) -> expr::MirRelationExpr {
        let mut lowerer = Lowerer::new(self);
        let mut id_gen = ore::id_gen::IdGen::default();
        expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
            .let_in(&mut id_gen, |_id_gen, get_outer| {
                lowerer.apply(self.top_box, get_outer, &ColumnMap::new())
            })
    }
}

struct Lowerer<'a> {
    model: &'a Model,
}

impl<'a> Lowerer<'a> {
    fn new(model: &'a Model) -> Self {
        Self { model }
    }

    /// Applies the given box identified by its ID to the given outer relation.
    fn apply(
        &mut self,
        box_id: BoxId,
        get_outer: expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
    ) -> expr::MirRelationExpr {
        use expr::MirRelationExpr as SR;
        let the_box = self.model.get_box(box_id).borrow();

        assert_eq!(
            the_box.distinct,
            DistinctOperation::Preserve,
            "DISTINCT is not supported yet"
        );

        match &the_box.box_type {
            BoxType::Values(values) => {
                let identity = SR::constant(vec![vec![]], RelationType::new(vec![]));
                // TODO(asenac) lower actual values
                assert!(values.rows.len() == 1 && the_box.columns.len() == 0);
                get_outer.product(identity)
            }
            BoxType::Select(select) => {
                assert!(select.order_key.is_none(), "ORDER BY is not supported yet");
                assert!(
                    select.limit.is_none() && select.offset.is_none(),
                    "LIMIT/OFFSET is not supported yet"
                );
                // A Select box combines three operations, join, filter and project,
                // in that order.

                // 1) Lower the join component of the Select box.
                // TODO(asenac) We could join first all non-correlated quantifiers
                // and then apply the correlated ones one by one in order of dependency
                // on top the join built so far, adding the predicates as soon as their
                // dependencies are satisfied.
                let correlation_info = the_box.correlation_info(&self.model);
                if !correlation_info.is_empty() {
                    panic!("correlated joins are not supported yet");
                }

                let outer_arity = get_outer.arity();
                let (mut input, column_map) =
                    self.lower_join(get_outer, outer_column_map, &the_box.quantifiers);
                let input_arity = input.arity();

                // 2) Lower the filter component.
                if !select.predicates.is_empty() {
                    input = input.filter(
                        select
                            .predicates
                            .iter()
                            .map(|p| Self::lower_expression(p, &column_map)),
                    );
                }

                // 3) Lower the project component.
                if !the_box.columns.is_empty() {
                    input = input.map(
                        the_box
                            .columns
                            .iter()
                            .map(|c| Self::lower_expression(&c.expr, &column_map))
                            .collect_vec(),
                    );
                }

                // Project the outer columns plus the ones in the projection of
                // this select box
                input.project(
                    (0..outer_arity)
                        .chain(input_arity..input_arity + the_box.columns.len())
                        .collect_vec(),
                )
            }
            _ => panic!(),
        }
    }

    /// Generates a join among the result of applying the outer relation to the given
    /// quantifiers. Returns a relation and a map of column references that can be
    /// used to lower expressions that sit directly on top of the join.
    ///
    /// The quantifiers are joined on the columns of the outer relation.
    /// TODO(asenac) Since decorrelation is not yet supported the outer relation is
    /// currently always the join identity, so the result of this method is always
    /// a cross-join of the given quantifiers, which makes part of this code untesteable
    /// at the moment.
    fn lower_join(
        &mut self,
        get_outer: expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
        quantifiers: &QuantifierSet,
    ) -> (expr::MirRelationExpr, ColumnMap) {
        if let expr::MirRelationExpr::Get { .. } = &get_outer {
        } else {
            panic!(
                "get_outer: expected a MirRelationExpr::Get, found {:?}",
                get_outer
            );
        }

        let outer_arity = get_outer.arity();

        // TODO(asenac) Lower scalar subquery quantifiers
        assert!(quantifiers.iter().all(|q_id| {
            self.model.get_quantifier(*q_id).borrow().quantifier_type == QuantifierType::Foreach
        }));

        let join_inputs = quantifiers
            .iter()
            .map(|q_id| {
                let input_box = self.model.get_quantifier(*q_id).borrow().input_box;
                self.apply(input_box, get_outer.clone(), outer_column_map)
            })
            .collect_vec();

        let join = if join_inputs.len() == 1 {
            join_inputs.into_iter().next().unwrap()
        } else {
            Self::join_on_prefix(join_inputs, outer_arity)
        };

        // Generate a column map with the projection of the join plus the
        // columns from the outer context.
        let mut column_map = outer_column_map.clone();
        let mut next_column = outer_arity;
        for q_id in quantifiers.iter() {
            let input_box = self.model.get_quantifier(*q_id).borrow().input_box;
            let arity = self.model.get_box(input_box).borrow().columns.len();
            for c in 0..arity {
                column_map.insert(
                    ColumnReference {
                        quantifier_id: *q_id,
                        position: c,
                    },
                    next_column + c,
                );
            }

            next_column += arity;
        }

        (join, column_map)
    }

    /// Join the given inputs on a shared common prefix.
    ///
    /// TODO(asenac) Given the lack of support for decorrelation at the moment, this
    /// method is always called with `prefix_length` 0, and hence, it remains untested.
    fn join_on_prefix(
        join_inputs: Vec<expr::MirRelationExpr>,
        prefix_length: usize,
    ) -> expr::MirRelationExpr {
        let input_mapper = expr::JoinInputMapper::new(&join_inputs);
        // Join on the outer columns
        let equivalences = (0..prefix_length)
            .map(|col| {
                join_inputs
                    .iter()
                    .enumerate()
                    .map(|(input, _)| {
                        expr::MirScalarExpr::Column(input_mapper.map_column_to_global(col, input))
                    })
                    .collect_vec()
            })
            .collect_vec();
        // Project only one copy of the outer columns
        let projection = (0..prefix_length)
            .chain(
                join_inputs
                    .iter()
                    .enumerate()
                    .map(|(index, input)| {
                        (prefix_length..input.arity())
                            .map(|c| input_mapper.map_column_to_global(c, index))
                            .collect_vec()
                    })
                    .flatten(),
            )
            .collect_vec();
        expr::MirRelationExpr::join_scalars(join_inputs, equivalences).project(projection)
    }

    /// Lowers a scalar expression, resolving the column references using
    /// the supplied column map.
    fn lower_expression(expr: &BoxScalarExpr, column_map: &ColumnMap) -> expr::MirScalarExpr {
        match expr {
            BoxScalarExpr::ColumnReference(c) => {
                expr::MirScalarExpr::Column(*column_map.get(c).unwrap())
            }
            BoxScalarExpr::Literal(row, column_type) => {
                expr::MirScalarExpr::Literal(Ok(row.clone()), column_type.clone())
            }
            _ => panic!("unsupported expression"),
        }
    }
}
