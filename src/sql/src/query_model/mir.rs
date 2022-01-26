// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Converts a Query Graph Model into a [expr::MirRelationExpr].
//!
//! The public interface consists of the implementation of
//! [`Into<expr::MirRelationExpr>`] for [`Model`].

use crate::query_model::model::{
    BaseColumn, BoxId, BoxScalarExpr, BoxType, ColumnReference, DistinctOperation, Get, Model,
    QuantifierId, QuantifierSet, QuantifierType,
};
use itertools::Itertools;
use ore::collections::CollectionExt;
use ore::id_gen::IdGen;
use repr::{Datum, RelationType, ScalarType};
use std::collections::HashMap;

impl From<Model> for expr::MirRelationExpr {
    fn from(model: Model) -> expr::MirRelationExpr {
        let mut lowerer = Lowerer::new(&model);
        let mut id_gen = IdGen::default();
        expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
            .let_in(&mut id_gen, |id_gen, get_outer| {
                lowerer.apply(model.top_box, get_outer, &ColumnMap::new(), id_gen)
            })
    }
}

/// Maps a column reference to a specific column position.
///
/// This is used for resolving column references when lowering expressions
/// on top of a `MirRelationExpr`, where it maps each column reference with
/// with a column position within the projection of that `MirRelationExpr`.
type ColumnMap = HashMap<ColumnReference, usize>;

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
        id_gen: &mut IdGen,
    ) -> expr::MirRelationExpr {
        use expr::MirRelationExpr as SR;
        let the_box = self.model.get_box(box_id);

        let input = match &the_box.box_type {
            BoxType::Get(Get { id }) => {
                let typ = RelationType::new(
                    the_box
                        .columns
                        .iter()
                        .map(|c| {
                            if let BoxScalarExpr::BaseColumn(BaseColumn { column_type, .. }) =
                                &c.expr
                            {
                                column_type.clone()
                            } else {
                                panic!("expected all columns in Get BoxType to BaseColumn");
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .with_keys(the_box.unique_keys.clone());
                get_outer.product(SR::Get {
                    id: expr::Id::Global(*id),
                    typ,
                })
            }
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
                let correlation_info = the_box.correlation_info();
                if !correlation_info.is_empty() {
                    panic!("correlated joins are not supported yet");
                }

                let outer_arity = get_outer.arity();
                let (mut input, column_map) =
                    self.lower_join(get_outer, outer_column_map, &the_box.quantifiers, id_gen);
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
            BoxType::Grouping(grouping) => {
                // Note: a grouping box must only contain a single quantifier but we can still
                // re-use `lower_join` for single quantifier joins
                let (mut input, column_map) =
                    self.lower_join(get_outer, outer_column_map, &the_box.quantifiers, id_gen);

                // Build the reduction
                let group_key = grouping
                    .key
                    .iter()
                    .map(|k| Self::lower_expression(k, &column_map))
                    .collect_vec();
                let aggregates = the_box
                    .columns
                    .iter()
                    .filter_map(|c| {
                        if let BoxScalarExpr::Aggregate {
                            func,
                            expr,
                            distinct,
                        } = &c.expr
                        {
                            Some(expr::AggregateExpr {
                                func: func.clone(),
                                expr: Self::lower_expression(expr, &column_map),
                                distinct: *distinct,
                            })
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                input = SR::Reduce {
                    input: Box::new(input),
                    group_key,
                    aggregates,
                    monotonic: false,
                    expected_group_size: None,
                };

                // Put the columns in the same order as projected by the Grouping box by
                // adding an additional projection.
                //
                // The aggregate expression are usually projected after the grouping key.
                // However, as a result of query rewrite, aggregate expression may be
                // replaced with grouping key items. For example, the following query:
                //
                //   select a, max(b), max(a) from t group by a;
                //
                // could be rewritten as:
                //
                //   select a, max(b), a from t group by a;
                //
                // resulting in a Grouping box projecting [a, max(b), a]. Even though
                // another query rewrite should remove duplicated columns in the projection
                // of the Grouping box, we should be able to lower any semantically valid
                // query graph.
                let mut aggregate_count = 0;
                let projection = the_box.columns.iter().map(|c| {
                    if let BoxScalarExpr::Aggregate { .. } = &c.expr {
                        let aggregate_pos = grouping.key.len() + aggregate_count;
                        aggregate_count += 1;
                        aggregate_pos
                    } else {
                        grouping
                            .key
                            .iter()
                            .position(|k| c.expr == *k)
                            .expect("expression in the projection of a Grouping box not included in the grouping key")
                    }
                }).collect_vec();
                input.project(projection)
            }
            _ => panic!(),
        };

        if the_box.distinct == DistinctOperation::Enforce {
            input.distinct()
        } else {
            input
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
        id_gen: &mut IdGen,
    ) -> (expr::MirRelationExpr, ColumnMap) {
        if let expr::MirRelationExpr::Get { .. } = &get_outer {
        } else {
            panic!(
                "get_outer: expected a MirRelationExpr::Get, found {:?}",
                get_outer
            );
        }

        let outer_arity = get_outer.arity();

        let join_inputs = quantifiers
            .iter()
            .map(|q_id| self.lower_quantifier(*q_id, get_outer.clone(), outer_column_map, id_gen))
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
            let input_box = self.model.get_quantifier(*q_id).input_box;
            let arity = self.model.get_box(input_box).columns.len();
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

    /// Lowers the given quantifier by applying it to the outer relation.
    fn lower_quantifier(
        &mut self,
        quantifier_id: QuantifierId,
        get_outer: expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
        id_gen: &mut IdGen,
    ) -> expr::MirRelationExpr {
        let quantifier = self.model.get_quantifier(quantifier_id);
        let input_box = quantifier.input_box;
        let mut input = self.apply(input_box, get_outer.clone(), outer_column_map, id_gen);

        match &quantifier.quantifier_type {
            QuantifierType::Foreach => {
                // No special handling required
            }
            QuantifierType::Scalar => {
                // Add the machinery to ensure the lowered plan always produce one row,
                // but one row at most, per outer key.
                let col_type = input.typ().column_types.into_last();

                let outer_arity = get_outer.arity();
                // We must determine a count for each `get_outer` prefix,
                // and report an error if that count exceeds one.
                let guarded = input.let_in(id_gen, |_id_gen, get_select| {
                    // Count for each `get_outer` prefix.
                    let counts = get_select.clone().reduce(
                        (0..outer_arity).collect::<Vec<_>>(),
                        vec![expr::AggregateExpr {
                            func: expr::AggregateFunc::Count,
                            expr: expr::MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
                            distinct: false,
                        }],
                        None,
                    );
                    // Errors should result from counts > 1.
                    let errors = counts
                        .filter(vec![expr::MirScalarExpr::Column(outer_arity).call_binary(
                            expr::MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                            expr::BinaryFunc::Gt,
                        )])
                        .project((0..outer_arity).collect::<Vec<_>>())
                        .map(vec![expr::MirScalarExpr::literal(
                            Err(expr::EvalError::MultipleRowsFromSubquery),
                            col_type.clone().scalar_type,
                        )]);
                    // Return `get_select` and any errors added in.
                    get_select.union(errors)
                });
                // append Null to anything that didn't return any rows
                let default = vec![(Datum::Null, col_type.nullable(true))];
                input = get_outer.lookup(id_gen, guarded, default);
            }
            _ => panic!("Unsupported quantifier type"),
        }

        input
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
            BoxScalarExpr::CallNullary(func) => expr::MirScalarExpr::CallNullary(func.clone()),
            BoxScalarExpr::CallUnary { func, expr } => expr::MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(Self::lower_expression(&*expr, column_map)),
            },
            BoxScalarExpr::CallBinary { func, expr1, expr2 } => expr::MirScalarExpr::CallBinary {
                func: func.clone(),
                expr1: Box::new(Self::lower_expression(expr1, column_map)),
                expr2: Box::new(Self::lower_expression(expr2, column_map)),
            },
            BoxScalarExpr::CallVariadic { func, exprs } => expr::MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: exprs
                    .into_iter()
                    .map(|expr| Self::lower_expression(expr, column_map))
                    .collect::<Vec<_>>(),
            },
            BoxScalarExpr::If { cond, then, els } => expr::MirScalarExpr::If {
                cond: Box::new(Self::lower_expression(cond, column_map)),
                then: Box::new(Self::lower_expression(then, column_map)),
                els: Box::new(Self::lower_expression(els, column_map)),
            },
            _ => panic!("unsupported expression"),
        }
    }
}
