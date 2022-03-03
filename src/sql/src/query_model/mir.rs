// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Converts a Query Graph Model into a [mz_expr::MirRelationExpr].
//!
//! The public interface consists of the implementation of
//! [`Into<mz_expr::MirRelationExpr>`] for [`Model`].

use crate::query_model::model::{
    BaseColumn, BoundRef, BoxId, BoxScalarExpr, BoxType, ColumnReference, DistinctOperation, Get,
    Model, QuantifierId, QuantifierSet, QuantifierType, QueryBox,
};
use itertools::Itertools;
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdGen;
use mz_repr::{Datum, RelationType, ScalarType};
use std::collections::HashMap;

impl From<Model> for mz_expr::MirRelationExpr {
    fn from(model: Model) -> mz_expr::MirRelationExpr {
        let mut lowerer = Lowerer::new(&model);
        let mut id_gen = IdGen::default();
        mz_expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
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
        get_outer: mz_expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
        id_gen: &mut IdGen,
    ) -> mz_expr::MirRelationExpr {
        use mz_expr::MirRelationExpr as SR;
        let the_box = self.model.get_box(box_id);

        let input = match &the_box.box_type {
            BoxType::Get(Get { id, unique_keys }) => {
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
                .with_keys(unique_keys.clone());
                get_outer.product(SR::Get {
                    id: mz_expr::Id::Global(*id),
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
                Self::lower_box_columns(input, &the_box, &column_map, outer_arity, input_arity)
            }
            BoxType::Grouping(grouping) => {
                // Reduce may not contain expressions with correlated subqueries, due to the constraints asserted
                // by the HIR ⇒ QGM conversion (aggregate and key are pushed to a preceding Select box and
                // are always trivial in the Grouping box).

                // In addition, here an empty reduction key signifies that we need to supply default values
                // in the case that there are no results (as in a SQL aggregation without an explicit GROUP BY).

                // Note: a grouping box must only contain a single quantifier but we can still
                // re-use `lower_join` for single quantifier joins
                let (mut input, column_map) = self.lower_join(
                    get_outer.clone(),
                    outer_column_map,
                    &the_box.quantifiers,
                    id_gen,
                );

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
                            Some(mz_expr::AggregateExpr {
                                func: func.clone(),
                                expr: Self::lower_expression(expr, &column_map),
                                distinct: *distinct,
                            })
                        } else {
                            None
                        }
                    })
                    .collect_vec();

                if group_key.is_empty() {
                    // SQL semantics require default values for global aggregates
                    let input_type = input.typ();
                    let default = aggregates
                        .iter()
                        .map(|agg| (agg.func.default(), agg.typ(&input_type).scalar_type))
                        .collect_vec();

                    input = SR::Reduce {
                        input: Box::new(input),
                        group_key,
                        aggregates,
                        monotonic: false,
                        expected_group_size: None,
                    };

                    get_outer.lookup(id_gen, input, default)
                } else {
                    // Put the columns in the same order as projected by the Grouping box by
                    // adding an additional projection.
                    //
                    // The aggregate expressions are usually projected after the grouping key.
                    // However, as a result of query rewrite, aggregate expressions may be
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

                    input = SR::Reduce {
                        input: Box::new(input),
                        group_key,
                        aggregates,
                        monotonic: false,
                        expected_group_size: None,
                    };

                    input.project(projection)
                }
            }
            BoxType::OuterJoin(box_struct) => {
                // TODO: Correlated outer joins are not yet supported.
                let correlation_info = the_box.correlation_info();
                if !correlation_info.is_empty() {
                    panic!("correlated joins are not supported yet");
                }

                let ot = get_outer.typ();
                let oa = ot.arity();

                // An OuterJoin box is similar to a Select box in that the
                // operations join, filter, project happen in that order.
                // However, between the filter and project stage, we union in
                // the rows that should be preserved.

                // 0) Lower the lhs and rhs.
                let mut q_iter = the_box.quantifiers.iter();
                let lhs_id = *q_iter.next().unwrap();
                let rhs_id = *q_iter.next().unwrap();
                let lhs =
                    self.lower_quantifier(lhs_id, get_outer.clone(), outer_column_map, id_gen);
                let lt = lhs.typ().column_types.into_iter().skip(oa).collect_vec();
                let la = lt.len();
                let rhs = self.lower_quantifier(rhs_id, get_outer, outer_column_map, id_gen);
                let rt = rhs.typ().column_types.into_iter().skip(oa).collect_vec();
                let ra = rt.len();

                lhs.let_in(id_gen, |id_gen, get_lhs| {
                    rhs.let_in(id_gen, |id_gen, get_rhs| {
                        // 1) Lower the inner join
                        let (mut inner_join, column_map) = self.lower_join_inner(
                            outer_column_map,
                            oa,
                            vec![(lhs_id, get_lhs.clone()), (rhs_id, get_rhs.clone())],
                        );

                        // 2) Lower the predicates as a filter following the
                        //    join.
                        let lowered_predicates = box_struct
                            .predicates
                            .iter()
                            .map(|p| Self::lower_expression(p, &column_map))
                            .collect_vec();
                        let equijoin_keys = crate::plan::lowering::derive_equijoin_cols(
                            oa,
                            la,
                            ra,
                            lowered_predicates.clone(),
                        );
                        inner_join = inner_join.filter(lowered_predicates.into_iter());

                        // 3) Calculate preserved rows
                        let result = inner_join.let_in(id_gen, |id_gen, get_join| {
                            let mut result = get_join.clone();
                            if let Some((l_keys, r_keys)) = equijoin_keys {
                                // Equijoin case

                                // A collection of keys present in both left and right collections.
                                let both_keys = get_join
                                    .project((0..oa).chain(l_keys.clone()).collect::<Vec<_>>())
                                    .distinct();
                                both_keys.let_in(id_gen, |_id_gen, get_both| {
                                    if self.model.get_quantifier(lhs_id).quantifier_type
                                        == QuantifierType::PreservedForeach
                                    {
                                        // Rows in `left` that are matched in the inner equijoin.
                                        let left_present = mz_expr::MirRelationExpr::join(
                                            vec![get_lhs.clone(), get_both.clone()],
                                            (0..oa)
                                                .chain(l_keys.clone())
                                                .enumerate()
                                                .map(|(i, c)| vec![(0, c), (1, i)])
                                                .collect::<Vec<_>>(),
                                        )
                                        .project((0..(oa + la)).collect());

                                        // Rows in `left` that are not matched in the inner equijoin.
                                        let left_absent =
                                            left_present.negate().union(get_lhs.clone());

                                        // Determine the types of nulls to use as filler.
                                        let right_fill = rt
                                            .into_iter()
                                            .map(|typ| {
                                                mz_expr::MirScalarExpr::literal_null(
                                                    typ.scalar_type,
                                                )
                                            })
                                            .collect();

                                        // Right-fill all left-absent elements with nulls and add them to the result.
                                        result = left_absent.map(right_fill).union(result);
                                    }

                                    if self.model.get_quantifier(rhs_id).quantifier_type
                                        == QuantifierType::PreservedForeach
                                    {
                                        // Rows in `right` that are matched in the inner equijoin.
                                        let right_present = mz_expr::MirRelationExpr::join(
                                            vec![get_rhs.clone(), get_both],
                                            (0..oa)
                                                .chain(r_keys.clone())
                                                .enumerate()
                                                .map(|(i, c)| vec![(0, c), (1, i)])
                                                .collect::<Vec<_>>(),
                                        )
                                        .project((0..(oa + ra)).collect());

                                        // Rows in `left` that are not matched in the inner equijoin.
                                        let right_absent =
                                            right_present.negate().union(get_rhs.clone());

                                        // Determine the types of nulls to use as filler.
                                        let left_fill = lt
                                            .into_iter()
                                            .map(|typ| {
                                                mz_expr::MirScalarExpr::literal_null(
                                                    typ.scalar_type,
                                                )
                                            })
                                            .collect();

                                        // Left-fill all right-absent elements with nulls and add them to the result.
                                        result = right_absent
                                            .map(left_fill)
                                            // Permute left fill before right values.
                                            .project(
                                                (0..oa)
                                                    .chain(oa + ra..oa + ra + la)
                                                    .chain(oa..oa + ra)
                                                    .collect(),
                                            )
                                            .union(result)
                                    }

                                    result
                                })
                            } else {
                                // General join case
                                if self.model.get_quantifier(lhs_id).quantifier_type
                                    == QuantifierType::PreservedForeach
                                {
                                    let left_outer = get_lhs.anti_lookup(
                                        id_gen,
                                        get_join.clone(),
                                        rt.into_iter()
                                            .map(|typ| (Datum::Null, typ.scalar_type))
                                            .collect(),
                                    );
                                    result = result.union(left_outer);
                                }
                                if self.model.get_quantifier(rhs_id).quantifier_type
                                    == QuantifierType::PreservedForeach
                                {
                                    let right_outer = get_rhs
                                        .anti_lookup(
                                            id_gen,
                                            get_join
                                                // need to swap left and right to make the anti_lookup work
                                                .project(
                                                    (0..oa)
                                                        .chain((oa + la)..(oa + la + ra))
                                                        .chain((oa)..(oa + la))
                                                        .collect(),
                                                ),
                                            lt.into_iter()
                                                .map(|typ| (Datum::Null, typ.scalar_type))
                                                .collect(),
                                        )
                                        // swap left and right back again
                                        .project(
                                            (0..oa)
                                                .chain((oa + ra)..(oa + ra + la))
                                                .chain((oa)..(oa + ra))
                                                .collect(),
                                        );
                                    result = result.union(right_outer);
                                }
                                result
                            }
                        });

                        // 4) Lower the project component.
                        Self::lower_box_columns(result, &the_box, &column_map, oa, oa + la + ra)
                    })
                })
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
        get_outer: mz_expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
        quantifiers: &QuantifierSet,
        id_gen: &mut IdGen,
    ) -> (mz_expr::MirRelationExpr, ColumnMap) {
        if let mz_expr::MirRelationExpr::Get { .. } = &get_outer {
        } else {
            panic!(
                "get_outer: expected a MirRelationExpr::Get, found {:?}",
                get_outer
            );
        }

        let outer_arity = get_outer.arity();

        let inputs = quantifiers
            .iter()
            .map(|q_id| {
                (
                    *q_id,
                    self.lower_quantifier(*q_id, get_outer.clone(), outer_column_map, id_gen),
                )
            })
            .collect_vec();

        self.lower_join_inner(outer_column_map, outer_arity, inputs)
    }

    /// Same as `lower_join` except the outer relation has already been applied
    /// to the quantifiers.
    fn lower_join_inner(
        &mut self,
        outer_column_map: &ColumnMap,
        outer_arity: usize,
        inputs: Vec<(QuantifierId, mz_expr::MirRelationExpr)>,
    ) -> (mz_expr::MirRelationExpr, ColumnMap) {
        let (quantifiers, join_inputs): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();
        let (join, input_mapper) = if join_inputs.len() == 1 {
            let only_input = join_inputs.into_iter().next().unwrap();
            let input_mapper = mz_expr::JoinInputMapper::new_from_input_arities(
                [outer_arity, only_input.arity() - outer_arity].into_iter(),
            );
            (only_input, input_mapper)
        } else {
            Self::join_on_prefix(join_inputs, outer_arity)
        };

        // Generate a column map with the projection of the join plus the
        // columns from the outer context.
        let mut column_map = outer_column_map.clone();
        for (index, q_id) in quantifiers.iter().enumerate() {
            for c in input_mapper.global_columns(index + 1) {
                column_map.insert(
                    ColumnReference {
                        quantifier_id: *q_id,
                        position: input_mapper.map_column_to_local(c).0,
                    },
                    c,
                );
            }
        }

        (join, column_map)
    }

    /// Lowers the given quantifier by applying it to the outer relation.
    fn lower_quantifier(
        &mut self,
        quantifier_id: QuantifierId,
        get_outer: mz_expr::MirRelationExpr,
        outer_column_map: &ColumnMap,
        id_gen: &mut IdGen,
    ) -> mz_expr::MirRelationExpr {
        let quantifier = self.model.get_quantifier(quantifier_id);
        let input_box = quantifier.input_box;
        let mut input = self.apply(input_box, get_outer.clone(), outer_column_map, id_gen);

        match &quantifier.quantifier_type {
            QuantifierType::Foreach | QuantifierType::PreservedForeach => {
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
                        vec![mz_expr::AggregateExpr {
                            func: mz_expr::AggregateFunc::Count,
                            expr: mz_expr::MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
                            distinct: false,
                        }],
                        None,
                    );
                    // Errors should result from counts > 1.
                    let errors = counts
                        .filter(vec![mz_expr::MirScalarExpr::Column(outer_arity)
                            .call_binary(
                                mz_expr::MirScalarExpr::literal_ok(
                                    Datum::Int64(1),
                                    ScalarType::Int64,
                                ),
                                mz_expr::BinaryFunc::Gt,
                            )])
                        .project((0..outer_arity).collect::<Vec<_>>())
                        .map_one(mz_expr::MirScalarExpr::literal(
                            Err(mz_expr::EvalError::MultipleRowsFromSubquery),
                            col_type.clone().scalar_type,
                        ));
                    // Return `get_select` and any errors added in.
                    get_select.union(errors)
                });
                // append Null to anything that didn't return any rows
                let default = vec![(Datum::Null, col_type.scalar_type)];
                input = get_outer.lookup(id_gen, guarded, default);
            }
            _ => panic!("Unsupported quantifier type"),
        }

        input
    }

    fn lower_box_columns(
        rel: mz_expr::MirRelationExpr,
        the_box: &BoundRef<'_, QueryBox>,
        column_map: &ColumnMap,
        outer_arity: usize,
        input_arity: usize,
    ) -> mz_expr::MirRelationExpr {
        if let Some(column_refs) = the_box.columns_as_refs() {
            // if all columns projected by this box are references, we only need a projection
            let mut outputs = Vec::<usize>::new();
            outputs.extend(0..outer_arity);
            outputs.extend(column_refs.iter().map(|c| column_map.get(c).unwrap()));
            rel.project(outputs)
        } else if !the_box.columns.is_empty() {
            // otherwise, we need to apply a map to compute the outputs and then project them
            rel.map(
                the_box
                    .columns
                    .iter()
                    .map(|c| Self::lower_expression(&c.expr, &column_map))
                    .collect_vec(),
            )
            .project(
                (0..outer_arity)
                    .chain((input_arity)..(input_arity + the_box.columns.len()))
                    .collect_vec(),
            )
        } else {
            // for boxes without output columns, only project the outer part
            rel.project((0..outer_arity).collect_vec())
        }
    }

    /// Join the given inputs on a shared common prefix.
    ///
    /// TODO(asenac) Given the lack of support for decorrelation at the moment, this
    /// method is always called with `prefix_length` 0, and hence, it remains untested.
    fn join_on_prefix(
        join_inputs: Vec<mz_expr::MirRelationExpr>,
        prefix_length: usize,
    ) -> (mz_expr::MirRelationExpr, mz_expr::JoinInputMapper) {
        let input_mapper = mz_expr::JoinInputMapper::new(&join_inputs);
        // Join on the outer columns
        let equivalences = (0..prefix_length)
            .map(|col| {
                join_inputs
                    .iter()
                    .enumerate()
                    .map(|(input, _)| {
                        mz_expr::MirScalarExpr::Column(
                            input_mapper.map_column_to_global(col, input),
                        )
                    })
                    .collect_vec()
            })
            .collect_vec();
        // Project only one copy of the outer columns
        let projection = (0..prefix_length)
            .chain(
                (0..join_inputs.len())
                    .map(|index| {
                        (prefix_length..input_mapper.input_arity(index))
                            .map(|c| input_mapper.map_column_to_global(c, index))
                            .collect_vec()
                    })
                    .flatten(),
            )
            .collect_vec();
        (
            mz_expr::MirRelationExpr::join_scalars(join_inputs, equivalences).project(projection),
            mz_expr::JoinInputMapper::new_from_input_arities(
                std::iter::once(prefix_length).chain(
                    (0..input_mapper.total_inputs())
                        .into_iter()
                        .map(|i| input_mapper.input_arity(i) - prefix_length),
                ),
            ),
        )
    }

    /// Lowers a scalar expression, resolving the column references using
    /// the supplied column map.
    fn lower_expression(expr: &BoxScalarExpr, column_map: &ColumnMap) -> mz_expr::MirScalarExpr {
        match expr {
            BoxScalarExpr::ColumnReference(c) => {
                mz_expr::MirScalarExpr::Column(*column_map.get(c).unwrap())
            }
            BoxScalarExpr::Literal(row, column_type) => {
                mz_expr::MirScalarExpr::Literal(Ok(row.clone()), column_type.clone())
            }
            BoxScalarExpr::CallNullary(func) => mz_expr::MirScalarExpr::CallNullary(func.clone()),
            BoxScalarExpr::CallUnary { func, expr } => mz_expr::MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(Self::lower_expression(&*expr, column_map)),
            },
            BoxScalarExpr::CallBinary { func, expr1, expr2 } => {
                mz_expr::MirScalarExpr::CallBinary {
                    func: func.clone(),
                    expr1: Box::new(Self::lower_expression(expr1, column_map)),
                    expr2: Box::new(Self::lower_expression(expr2, column_map)),
                }
            }
            BoxScalarExpr::CallVariadic { func, exprs } => mz_expr::MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: exprs
                    .into_iter()
                    .map(|expr| Self::lower_expression(expr, column_map))
                    .collect::<Vec<_>>(),
            },
            BoxScalarExpr::If { cond, then, els } => mz_expr::MirScalarExpr::If {
                cond: Box::new(Self::lower_expression(cond, column_map)),
                then: Box::new(Self::lower_expression(then, column_map)),
                els: Box::new(Self::lower_expression(els, column_map)),
            },
            _ => panic!("unsupported expression"),
        }
    }
}
