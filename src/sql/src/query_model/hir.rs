// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a Query Graph Model from a [HirRelationExpr].

use itertools::Itertools;
use std::collections::{BTreeSet, HashMap};

use crate::plan::expr::{HirScalarExpr, JoinKind};
use crate::query_model::{
    BaseColumn, BoxId, BoxScalarExpr, BoxType, Column, ColumnReference, DistinctOperation, Get,
    Grouping, Model, OuterJoin, QuantifierType, Select, Values,
};

use crate::plan::expr::HirRelationExpr;

impl From<HirRelationExpr> for Model {
    fn from(expr: HirRelationExpr) -> Model {
        FromHir::generate(expr)
    }
}

struct FromHir {
    model: Model,
    /// The stack of context boxes for resolving offset-based column references.
    context_stack: Vec<BoxId>,
    /// Track the `BoxId` that represents each HirRelationExpr::Get expression
    /// we have seen so far.
    gets_seen: HashMap<expr::Id, BoxId>,
}

impl FromHir {
    /// Generates a Query Graph Model for representing the given query.
    fn generate(expr: HirRelationExpr) -> Model {
        let mut generator = FromHir {
            model: Model::new(),
            context_stack: Vec::new(),
            gets_seen: HashMap::new(),
        };
        generator.model.top_box = generator.generate_select(expr);
        fixup::fixup_model(&mut generator.model);
        generator.model
    }

    /// Generates a sub-graph representing the given expression, ensuring
    /// that the resulting graph starts with a Select box.
    fn generate_select(&mut self, expr: HirRelationExpr) -> BoxId {
        let mut box_id = self.generate_internal(expr);
        if !self.model.get_box(box_id).is_select() {
            box_id = self.wrap_within_select(box_id);
        }
        box_id
    }

    /// Generates a sub-graph representing the given expression.
    fn generate_internal(&mut self, expr: HirRelationExpr) -> BoxId {
        match expr {
            HirRelationExpr::Get { id, mut typ } => {
                if let Some(box_id) = self.gets_seen.get(&id) {
                    return *box_id;
                }
                if let expr::Id::Global(id) = id {
                    let result = self.model.make_box(BoxType::Get(Get { id }));
                    let mut b = self.model.get_mut_box(result);
                    self.gets_seen.insert(expr::Id::Global(id), result);
                    b.unique_keys.append(&mut typ.keys);
                    b.columns
                        .extend(typ.column_types.into_iter().enumerate().map(
                            |(position, column_type)| Column {
                                expr: BoxScalarExpr::BaseColumn(BaseColumn {
                                    position,
                                    column_type,
                                }),
                                alias: None,
                            },
                        ));
                    result
                } else {
                    // Other id variants should not be present in the
                    // HirRelationExpr.
                    panic!("unsupported get id {:?}", id);
                }
            }
            HirRelationExpr::Let {
                name: _,
                id,
                value,
                body,
            } => {
                let id = expr::Id::Local(id);
                let value_box = self.generate_internal(*value);
                let prev_value = self.gets_seen.insert(id.clone(), value_box);
                let body_box = self.generate_internal(*body);
                if let Some(prev_value) = prev_value {
                    self.gets_seen.insert(id, prev_value);
                } else {
                    self.gets_seen.remove(&id);
                }
                body_box
            }
            HirRelationExpr::Constant { rows, typ } => {
                assert!(typ.arity() == 0, "expressions are not yet supported",);
                self.model.make_box(BoxType::Values(Values {
                    rows: rows.iter().map(|_| Vec::new()).collect_vec(),
                }))
            }
            HirRelationExpr::Map { input, mut scalars } => {
                let mut box_id = self.generate_internal(*input);
                // We could install the predicates in `input_box` if it happened
                // to be a `Select` box. However, that would require pushing down
                // the predicates through its projection, since the predicates are
                // written in terms of elements in `input`'s projection.
                // Instead, we just install a new `Select` box for holding the
                // predicate, and let normalization tranforms simplify the graph.
                box_id = self.wrap_within_select(box_id);

                loop {
                    let old_arity = self.model.get_box(box_id).columns.len();

                    // 1) Find a batch of scalars such that no scalar in the
                    // current batch depends on columns from the same batch.
                    let end_idx = scalars
                        .iter_mut()
                        .position(|s| {
                            let mut requires_nonexistent_column = false;
                            s.visit_columns(0, &mut |depth, col| {
                                if col.level == depth {
                                    requires_nonexistent_column |= (col.column + 1) > old_arity
                                }
                            });
                            requires_nonexistent_column
                        })
                        .unwrap_or(scalars.len());

                    // 2) Add the scalars in the batch to the box.
                    for scalar in scalars.drain(0..end_idx) {
                        let expr = self.generate_expr(scalar, box_id);
                        let mut b = self.model.get_mut_box(box_id);
                        b.add_column(expr);
                    }

                    // 3) If there are scalars remaining, wrap the box so the
                    // remaining scalars can point to the scalars in this batch.
                    if scalars.is_empty() {
                        break;
                    }
                    box_id = self.wrap_within_select(box_id);
                }
                box_id
            }
            HirRelationExpr::Distinct { input } => {
                let select_id = self.generate_select(*input);
                let mut select_box = self.model.get_mut_box(select_id);
                select_box.distinct = DistinctOperation::Enforce;
                select_id
            }
            HirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                expected_group_size: _,
            } => {
                // An intermediate Select Box is generated between the input box and
                // the resulting grouping box so that the grouping key and the arguments
                // of the aggregations contain only column references
                let input_box_id = self.generate_internal(*input);
                let select_id = self.model.make_select_box();
                let input_q_id =
                    self.model
                        .make_quantifier(QuantifierType::Foreach, input_box_id, select_id);
                let group_box_id = self
                    .model
                    .make_box(BoxType::Grouping(Grouping { key: Vec::new() }));
                let select_q_id =
                    self.model
                        .make_quantifier(QuantifierType::Foreach, select_id, group_box_id);
                let mut key = Vec::new();
                for k in group_key.into_iter() {
                    // Make sure the input select box projects the source column needed
                    let input_col_ref = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: input_q_id,
                        position: k,
                    });
                    let position = self
                        .model
                        .get_mut_box(select_id)
                        .add_column_if_not_exists(input_col_ref);
                    // Reference of the column projected by the input select box
                    let select_box_col_ref = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: select_q_id,
                        position,
                    });
                    // Add it to the grouping key and to the projection of the
                    // Grouping box
                    key.push(select_box_col_ref.clone());
                    self.model
                        .get_mut_box(group_box_id)
                        .add_column(select_box_col_ref);
                }
                for aggregate in aggregates.into_iter() {
                    // Any computed expression passed as an argument of an aggregate
                    // function is computed by the input select box.
                    let input_expr = self.generate_expr(*aggregate.expr, select_id);
                    let position = self
                        .model
                        .get_mut_box(select_id)
                        .add_column_if_not_exists(input_expr);
                    // Reference of the column projected by the input select box
                    let col_ref = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: select_q_id,
                        position,
                    });
                    // Add the aggregate expression to the projection of the Grouping
                    // box
                    let aggregate = BoxScalarExpr::Aggregate {
                        func: aggregate.func.into_expr(),
                        expr: Box::new(col_ref),
                        distinct: aggregate.distinct,
                    };
                    self.model.get_mut_box(group_box_id).add_column(aggregate);
                }

                // Update the key of the grouping box
                if let BoxType::Grouping(g) = &mut self.model.get_mut_box(group_box_id).box_type {
                    g.key.extend(key);
                }
                group_box_id
            }
            HirRelationExpr::Filter { input, predicates } => {
                let input_box = self.generate_internal(*input);
                // We could install the predicates in `input_box` if it happened
                // to be a `Select` box. However, that would require pushing down
                // the predicates through its projection, since the predicates are
                // written in terms of elements in `input`'s projection.
                // Instead, we just install a new `Select` box for holding the
                // predicate, and let normalization tranforms simplify the graph.
                let select_id = self.wrap_within_select(input_box);
                for predicate in predicates {
                    let expr = self.generate_expr(predicate, select_id);
                    self.add_predicate(select_id, expr);
                }
                select_id
            }

            HirRelationExpr::Project { input, outputs } => {
                let input_box_id = self.generate_internal(*input);
                let select_id = self.model.make_select_box();
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Foreach, input_box_id, select_id);
                let mut select_box = self.model.get_mut_box(select_id);
                for position in outputs {
                    select_box.add_column(BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id,
                        position,
                    }));
                }
                select_id
            }
            HirRelationExpr::Join {
                left,
                mut right,
                on,
                kind,
            } => {
                let (box_type, left_q_type, right_q_type) = match kind {
                    JoinKind::Inner { .. } => (
                        BoxType::Select(Select::default()),
                        QuantifierType::Foreach,
                        QuantifierType::Foreach,
                    ),
                    JoinKind::LeftOuter { .. } => (
                        BoxType::OuterJoin(OuterJoin::default()),
                        QuantifierType::PreservedForeach,
                        QuantifierType::Foreach,
                    ),
                    JoinKind::RightOuter => (
                        BoxType::OuterJoin(OuterJoin::default()),
                        QuantifierType::Foreach,
                        QuantifierType::PreservedForeach,
                    ),
                    JoinKind::FullOuter => (
                        BoxType::OuterJoin(OuterJoin::default()),
                        QuantifierType::PreservedForeach,
                        QuantifierType::PreservedForeach,
                    ),
                };
                let join_box = self.model.make_box(box_type);

                // Left box
                let left_box = self.generate_internal(*left);
                self.model.make_quantifier(left_q_type, left_box, join_box);

                // Right box
                let right_box = self.within_context(join_box, &mut |generator| -> BoxId {
                    let right = right.take();
                    generator.generate_internal(right)
                });
                self.model
                    .make_quantifier(right_q_type, right_box, join_box);

                // ON clause
                let predicate = self.generate_expr(on, join_box);
                self.add_predicate(join_box, predicate);

                // Default projection
                self.model
                    .get_mut_box(join_box)
                    .add_all_input_columns(&self.model);

                join_box
            }

            _ => panic!("unsupported expression type {:?}", expr),
        }
    }

    /// Returns a Select box ranging over the given box, projecting
    /// all of its columns.
    fn wrap_within_select(&mut self, box_id: BoxId) -> BoxId {
        let select_id = self.model.make_select_box();
        self.model
            .make_quantifier(QuantifierType::Foreach, box_id, select_id);
        let mut select_box = self.model.get_mut_box(select_id);
        select_box.add_all_input_columns(&self.model);
        select_id
    }

    /// Lowers the given expression within the context of the given box.
    ///
    /// Note that this method may add new quantifiers to the box for subquery
    /// expressions.
    fn generate_expr(&mut self, expr: HirScalarExpr, context_box: BoxId) -> BoxScalarExpr {
        match expr {
            HirScalarExpr::Literal(row, col_type) => BoxScalarExpr::Literal(row, col_type),
            HirScalarExpr::Column(c) => {
                let context_box = match c.level {
                    0 => context_box,
                    _ => self.context_stack[self.context_stack.len() - c.level],
                };
                BoxScalarExpr::ColumnReference(self.find_column_within_box(context_box, c.column))
            }
            HirScalarExpr::CallNullary(func) => BoxScalarExpr::CallNullary(func),
            HirScalarExpr::CallUnary { func, expr } => BoxScalarExpr::CallUnary {
                func,
                expr: Box::new(self.generate_expr(*expr, context_box)),
            },
            HirScalarExpr::CallBinary { func, expr1, expr2 } => BoxScalarExpr::CallBinary {
                func,
                expr1: Box::new(self.generate_expr(*expr1, context_box)),
                expr2: Box::new(self.generate_expr(*expr2, context_box)),
            },
            HirScalarExpr::CallVariadic { func, exprs } => BoxScalarExpr::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| self.generate_expr(expr, context_box))
                    .collect::<Vec<_>>(),
            },
            HirScalarExpr::If { cond, then, els } => BoxScalarExpr::If {
                cond: Box::new(self.generate_expr(*cond, context_box)),
                then: Box::new(self.generate_expr(*then, context_box)),
                els: Box::new(self.generate_expr(*els, context_box)),
            },
            HirScalarExpr::Select(mut expr) => {
                let box_id = self.within_context(context_box, &mut move |generator| -> BoxId {
                    generator.generate_select(expr.take())
                });
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Scalar, box_id, context_box);
                BoxScalarExpr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: 0,
                })
            }
            HirScalarExpr::Windowing(expr) => {
                let func = match expr.func {
                    crate::plan::expr::WindowExprType::Scalar(scalar) => {
                        // Note: the order_by information in `scalar.order_by` is redundant.
                        // We only need to preserve here the expressions the input will be
                        // sorted by, not the columns that will contain the result of evaluating
                        // those expressions.
                        crate::query_model::scalar_expr::WindowExprType::Scalar(scalar.func)
                    }
                };
                BoxScalarExpr::Windowing(crate::query_model::WindowExpr {
                    func,
                    partition: expr
                        .partition
                        .into_iter()
                        .map(|e| self.generate_expr(e, context_box))
                        .collect(),
                    order_by: expr
                        .order_by
                        .into_iter()
                        .map(|e| self.generate_expr(e, context_box))
                        .collect(),
                })
            }
            _ => panic!("unsupported expression type {:?}", expr),
        }
    }

    /// Find the N-th column among the columns projected by the input quantifiers
    /// of the given box. This method translates Hir's offset-based column into
    /// quantifier-based column references.
    ///
    /// This method is equivalent to `expr::JoinInputMapper::map_column_to_local`, in
    /// the sense that given all the columns projected by a join (represented by the
    /// set of input quantifiers of the given box) it returns the input the column
    /// belongs to and its offset within the projection of the underlying operator.
    fn find_column_within_box(&self, box_id: BoxId, mut position: usize) -> ColumnReference {
        let b = self.model.get_box(box_id);
        for q_id in b.quantifiers.iter() {
            let q = self.model.get_quantifier(*q_id);
            let ib = self.model.get_box(q.input_box);
            if position < ib.columns.len() {
                return ColumnReference {
                    quantifier_id: *q_id,
                    position,
                };
            }
            position -= ib.columns.len();
        }
        unreachable!("column not found")
    }

    /// Executes the given action within the context of the given box.
    fn within_context<F, T>(&mut self, context_box: BoxId, f: &mut F) -> T
    where
        F: FnMut(&mut Self) -> T,
    {
        self.context_stack.push(context_box);
        let result = f(self);
        self.context_stack.pop();
        result
    }

    /// Adds the given predicate to the given box.
    ///
    /// The given box must support predicates, ie. it must be either a Select box
    /// or an OuterJoin one.
    fn add_predicate(&mut self, box_id: BoxId, predicate: BoxScalarExpr) {
        let mut the_box = self.model.get_mut_box(box_id);
        match &mut the_box.box_type {
            BoxType::Select(select) => select.predicates.push(predicate),
            BoxType::OuterJoin(outer_join) => outer_join.predicates.push(predicate),
            _ => unreachable!(),
        }
    }
}

/// Special transformations to fix semantically invalid constructions allowed
/// temporarily for the sake of keeping the generator above as simple as possible.
mod fixup {

    use super::*;

    pub(crate) fn fixup_model(model: &mut Model) {
        let mut windowing_fixup = Windowing::new();

        let boxes = model.boxes.keys().cloned().collect_vec();
        for box_id in boxes {
            if windowing_fixup.condition(model, box_id) {
                windowing_fixup.action(model, box_id);
            }
        }
    }

    struct Windowing {
        /// Window function expression found in a select box that
        /// must be pushed down to a Windowing box
        to_pushdown: BTreeSet<BoxScalarExpr>,
    }

    impl Windowing {
        fn new() -> Self {
            Self {
                to_pushdown: BTreeSet::new(),
            }
        }

        /// Returns true if the given box is a Select box containing Windowing expressions.
        fn condition(&mut self, model: &Model, box_id: BoxId) -> bool {
            self.to_pushdown.clear();
            let b = model.get_box(box_id);
            if b.is_select() {
                let _ = b.visit_expressions(&mut |expr: &BoxScalarExpr| -> Result<(), ()> {
                    expr.visit(&mut |expr: &BoxScalarExpr| {
                        if let BoxScalarExpr::Windowing(_) = expr {
                            self.to_pushdown.insert(expr.clone());
                        }
                    });
                    Ok(())
                });

                !self.to_pushdown.is_empty()
            } else {
                false
            }
        }

        /// Pushes down the windowing expressions in the given Select box into
        /// a Windowing box that will be the only input of the Select box. Since
        /// Windowing must happen after the Join, all the input quantifiers of
        /// the given Select box are pushed down into a new Select box that
        /// becomes the only input of the Windowing box.
        ///
        /// The Windowing box forwards all the columns from its input and adds
        /// new columns for the computation of the windowing expressions. The
        /// windowing expressions in the original Select box are then replaced
        /// with column references to the corresponding column in the Windowing
        /// box. Also, all the column references in the original Select box
        /// are updated to make them point to the columns forwarded by the
        /// new Windowing box.
        //
        // Input sub-graph:
        //
        // +-----------------------------------------+
        // | Box0: Select with Windowing expressions |
        // |                                         |
        // | +----+   +----+                         |
        // | | Q1 |   | Q2 | ...                     |
        // | +----+   +----+                         |
        // |    |        |                           |
        // +----|--------|---------------------------+
        //      |        |
        //   +------+ +------+
        //   | BoxA | | BoxB |
        //   +------+ +------+
        //
        // Output sub-graph:
        //
        // +--------------+
        // | Box0: Select |
        // |              |
        // | +----+       |
        // | | QA |       |
        // | +----+       |
        // |    |         |
        // +----|---------+
        //      |
        // +-----------------+
        // | BoxC: Windowing |
        // |                 |
        // | +----+          |
        // | | QB |          |
        // | +----+          |
        // |    |            |
        // +----|------------+
        //      |
        // +---------------------+
        // | BoxD: Select        |
        // |                     |
        // | +----+   +----+     |
        // | | Q1 |   | Q2 | ... |
        // | +----+   +----+     |
        // |    |        |       |
        // +----|--------|-------+
        //      |        |
        //   +------+ +------+
        //   | BoxA | | BoxB |
        //   +------+ +------+
        fn action(&mut self, model: &mut Model, box_id: BoxId) {
            let new_select_box = model.make_select_box();
            model.swap_quantifiers(box_id, new_select_box);
            model
                .get_mut_box(new_select_box)
                .add_all_input_columns(model);

            let expr_map = model
                .get_box(new_select_box)
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| (c.expr.clone(), i))
                .collect::<HashMap<BoxScalarExpr, usize>>();

            let windowing_box = model.make_box(BoxType::Windowing);
            let windowing_input_q =
                model.make_quantifier(QuantifierType::Foreach, new_select_box, windowing_box);

            // Fill the windowing box
            {
                let mut windowing_box = model.get_mut_box(windowing_box);
                windowing_box.add_all_input_columns(model);
                for mut window_expr in self.to_pushdown.iter().cloned() {
                    // Update any parameters the window function may have
                    window_expr.visit_mut(&mut |expr: &mut BoxScalarExpr| {
                        if let Some(position) = expr_map.get(expr) {
                            *expr = BoxScalarExpr::ColumnReference(ColumnReference {
                                quantifier_id: windowing_input_q,
                                position: *position,
                            });
                        }
                    });
                    windowing_box.add_column(window_expr);
                }
            }

            let windowing_output_q =
                model.make_quantifier(QuantifierType::Foreach, windowing_box, box_id);

            // Update all the expression in the box to point to columns projected by the
            // windowing box
            // 1) convert the window functions into column references
            let first_window_expr = expr_map.len();
            let window_function_map = self
                .to_pushdown
                .iter()
                .cloned()
                .zip(first_window_expr..)
                .collect::<HashMap<_, _>>();
            let _ = model.get_mut_box(box_id).visit_expressions_mut(
                &mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
                    expr.visit_mut(&mut |expr: &mut BoxScalarExpr| {
                        if let Some(position) = window_function_map.get(expr) {
                            *expr = BoxScalarExpr::ColumnReference(ColumnReference {
                                quantifier_id: windowing_output_q,
                                position: *position,
                            });
                        }
                    });
                    Ok(())
                },
            );
            // 2) replace column references to make them point to the new quantifier
            // Note: the 1) and 2) could be fused if we had a `visit_pre_mut` method.
            // Otherwise, any column reference if the parameters of a window function
            // will be updated before the window function itself is converted into
            // a column reference.
            let _ = model.get_mut_box(box_id).visit_expressions_mut(
                &mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
                    expr.visit_mut(&mut |expr: &mut BoxScalarExpr| {
                        if let Some(position) = expr_map.get(expr) {
                            *expr = BoxScalarExpr::ColumnReference(ColumnReference {
                                quantifier_id: windowing_output_q,
                                position: *position,
                            });
                        }
                    });
                    Ok(())
                },
            );
        }
    }
}
