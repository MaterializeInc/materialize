// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a Query Graph Model from a [`HirRelationExpr`].
//!
//! The public interface consists of the [`From<HirRelationExpr>`]
//! implementation for [`Result<Model, QGMError>`].

use itertools::Itertools;
use std::collections::HashMap;

use crate::plan::expr::HirRelationExpr;
use crate::plan::expr::{HirScalarExpr, JoinKind};
use crate::query_model::error::{QGMError, UnsupportedHirRelationExpr, UnsupportedHirScalarExpr};
use crate::query_model::model::*;

impl TryFrom<HirRelationExpr> for Model {
    type Error = QGMError;
    fn try_from(expr: HirRelationExpr) -> Result<Self, Self::Error> {
        FromHir::default().generate(expr)
    }
}

#[derive(Default)]
struct FromHir {
    model: Model,
    /// The stack of context boxes for resolving offset-based column references.
    context_stack: Vec<BoxId>,
    /// Track the `BoxId` that represents each HirRelationExpr::Get expression
    /// we have seen so far.
    gets_seen: HashMap<mz_expr::Id, BoxId>,
}

impl FromHir {
    /// Generates a Query Graph Model for representing the given query.
    fn generate(mut self, expr: HirRelationExpr) -> Result<Model, QGMError> {
        self.model.top_box = self.generate_select(expr)?;
        Ok(self.model)
    }

    /// Generates a sub-graph representing the given expression, ensuring
    /// that the resulting graph starts with a Select box.
    fn generate_select(&mut self, expr: HirRelationExpr) -> Result<BoxId, QGMError> {
        let mut box_id = self.generate_internal(expr)?;
        if !self.model.get_box(box_id).is_select() {
            box_id = self.wrap_within_select(box_id);
        }
        Ok(box_id)
    }

    /// Generates a sub-graph representing the given expression.
    fn generate_internal(&mut self, expr: HirRelationExpr) -> Result<BoxId, QGMError> {
        match expr {
            HirRelationExpr::Constant { rows, typ } => {
                if typ.arity() == 0 && rows.len() == 1 {
                    let values_box_id = self.model.make_box(BoxType::Values(Values {
                        rows: rows.iter().map(|_| Vec::new()).collect_vec(),
                    }));
                    Ok(values_box_id)
                } else {
                    // The only expected constant collection is `{ () }` (the
                    // singleton collection of a zero-arity tuple) and is handled above.
                    // In theory, this should be `unreachable!(...)`, but we return an
                    // `Err` in order to bubble up this to the user without panicking.
                    Err(QGMError::from(UnsupportedHirRelationExpr {
                        expr: HirRelationExpr::Constant { rows, typ },
                        explanation: Some(String::from("Constant with arity > 0 or length != 1")),
                    }))
                }
            }
            HirRelationExpr::Get { id, typ } => {
                if let Some(box_id) = self.gets_seen.get(&id) {
                    return Ok(*box_id);
                }
                if let mz_expr::Id::Global(id) = id {
                    let result = self.model.make_box(BoxType::Get(Get {
                        id,
                        unique_keys: typ.keys,
                    }));
                    let mut b = self.model.get_mut_box(result);
                    self.gets_seen.insert(mz_expr::Id::Global(id), result);
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
                    Ok(result)
                } else {
                    // Other id variants should not be present in the HirRelationExpr.
                    // In theory, this should be `unreachable!(...)`, but we return an
                    // `Err` in order to bubble up this to the user without panicking.
                    Err(QGMError::from(UnsupportedHirRelationExpr {
                        expr: HirRelationExpr::Get { id, typ },
                        explanation: Some(String::from("Unexpected Id variant in Get")),
                    }))
                }
            }
            HirRelationExpr::Let {
                name: _,
                id,
                value,
                body,
            } => {
                let id = mz_expr::Id::Local(id);
                let value_box_id = self.generate_internal(*value)?;
                let prev_value = self.gets_seen.insert(id.clone(), value_box_id);
                let body_box_id = self.generate_internal(*body)?;
                if let Some(prev_value) = prev_value {
                    self.gets_seen.insert(id, prev_value);
                } else {
                    self.gets_seen.remove(&id);
                }
                Ok(body_box_id)
            }
            HirRelationExpr::Project { input, outputs } => {
                let input_box_id = self.generate_internal(*input)?;
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
                Ok(select_id)
            }
            HirRelationExpr::Map { input, mut scalars } => {
                let mut box_id = self.generate_internal(*input)?;
                // We could install the predicates in `input_box` if it happened
                // to be a `Select` box. However, that would require pushing down
                // the predicates through its projection, since the predicates are
                // written in terms of elements in `input`'s projection.
                // Instead, we just install a new `Select` box for holding the
                // predicate, and let normalization tranforms simplify the graph.
                box_id = self.wrap_within_select(box_id);

                loop {
                    let old_arity = self.model.get_box(box_id).columns.len();

                    // 1) Find a prefix of scalars such that no scalar in the
                    // current batch depends on columns from the same batch.
                    let end_idx = scalars
                        .iter_mut()
                        .position(|s| {
                            let mut requires_nonexistent_column = false;
                            #[allow(deprecated)]
                            s.visit_columns(0, &mut |depth, col| {
                                if col.level == depth {
                                    requires_nonexistent_column |= (col.column + 1) > old_arity
                                }
                            });
                            requires_nonexistent_column
                        })
                        .unwrap_or(scalars.len());

                    // 2) Add the scalars in the prefix to the box.
                    for scalar in scalars.drain(0..end_idx) {
                        let expr = self.generate_expr(scalar, box_id)?;
                        let mut b = self.model.get_mut_box(box_id);
                        b.add_column(expr);
                    }

                    // 3) If there are scalars remaining, wrap the box so the
                    // remaining scalars can point to the scalars in this prefix.
                    if scalars.is_empty() {
                        break;
                    }
                    box_id = self.wrap_within_select(box_id);
                }
                Ok(box_id)
            }
            HirRelationExpr::CallTable { func, exprs } => {
                // mark the output type of the called function for later
                let output_type = func.output_type();

                // create box with empty function call argument expressions
                let call_table = CallTable::new(func, vec![]);
                let box_id = self.model.make_box(call_table.into());

                // fix function the call arguments (it is a bit awkward that
                // this needs to be adapted only after creating the box)
                let exprs = self.generate_exprs(exprs, box_id)?;
                let mut r#box = self.model.get_mut_box(box_id);
                if let BoxType::CallTable(call_table) = &mut r#box.box_type {
                    call_table.exprs = exprs;
                }

                // add the output of the table as projected columns
                for (position, column_type) in output_type.column_types.into_iter().enumerate() {
                    r#box.add_column(BoxScalarExpr::BaseColumn(BaseColumn {
                        position,
                        column_type,
                    }));
                }

                Ok(box_id)
            }
            HirRelationExpr::Filter { input, predicates } => {
                let input_box = self.generate_internal(*input)?;
                // We could install the predicates in `input_box` if it happened
                // to be a `Select` box. However, that would require pushing down
                // the predicates through its projection, since the predicates are
                // written in terms of elements in `input`'s projection.
                // Instead, we just install a new `Select` box for holding the
                // predicate, and let normalization tranforms simplify the graph.
                let select_id = self.wrap_within_select(input_box);
                for predicate in predicates {
                    let expr = self.generate_expr(predicate, select_id)?;
                    self.add_predicate(select_id, expr);
                }
                Ok(select_id)
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
                let left_box = self.generate_internal(*left)?;
                self.model.make_quantifier(left_q_type, left_box, join_box);

                // Right box
                let right_box = self.within_context(join_box, &mut |generator| {
                    let right = right.take();
                    generator.generate_internal(right)
                })?;
                self.model
                    .make_quantifier(right_q_type, right_box, join_box);

                // ON clause
                let predicate = self.generate_expr(on, join_box)?;
                self.add_predicate(join_box, predicate);

                // Default projection
                self.model.get_mut_box(join_box).add_all_input_columns();

                Ok(join_box)
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
                let input_box_id = self.generate_internal(*input)?;
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
                    let input_expr = self.generate_expr(*aggregate.expr, select_id)?;
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
                Ok(group_box_id)
            }
            HirRelationExpr::Distinct { input } => {
                let select_id = self.generate_select(*input)?;
                let mut select_box = self.model.get_mut_box(select_id);
                select_box.distinct = DistinctOperation::Enforce;
                Ok(select_id)
            }
            // HirRelationExpr::TopK {
            //     input,
            //     group_key,
            //     order_key,
            //     limit,
            //     offset,
            // } => todo!(),
            // HirRelationExpr::Negate { input } => todo!(),
            // HirRelationExpr::Threshold { input } => todo!(),
            HirRelationExpr::Union { base, inputs } => {
                use QuantifierType::Foreach;

                // recurse to inputs
                let mut input_ids = vec![self.generate_internal(*base)?];
                for input in inputs {
                    input_ids.push(self.generate_internal(input)?);
                }

                // create Union box and connect inputs
                let union_id = self.model.make_box(BoxType::Union);
                let quant_ids = input_ids
                    .iter()
                    .map(|input_id| self.model.make_quantifier(Foreach, *input_id, union_id))
                    .collect::<Vec<_>>();

                // project all columns from the first input (convention-based constraint)
                let columns_len = self.model.get_box(input_ids[0]).columns.len();
                let mut union_box = self.model.get_mut_box(union_id);
                for n in 0..columns_len {
                    union_box.add_column(BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: quant_ids[0],
                        position: n,
                    }));
                }

                Ok(union_id)
            }
            expr => Err(QGMError::from(UnsupportedHirRelationExpr {
                expr,
                explanation: None,
            })),
        }
    }

    /// Returns a Select box ranging over the given box, projecting
    /// all of its columns.
    fn wrap_within_select(&mut self, box_id: BoxId) -> BoxId {
        let select_id = self.model.make_select_box();
        self.model
            .make_quantifier(QuantifierType::Foreach, box_id, select_id);
        self.model.get_mut_box(select_id).add_all_input_columns();
        select_id
    }

    /// Lowers the given expression within the context of the given box.
    ///
    /// Note that this method may add new quantifiers to the box for subquery
    /// expressions.
    fn generate_expr(
        &mut self,
        expr: HirScalarExpr,
        context_box: BoxId,
    ) -> Result<BoxScalarExpr, QGMError> {
        match expr {
            HirScalarExpr::Literal(row, col_type) => Ok(BoxScalarExpr::Literal(row, col_type)),
            HirScalarExpr::Column(c) => {
                let context_box = match c.level {
                    0 => context_box,
                    _ => self.context_stack[self.context_stack.len() - c.level],
                };
                Ok(BoxScalarExpr::ColumnReference(
                    self.find_column_within_box(context_box, c.column),
                ))
            }
            HirScalarExpr::CallUnmaterializable(func) => {
                Ok(BoxScalarExpr::CallUnmaterializable(func))
            }
            HirScalarExpr::CallUnary { func, expr } => Ok(BoxScalarExpr::CallUnary {
                func,
                expr: Box::new(self.generate_expr(*expr, context_box)?),
            }),
            HirScalarExpr::CallBinary { func, expr1, expr2 } => Ok(BoxScalarExpr::CallBinary {
                func,
                expr1: Box::new(self.generate_expr(*expr1, context_box)?),
                expr2: Box::new(self.generate_expr(*expr2, context_box)?),
            }),
            HirScalarExpr::CallVariadic { func, exprs } => Ok(BoxScalarExpr::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| self.generate_expr(expr, context_box))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            HirScalarExpr::If { cond, then, els } => Ok(BoxScalarExpr::If {
                cond: Box::new(self.generate_expr(*cond, context_box)?),
                then: Box::new(self.generate_expr(*then, context_box)?),
                els: Box::new(self.generate_expr(*els, context_box)?),
            }),
            HirScalarExpr::Select(mut expr) => {
                let box_id = self.within_context(context_box, &mut move |generator| {
                    generator.generate_select(expr.take())
                })?;
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Scalar, box_id, context_box);
                Ok(BoxScalarExpr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: 0,
                }))
            }
            HirScalarExpr::Exists(mut expr) => {
                let box_id = self.within_context(context_box, &mut move |generator| {
                    generator.generate_select(expr.take())
                })?;
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Existential, box_id, context_box);
                Ok(BoxScalarExpr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: 0,
                }))
            }
            scalar @ HirScalarExpr::Windowing(..) => {
                Err(QGMError::from(UnsupportedHirScalarExpr { scalar }))
            }
            // A Parameter should never be part of the input expression, see
            // `expr.bind_parameters(&params)?;` calls in `dml::plan_*` and `ddl:plan_*`.
            // In theory, this should be `unreachable!(...)`, but we return an
            // `Err` in order to bubble up this to the user without panicking.
            scalar @ HirScalarExpr::Parameter(..) => {
                Err(QGMError::from(UnsupportedHirScalarExpr { scalar }))
            }
        }
    }

    /// Lowers the given expressions within the context of the given box.
    ///
    /// Delegates to [`FromHir::generate_expr`] for each element.
    fn generate_exprs(
        &mut self,
        exprs: Vec<HirScalarExpr>,
        context_box: BoxId,
    ) -> Result<Vec<BoxScalarExpr>, QGMError> {
        exprs
            .into_iter()
            .map(|expr| self.generate_expr(expr, context_box))
            .collect()
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
