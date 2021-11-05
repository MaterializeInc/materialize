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

use crate::plan::expr::{HirScalarExpr, JoinKind};
use crate::query_model::{
    BoxId, BoxScalarExpr, BoxType, Column, ColumnReference, Model, OuterJoin, QuantifierType,
    Select, Values,
};

use crate::plan::expr::HirRelationExpr;

impl From<&HirRelationExpr> for Model {
    fn from(expr: &HirRelationExpr) -> Model {
        FromHir::generate(expr)
    }
}

struct FromHir {
    model: Model,
    /// The stack of context boxes for resolving offset-based column references.
    context_stack: Vec<BoxId>,
}

impl FromHir {
    /// Generates a Query Graph Model for representing the given query.
    fn generate(expr: &HirRelationExpr) -> Model {
        let mut generator = FromHir {
            model: Model::new(),
            context_stack: Vec::new(),
        };
        generator.model.top_box = generator.generate_select(expr);
        generator.model
    }

    /// Generates a sub-graph representing the given expression, ensuring
    /// that the resulting graph starts with a Select box.
    fn generate_select(&mut self, expr: &HirRelationExpr) -> BoxId {
        let mut box_id = self.generate_internal(expr);
        if !self.model.get_box(box_id).borrow().is_select() {
            box_id = self.wrap_within_select(box_id);
        }
        box_id
    }

    /// Generates a sub-graph representing the given expression.
    fn generate_internal(&mut self, expr: &HirRelationExpr) -> BoxId {
        match expr {
            // HirRelationExpr::Get { id, typ } => {
            //     self.model.make_box(BoxType::BaseTable(BaseTable {}))
            // }
            HirRelationExpr::Constant { rows, typ } => {
                assert!(typ.arity() == 0, "expressions are not yet supported",);
                self.model.make_box(BoxType::Values(Values {
                    rows: rows.iter().map(|_| Vec::new()).collect_vec(),
                }))
            }
            HirRelationExpr::Map { input, scalars } => {
                let box_id = self.generate_select(input);
                // @todo self-referencing Maps
                for scalar in scalars.iter() {
                    let expr = self.generate_expr(scalar, box_id);
                    let b = self.model.get_box(box_id);
                    b.borrow_mut().columns.push(Column { expr, alias: None });
                }
                box_id
            }
            HirRelationExpr::Filter { input, predicates } => {
                let input_box = self.generate_internal(input);
                // We could install the predicates in `input_box` if it happened
                // to be a `Select` box. However, that would require pushing down
                // the predicates through its projection, since the predicates are
                // written in terms of elements in `input`'s projection.
                // Instead, we just install a new `Select` box for holding the
                // predicate, and let normalization tranforms simply the graph.
                let select_id = self.wrap_within_select(input_box);
                for predicate in predicates {
                    let expr = self.generate_expr(predicate, select_id);
                    self.add_predicate(select_id, expr);
                }
                select_id
            }

            HirRelationExpr::Project { input, outputs } => {
                let input_box_id = self.generate_internal(input);
                let select_id = self.model.make_select_box();
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Foreach, input_box_id, select_id);
                let mut select_box = self.model.get_box(select_id).borrow_mut();
                for position in outputs {
                    select_box.columns.push(Column {
                        expr: BoxScalarExpr::ColumnReference(ColumnReference {
                            quantifier_id,
                            position: *position,
                        }),
                        alias: None,
                    });
                }
                select_id
            }
            HirRelationExpr::Join {
                left,
                right,
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
                let left_box = self.generate_internal(left);
                self.model.make_quantifier(left_q_type, left_box, join_box);

                // Right box
                let right_box = if kind.is_lateral() {
                    self.within_context(join_box, &mut |generator| -> BoxId {
                        generator.generate_internal(right)
                    })
                } else {
                    self.generate_internal(right)
                };
                self.model
                    .make_quantifier(right_q_type, right_box, join_box);

                // ON clause
                let predicate = self.generate_expr(on, join_box);
                self.add_predicate(join_box, predicate);

                // Default projection
                self.model
                    .get_box(join_box)
                    .borrow_mut()
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
        let mut select_box = self.model.get_box(select_id).borrow_mut();
        select_box.add_all_input_columns(&self.model);
        select_id
    }

    /// Lowers the given expression within the context of the given box.
    ///
    /// Note that this method may add new quantifiers to the box for subquery
    /// expressions.
    fn generate_expr(&mut self, expr: &HirScalarExpr, context_box: BoxId) -> BoxScalarExpr {
        match expr {
            HirScalarExpr::Literal(row, col_type) => {
                BoxScalarExpr::Literal(row.clone(), col_type.clone())
            }
            HirScalarExpr::Column(c) => {
                let context_box = match c.level {
                    0 => context_box,
                    _ => self.context_stack[self.context_stack.len() - c.level],
                };
                BoxScalarExpr::ColumnReference(self.find_column_within_box(context_box, c.column))
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
        let b = self.model.get_box(box_id).borrow();
        for q_id in b.quantifiers.iter() {
            let q = self.model.get_quantifier(*q_id).borrow();
            let ib = self.model.get_box(q.input_box).borrow();
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
        let mut the_box = self.model.get_box(box_id).borrow_mut();
        match &mut the_box.box_type {
            BoxType::Select(select) => select.predicates.push(Box::new(predicate)),
            BoxType::OuterJoin(outer_join) => outer_join.predicates.push(Box::new(predicate)),
            _ => unreachable!(),
        }
    }
}
