// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;

use crate::plan::expr::{HirRelationExpr, HirScalarExpr, JoinKind};
use crate::query_model::{
    BoxId, BoxType, Column, ColumnReference, DistinctOperation, Expr, Model, OuterJoin,
    QuantifierType, Select, Values,
};

pub struct FromHir {
    /// The model being built.
    model: Model,
    /// The stack of context boxes for resolving offset-based column references.
    context_stack: Vec<BoxId>,
}

impl FromHir {
    /// Generates a Query Graph Model for representing the given query.
    pub fn generate(expr: &HirRelationExpr) -> Model {
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

    fn generate_internal(&mut self, expr: &HirRelationExpr) -> BoxId {
        match expr {
            // HirRelationExpr::Get { id, typ } => {
            //     self.model.make_box(BoxType::BaseTable(BaseTable {}))
            // }
            HirRelationExpr::Constant { rows, typ } => {
                if typ.arity() != 0 {
                    panic!("expressions are not yet supported");
                }
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
            HirRelationExpr::Project { input, outputs } => {
                let input_box_id = self.generate_internal(input);
                let select_id = self.model.make_select_box();
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Foreach, input_box_id, select_id);
                let mut select_box = self.model.get_box(select_id).borrow_mut();
                for position in outputs {
                    select_box.columns.push(Column {
                        expr: Expr::ColumnReference(ColumnReference {
                            quantifier_id,
                            position: *position,
                        }),
                        alias: None,
                    });
                }
                select_id
            }
            HirRelationExpr::Distinct { input } => {
                let select_id = self.generate_select(input);
                let mut select_box = self.model.get_box(select_id).borrow_mut();
                select_box.distinct = DistinctOperation::Enforce;
                select_id
            }
            HirRelationExpr::Filter { input, predicates } => {
                let input_box = self.generate_internal(input);
                // Wrap the input within a select so the referenced columns are
                // in the expected order
                let select_id = self.wrap_within_select(input_box);
                for predicate in predicates {
                    let expr = self.generate_expr(predicate, select_id);
                    let select_box = self.model.get_box(select_id);
                    select_box.borrow_mut().add_predicate(Box::new(expr));
                }
                select_id
            }
            HirRelationExpr::Join {
                left,
                right,
                on,
                kind,
            } => {
                let box_type = if matches!(
                    kind,
                    JoinKind::LeftOuter { .. } | JoinKind::RightOuter { .. } | JoinKind::FullOuter
                ) {
                    BoxType::OuterJoin(OuterJoin::new())
                } else {
                    BoxType::Select(Select::new())
                };
                let join_box = self.model.make_box(box_type);

                // Left box
                let left_box = self.generate_internal(left);
                let left_q_type = if matches!(kind, JoinKind::LeftOuter { .. }) {
                    QuantifierType::PreservedForeach
                } else {
                    QuantifierType::Foreach
                };

                let _ = self.model.make_quantifier(left_q_type, left_box, join_box);

                // Right box
                let right_box = if kind.is_lateral() {
                    self.within_context(join_box, &mut |generator| -> BoxId {
                        generator.generate_internal(right)
                    })
                } else {
                    self.generate_internal(right)
                };

                let right_q_type = if matches!(kind, JoinKind::RightOuter { .. }) {
                    QuantifierType::PreservedForeach
                } else {
                    QuantifierType::Foreach
                };

                let _ = self
                    .model
                    .make_quantifier(right_q_type, right_box, join_box);

                // ON clause
                let predicate = self.generate_expr(on, join_box);
                self.model
                    .get_box(join_box)
                    .borrow_mut()
                    .add_predicate(Box::new(predicate));

                // Default projection
                self.model
                    .get_box(join_box)
                    .borrow_mut()
                    .add_all_input_columns(&self.model);

                join_box
            }
            _ => panic!("unsupported expression type"),
        }
    }

    /// Returns a Select box ranging over the given box, projecting
    /// all of its columns.
    fn wrap_within_select(&mut self, box_id: BoxId) -> BoxId {
        let select_id = self.model.make_select_box();
        let _ = self
            .model
            .make_quantifier(QuantifierType::Foreach, box_id, select_id);
        let mut select_box = self.model.get_box(select_id).borrow_mut();
        select_box.add_all_input_columns(&self.model);
        select_id
    }

    fn generate_expr(&mut self, expr: &HirScalarExpr, context_box: BoxId) -> Expr {
        match expr {
            HirScalarExpr::Literal(row, col_type) => Expr::Literal(row.clone(), col_type.clone()),
            HirScalarExpr::Column(c) => {
                let context_box = if c.level == 0 {
                    context_box
                } else {
                    self.context_stack[self.context_stack.len() - c.level]
                };
                Expr::ColumnReference(self.find_column_within_box(context_box, c.column))
            }
            HirScalarExpr::CallBinary { func, expr1, expr2 } => Expr::CallBinary {
                func: func.clone(),
                expr1: Box::new(self.generate_expr(expr1, context_box)),
                expr2: Box::new(self.generate_expr(expr2, context_box)),
            },
            HirScalarExpr::Select(expr) => {
                let box_id = self.within_context(context_box, &mut |generator| -> BoxId {
                    generator.generate_select(expr)
                });
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Scalar, box_id, context_box);
                Expr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: 0,
                })
            }

            HirScalarExpr::Exists(expr) => {
                // EXISTS(...) subqueries are represented as TRUE IN (SELECT TRUE ...)
                let box_id = self.within_context(context_box, &mut |generator| -> BoxId {
                    generator.generate_internal(expr)
                });
                let literal_true = self.generate_expr(&HirScalarExpr::literal_true(), context_box);

                // The SELECT box wrapping the EXISTS subquery
                let select_id = self.model.make_select_box();
                let _ = self
                    .model
                    .make_quantifier(QuantifierType::Foreach, box_id, select_id);
                self.model
                    .get_box(select_id)
                    .borrow_mut()
                    .columns
                    .push(Column {
                        expr: literal_true.clone(),
                        alias: None,
                    });

                // Add the existential (IN) quantifier in the context box
                let quantifier_id =
                    self.model
                        .make_quantifier(QuantifierType::Existential, select_id, context_box);

                // Return the existential comparison
                let col_ref = Expr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: 0,
                });
                Expr::CallBinary {
                    func: expr::BinaryFunc::Eq,
                    expr1: Box::new(literal_true),
                    expr2: Box::new(col_ref),
                }
            }
            _ => panic!("unsupported expression type"),
        }
    }

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
        panic!("column not found")
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
}
