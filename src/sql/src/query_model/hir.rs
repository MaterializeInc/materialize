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

use crate::plan::HirScalarExpr;
use crate::query_model::{
    BoxId, BoxScalarExpr, BoxType, Column, ColumnReference, Model, QuantifierType, Values,
};

use crate::plan::expr::HirRelationExpr;

impl From<&HirRelationExpr> for Model {
    fn from(expr: &HirRelationExpr) -> Model {
        FromHir::generate(expr)
    }
}

struct FromHir {
    model: Model,
}

impl FromHir {
    /// Generates a Query Graph Model for representing the given query.
    fn generate(expr: &HirRelationExpr) -> Model {
        let mut generator = FromHir {
            model: Model::new(),
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
            _ => panic!("unsupported expression type"),
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

    fn generate_expr(&mut self, expr: &HirScalarExpr, context_box: BoxId) -> BoxScalarExpr {
        match expr {
            HirScalarExpr::Literal(row, col_type) => {
                BoxScalarExpr::Literal(row.clone(), col_type.clone())
            }
            HirScalarExpr::Column(c) => {
                assert!(c.level == 0, "correlated columns not yet supported");
                BoxScalarExpr::ColumnReference(self.find_column_within_box(context_box, c.column))
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
}
