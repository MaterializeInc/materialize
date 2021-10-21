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

use crate::query_model::{BoxId, BoxType, Model, QuantifierType, Values};

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
}
