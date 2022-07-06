// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Mir` structures.

pub(crate) mod text;

use mz_compute_client::command::DataflowDescription;
use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};

use super::common::{DataflowGraphFormatter, Explanation};
use super::{ExplainContext, Explainable};

impl<'a> Explain<'a> for Explainable<'a, OptimizedMirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = Explanation<'a, DataflowGraphFormatter<'a>, OptimizedMirRelationExpr>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let formatter = DataflowGraphFormatter::new(context.humanizer, config.types);
        let mut explanation = Explanation::new(self.0, context.humanizer, formatter);
        if let Some(row_set_finishing) = context.finishing.clone() {
            explanation.explain_row_set_finishing(row_set_finishing);
        }
        Ok(explanation)
    }
}

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<OptimizedMirRelationExpr>> {
    type Context = ExplainContext<'a>;

    type Text = Explanation<'a, DataflowGraphFormatter<'a>, OptimizedMirRelationExpr>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let formatter = DataflowGraphFormatter::new(context.humanizer, config.types);
        let mut explanation = Explanation::new_from_dataflow(self.0, context.humanizer, formatter);
        if let Some(row_set_finishing) = context.finishing.clone() {
            explanation.explain_row_set_finishing(row_set_finishing);
        }
        Ok(explanation)
    }
}
