// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for HIR structures.

use mz_repr::explain::{Explain, ExplainError};
use mz_sql::plan::HirRelationExpr;

use crate::explain::Explainable;

impl<'a> Explain<'a> for Explainable<'a, HirRelationExpr> {
    type Context = <HirRelationExpr as Explain<'a>>::Context;

    type Text = <HirRelationExpr as Explain<'a>>::Text;

    type Json = <HirRelationExpr as Explain<'a>>::Json;

    type Dot = <HirRelationExpr as Explain<'a>>::Dot;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.0.explain_text(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Json, ExplainError> {
        self.0.explain_json(context)
    }

    fn explain_dot(&'a mut self, context: &'a Self::Context) -> Result<Self::Dot, ExplainError> {
        self.0.explain_dot(context)
    }
}
