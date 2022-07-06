// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Mir` structures.

pub(crate) mod json;

use mz_compute_client::command::DataflowDescription;
use mz_compute_client::plan::Plan;
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};

use super::common::{Explanation, JsonViewFormatter};
use super::{ExplainContext, Explainable};

impl<'a> Explain<'a> for Explainable<'a, DataflowDescription<Plan>> {
    type Context = ExplainContext<'a>;

    type Text = UnsupportedFormat;

    type Json = Explanation<'a, JsonViewFormatter, Plan>;

    type Dot = UnsupportedFormat;

    fn explain_json(
        &'a mut self,
        _config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Json, ExplainError> {
        let formatter = JsonViewFormatter {};
        let mut explanation = Explanation::new_from_dataflow(self.0, context.humanizer, formatter);
        if let Some(row_set_finishing) = context.finishing.clone() {
            explanation.explain_row_set_finishing(row_set_finishing);
        }
        Ok(explanation)
    }
}
