// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `QGM` structures.

pub(crate) mod dot;

use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_sql::query_model::Model;

use super::{ExplainContext, Explainable};
use dot::ModelDotExplanation;

impl<'a> Explain<'a> for Explainable<'a, Model> {
    type Context = ExplainContext<'a>;

    type Text = UnsupportedFormat;

    type Json = UnsupportedFormat;

    type Dot = ModelDotExplanation; // FIXME: see ModelDotExplanation docs

    fn explain_dot(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Dot, ExplainError> {
        let result = self.0.as_dot("", context.humanizer, config.types)?;
        Ok(ModelDotExplanation::from(result))
    }
}
