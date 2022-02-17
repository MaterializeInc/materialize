// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generates a [`HirRelationExpr`] from a [`Model`].
//!
//! The public interface consists of the [`From<Model>`]
//! implementation for [`HirRelationExpr`].

use mz_repr::RelationType;

use crate::plan::expr::HirRelationExpr;
use crate::query_model::{Model, QGMError};

impl From<Model> for HirRelationExpr {
    fn from(model: Model) -> Self {
        FromModel::default().generate(model)
    }
}

#[derive(Default)]
struct FromModel;

impl FromModel {
    #[allow(dead_code, unused_variables, unused_mut)]
    fn generate(mut self, model: Model) -> HirRelationExpr {
        let _ = model.try_visit_pre_post(
            &mut |model, box_id| -> Result<(), QGMError> {
                println!("pre-visit");
                Ok(())
            },
            &mut |model, box_id| -> Result<(), QGMError> {
                println!("post-visit");
                Ok(())
            },
        );
        // this should be todo!(), but we cannot merge to main code
        // with todo!() statements
        HirRelationExpr::constant(vec![vec![]], RelationType::empty())
    }
}
