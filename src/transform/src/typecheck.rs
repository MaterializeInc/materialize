// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Check that the visible type of each query has not been changed

use std::{cell::RefCell, collections::BTreeMap};

use mz_compute_client::types::dataflows::BuildDesc;
use mz_expr::{typecheck::columns_match, Id, OptimizedMirRelationExpr};
use mz_repr::ColumnType;
use tracing::warn;

/// Check that the visible type of each query has not been changed
#[derive(Debug)]
pub struct Typecheck {
    /// The known types of the queries so far
    pub ctx: RefCell<BTreeMap<Id, Vec<ColumnType>>>,
}

impl Default for Typecheck {
    fn default() -> Self {
        Self {
            ctx: RefCell::new(BTreeMap::new()),
        }
    }
}

impl crate::Transform for Typecheck {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "typecheck")
    )]
    fn transform(
        &self,
        _relation: &mut mz_expr::MirRelationExpr,
        _args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        unimplemented!("typechecking should be called with transform_query")
    }

    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "typecheck")
    )]
    fn transform_query(
        &self,
        build_desc: &mut BuildDesc<OptimizedMirRelationExpr>,
        _args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let BuildDesc { id, plan } = build_desc;
        let mut ctx = self.ctx.borrow_mut();

        let expected = ctx.get(&Id::Global(*id));

        if expected.is_none() {
            warn!("NEW TOP LEVEL QUERY {}\n{:#?}", id, plan);
        }

        let got = plan.typecheck(&ctx);
        //            .map_err(|err| TransformError::Internal(format!("type error: {:?}", err)));

        match (got, expected) {
            (Ok(got), Some(expected)) => {
                if !columns_match(&got, expected) {
                    warn!(
                        "TYPE ERROR: got {:#?} expected {:#?} \nIN QUERY BOUND TO {}:\n{:#?}",
                        got, expected, id, plan
                    );
                }
            }
            (Ok(got), None) => {
                ctx.insert(Id::Global(id.clone()), got);
            }
            (Err(err), _) => warn!(
                "TYPE ERROR: got {} expected {:#?} \nIN QUERY BOUND TO {}:\n{:#?}",
                err, expected, id, plan
            ),
        }

        Ok(())
    }
}
