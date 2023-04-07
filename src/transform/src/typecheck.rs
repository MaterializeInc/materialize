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
use mz_expr::{Id, OptimizedMirRelationExpr};
use mz_repr::ColumnType;
use tracing::{error, info, warn};

use crate::TransformError;

/// Type checking contexts.
/// 
/// We use a `RefCell` to ensure that contexts are shared by multiple typechecker passes.
/// Shared contexts help catch consistency issues.
pub type Context = RefCell<BTreeMap<Id, Vec<ColumnType>>>;

/// Generates an empty context
pub fn empty_context() -> Context {
    RefCell::new(BTreeMap::new())
}

/// Check that the visible type of each query has not been changed
#[derive(Debug)]
pub struct Typecheck {
    /// The known types of the queries so far
    pub ctx: Context,
}

impl Typecheck {
    /// Creates a typechecking consistency checking pass using a given shared context
    pub fn new(ctx: Context) -> Self {
        Self { ctx, }
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
        relation: &mut mz_expr::MirRelationExpr,
        _args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let ctx = self.ctx.borrow();

        if let Err(err) = relation.typecheck(&ctx) {
            warn!("TYPE ERROR: {}\nIN UNKNOWN QUERY:\n{:#?}", err, relation);
        }

        Ok(())
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

        if expected.is_none() && !id.is_transient() {
            info!(
                "TYPECHECKER FOUND NEW NON-TRANSIENT TOP LEVEL QUERY {}\n{:#?}",
                id, plan
            );
        }

        let got = plan.typecheck(&ctx);

        match (got, expected) {
            (Ok(got), Some(expected)) => {
                if !mz_expr::typecheck::columns_match(&got, expected) {
                    return Err(TransformError::Internal(format!(
                        "TYPE ERROR: got {:#?} expected {:#?} \nIN QUERY BOUND TO {}:\n{:#?}",
                        got, expected, id, plan
                    )));
                }
            }
            (Ok(got), None) => {
                ctx.insert(Id::Global(*id), got);
            }
            (Err(err), _) => {
                error!(
                    "TYPE ERROR:\n{} expected type for this plan: {:#?} \nIN QUERY BOUND TO {}:\n{:#?}",
                    err, expected, id, plan
                );
            }
        }

        Ok(())
    }
}
