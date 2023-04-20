// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Check that the visible type of each query has not been changed

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use mz_compute_client::types::dataflows::BuildDesc;
use mz_expr::{
    typecheck::{columns_pretty, is_subtype_of},
    Id, OptimizedMirRelationExpr,
};

use crate::TransformError;

/// Type checking contexts.
///
/// We use a `RefCell` to ensure that contexts are shared by multiple typechecker passes.
/// Shared contexts help catch consistency issues.
pub type Context = Rc<RefCell<mz_expr::typecheck::Context>>;

/// Generates an empty context
pub fn empty_context() -> Context {
    Rc::new(RefCell::new(BTreeMap::new()))
}

/// Check that the visible type of each query has not been changed
#[derive(Debug)]
pub struct Typecheck {
    /// The known types of the queries so far
    ctx: Context,
    /// Whether or not this is the first run of the transform
    disallow_new_globals: bool,
}

impl Typecheck {
    /// Creates a typechecking consistency checking pass using a given shared context
    pub fn new(ctx: Context) -> Self {
        Self {
            ctx,
            disallow_new_globals: false,
        }
    }

    /// New non-transient global IDs will be treated as an error
    ///
    /// Only turn this on after the context has been appropraitely populated by, e.g., an earlier run
    pub fn disallow_new_globals(mut self) -> Self {
        self.disallow_new_globals = true;
        self
    }
}

// Detailed type error logging as a warning, with failures in CI (SOFT_ASSERTIONS) and a logged error in production
macro_rules! type_error {
    ($($arg:tt)+) => {{
        ::tracing::warn!($($arg)+);

        if mz_ore::assert::SOFT_ASSERTIONS.load(::std::sync::atomic::Ordering::Relaxed) {
            return Err(TransformError::Internal("type error in MIR optimization (details in warning)".to_string()));
        } else {
            ::tracing::error!("type error in MIR optimization (details in warning)");
        }
    }}
}

impl crate::Transform for Typecheck {
    fn transform(
        &self,
        relation: &mut mz_expr::MirRelationExpr,
        _args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let ctx = self.ctx.borrow();

        if let Err(err) = relation.typecheck(&ctx) {
            type_error!(
                "TYPE ERROR: {err}\nIN UNKNOWN QUERY:\n{}",
                relation.pretty()
            );
        }

        Ok(())
    }

    fn transform_query(
        &self,
        build_desc: &mut BuildDesc<OptimizedMirRelationExpr>,
        _args: crate::TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let BuildDesc { id, plan } = build_desc;
        let mut ctx = self.ctx.borrow_mut();

        let expected = ctx.get(&Id::Global(*id));

        if self.disallow_new_globals && expected.is_none() && !id.is_transient() {
            type_error!(
                "FOUND NEW NON-TRANSIENT TOP LEVEL QUERY BOUND TO {id}:\n{}",
                plan.pretty()
            );
        }

        let got = plan.typecheck(&ctx);

        let humanizer = mz_repr::explain::DummyHumanizer;
        match (got, expected) {
            (Ok(got), Some(expected)) => {
                // contravariant: global types can be updated
                if !is_subtype_of(expected, &got) {
                    let got = columns_pretty(&got, &humanizer);
                    let expected = columns_pretty(expected, &humanizer);

                    type_error!(
                        "TYPE ERROR: GLOBAL ID TYPE CHANGED\n     got {got}\nexpected {expected} \nIN KNOWN QUERY BOUND TO {id}:\n{}",
                        plan.pretty()
                    );
                }
            }
            (Ok(got), None) => {
                ctx.insert(Id::Global(*id), got);
            }
            (Err(err), _) => {
                let (expected, known) = match expected {
                    Some(expected) => (
                        format!("expected type {}", columns_pretty(expected, &humanizer)),
                        "KNOWN".to_string(),
                    ),
                    None => ("no expected type".to_string(), "NEW".to_string()),
                };

                type_error!(
                    "TYPE ERROR:\n{err}\n{expected}\nIN {known} QUERY BOUND TO {id}:\n{}",
                    plan.pretty()
                );
            }
        }

        Ok(())
    }
}
