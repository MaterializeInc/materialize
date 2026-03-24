// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Re-exports HIR types from `mz_hir` and provides extension methods
//! that depend on `mz_sql` internals (type coercion, parameter binding,
//! expression simplification via lowering).

use std::mem;
use std::sync::Arc;

use mz_expr::RowSetFinishing;
use mz_ore::error::ErrorExt;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::*;

use crate::plan::error::PlanError;
use crate::plan::query::{EXECUTE_CAST_CONTEXT, ExprContext, execute_expr_context};
use crate::plan::typeconv::{self, CastContext, plan_cast};
use crate::plan::{Params, QueryContext, QueryLifetime, StatementContext};

use super::plan_utils::GroupSizeHints;

// Re-export everything from mz_hir so that existing `use crate::plan::hir::*`
// patterns continue to work.
pub use mz_hir::*;

/// Extension trait for [`CoercibleScalarExpr`] methods that depend on `mz_sql` internals.
pub trait CoercibleScalarExprExt {
    fn type_as(self, ecx: &ExprContext, ty: &SqlScalarType) -> Result<HirScalarExpr, PlanError>;

    fn type_as_any(self, ecx: &ExprContext) -> Result<HirScalarExpr, PlanError>;

    fn cast_to(
        self,
        ecx: &ExprContext,
        ccx: CastContext,
        ty: &SqlScalarType,
    ) -> Result<HirScalarExpr, PlanError>;
}

impl CoercibleScalarExprExt for CoercibleScalarExpr {
    fn type_as(self, ecx: &ExprContext, ty: &SqlScalarType) -> Result<HirScalarExpr, PlanError> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        let expr_ty = ecx.scalar_type(&expr);
        if ty != &expr_ty {
            sql_bail!(
                "{} must have type {}, not type {}",
                ecx.name,
                ecx.humanize_sql_scalar_type(ty, false),
                ecx.humanize_sql_scalar_type(&expr_ty, false),
            );
        }
        Ok(expr)
    }

    fn type_as_any(self, ecx: &ExprContext) -> Result<HirScalarExpr, PlanError> {
        typeconv::plan_coerce(ecx, self, &SqlScalarType::String)
    }

    fn cast_to(
        self,
        ecx: &ExprContext,
        ccx: CastContext,
        ty: &SqlScalarType,
    ) -> Result<HirScalarExpr, PlanError> {
        let expr = typeconv::plan_coerce(ecx, self, ty)?;
        typeconv::plan_cast(ecx, ccx, expr, ty)
    }
}

/// Extension trait for [`HirRelationExpr`] methods that depend on `mz_sql` internals.
pub trait HirRelationExprExt {
    fn bind_parameters(
        &mut self,
        scx: &StatementContext,
        lifetime: QueryLifetime,
        params: &Params,
    ) -> Result<(), PlanError>;

    fn contains_parameters(&self) -> Result<bool, PlanError>;

    fn finish_maintained(
        &mut self,
        finishing: &mut RowSetFinishing<HirScalarExpr, HirScalarExpr>,
        group_size_hints: GroupSizeHints,
    );

    fn trivial_row_set_finishing_hir(arity: usize)
    -> RowSetFinishing<HirScalarExpr, HirScalarExpr>;

    fn is_trivial_row_set_finishing_hir(
        rsf: &RowSetFinishing<HirScalarExpr, HirScalarExpr>,
        arity: usize,
    ) -> bool;
}

impl HirRelationExprExt for HirRelationExpr {
    fn bind_parameters(
        &mut self,
        scx: &StatementContext,
        lifetime: QueryLifetime,
        params: &Params,
    ) -> Result<(), PlanError> {
        #[allow(deprecated)]
        self.visit_scalar_expressions_mut(0, &mut |e: &mut HirScalarExpr, _: usize| {
            e.bind_parameters(scx, lifetime, params)
        })
    }

    fn contains_parameters(&self) -> Result<bool, PlanError> {
        let mut contains_parameters = false;
        #[allow(deprecated)]
        self.visit_scalar_expressions(0, &mut |e: &HirScalarExpr, _: usize| {
            if e.contains_parameters() {
                contains_parameters = true;
            }
            Ok::<(), PlanError>(())
        })?;
        Ok(contains_parameters)
    }

    fn finish_maintained(
        &mut self,
        finishing: &mut RowSetFinishing<HirScalarExpr, HirScalarExpr>,
        group_size_hints: GroupSizeHints,
    ) {
        if !HirRelationExpr::is_trivial_row_set_finishing_hir(finishing, self.arity()) {
            let old_finishing = mem::replace(
                finishing,
                HirRelationExpr::trivial_row_set_finishing_hir(finishing.project.len()),
            );
            *self = HirRelationExpr::top_k(
                std::mem::replace(
                    self,
                    HirRelationExpr::Constant {
                        rows: vec![],
                        typ: SqlRelationType::new(Vec::new()),
                    },
                ),
                vec![],
                old_finishing.order_by,
                old_finishing.limit,
                old_finishing.offset,
                group_size_hints.limit_input_group_size,
            )
            .project(old_finishing.project);
        }
    }

    fn trivial_row_set_finishing_hir(
        arity: usize,
    ) -> RowSetFinishing<HirScalarExpr, HirScalarExpr> {
        RowSetFinishing {
            order_by: Vec::new(),
            limit: None,
            offset: HirScalarExpr::literal(Datum::Int64(0), SqlScalarType::Int64),
            project: (0..arity).collect(),
        }
    }

    fn is_trivial_row_set_finishing_hir(
        rsf: &RowSetFinishing<HirScalarExpr, HirScalarExpr>,
        arity: usize,
    ) -> bool {
        rsf.limit.is_none()
            && rsf.order_by.is_empty()
            && rsf
                .offset
                .clone()
                .try_into_literal_int64()
                .is_ok_and(|o| o == 0)
            && rsf.project.iter().copied().eq(0..arity)
    }
}

/// Extension trait for [`HirScalarExpr`] methods that depend on `mz_sql` internals.
pub trait HirScalarExprExt {
    /// Replaces any parameter references in the expression with the
    /// corresponding datum in `params`.
    fn bind_parameters(
        &mut self,
        scx: &StatementContext,
        lifetime: QueryLifetime,
        params: &Params,
    ) -> Result<(), PlanError>;

    fn literal_1d_array(
        datums: Vec<Datum>,
        element_scalar_type: SqlScalarType,
    ) -> Result<HirScalarExpr, PlanError>;

    /// Attempts to simplify this expression to a literal 64-bit integer.
    ///
    /// Returns `None` if this expression cannot be simplified, e.g. because it
    /// contains non-literal values.
    ///
    /// # Panics
    ///
    /// Panics if this expression does not have type [`SqlScalarType::Int64`].
    fn into_literal_int64(self) -> Option<i64>;

    /// Attempts to simplify this expression to a literal string.
    ///
    /// Returns `None` if this expression cannot be simplified, e.g. because it
    /// contains non-literal values.
    ///
    /// # Panics
    ///
    /// Panics if this expression does not have type [`SqlScalarType::String`].
    fn into_literal_string(self) -> Option<String>;

    /// Attempts to simplify this expression to a literal MzTimestamp.
    fn into_literal_mz_timestamp(self) -> Option<Timestamp>;

    /// Attempts to simplify this expression of [`SqlScalarType::Int64`] to a literal Int64.
    fn try_into_literal_int64(self) -> Result<i64, PlanError>;
}

impl HirScalarExprExt for HirScalarExpr {
    fn bind_parameters(
        &mut self,
        scx: &StatementContext,
        lifetime: QueryLifetime,
        params: &Params,
    ) -> Result<(), PlanError> {
        #[allow(deprecated)]
        self.visit_recursively_mut(0, &mut |_: usize, e: &mut HirScalarExpr| {
            if let HirScalarExpr::Parameter(n, name) = e {
                let datum = match params.datums.iter().nth(*n - 1) {
                    None => return Err(PlanError::UnknownParameter(*n)),
                    Some(datum) => datum,
                };
                let scalar_type = &params.execute_types[*n - 1];
                let row = Row::pack([datum]);
                let column_type = scalar_type.clone().nullable(datum.is_null());

                let name = if let Some(name) = &name.0 {
                    Some(Arc::clone(name))
                } else {
                    Some(Arc::from(format!("${n}")))
                };

                let qcx = QueryContext::root(scx, lifetime);
                let ecx = execute_expr_context(&qcx);

                *e = plan_cast(
                    &ecx,
                    *EXECUTE_CAST_CONTEXT,
                    HirScalarExpr::Literal(row, column_type, TreatAsEqual(name)),
                    &params.expected_types[*n - 1],
                )
                .expect("checked in plan_params");
            }
            Ok(())
        })
    }

    fn literal_1d_array(
        datums: Vec<Datum>,
        element_scalar_type: SqlScalarType,
    ) -> Result<HirScalarExpr, PlanError> {
        let scalar_type = match element_scalar_type {
            SqlScalarType::Array(_) => {
                sql_bail!("cannot build array from array type");
            }
            typ => SqlScalarType::Array(Box::new(typ)).nullable(false),
        };

        let mut row = Row::default();
        row.packer()
            .try_push_array(
                &[ArrayDimension {
                    lower_bound: 1,
                    length: datums.len(),
                }],
                datums,
            )
            .expect("array constructed to be valid");

        Ok(HirScalarExpr::Literal(row, scalar_type, TreatAsEqual(None)))
    }

    fn into_literal_int64(self) -> Option<i64> {
        simplify_to_literal(self).and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                None
            } else {
                Some(datum.unwrap_int64())
            }
        })
    }

    fn into_literal_string(self) -> Option<String> {
        simplify_to_literal(self).and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                None
            } else {
                Some(datum.unwrap_str().to_owned())
            }
        })
    }

    fn into_literal_mz_timestamp(self) -> Option<Timestamp> {
        simplify_to_literal(self).and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                None
            } else {
                Some(datum.unwrap_mz_timestamp())
            }
        })
    }

    fn try_into_literal_int64(self) -> Result<i64, PlanError> {
        if !self.is_constant() {
            return Err(PlanError::ConstantExpressionSimplificationFailed(format!(
                "Expected a constant expression, got {}",
                self
            )));
        }
        simplify_to_literal_with_result(self.clone()).and_then(|row| {
            let datum = row.unpack_first();
            if datum.is_null() {
                Err(PlanError::ConstantExpressionSimplificationFailed(format!(
                    "Expected an expression that evaluates to a non-null value, got {}",
                    self
                )))
            } else {
                Ok(datum.unwrap_int64())
            }
        })
    }
}

// Private helper functions for HirScalarExpr simplification.
// These use lowering internals and cannot be methods on the foreign type.
fn simplify_to_literal(expr: HirScalarExpr) -> Option<Row> {
    use crate::plan::lowering::HirScalarExprLowering;
    let mut mir = expr
        .lower_uncorrelated(crate::plan::lowering::Config::default())
        .ok()?;
    mir.reduce(&[]);
    match mir {
        mz_expr::MirScalarExpr::Literal(Ok(row), _) => Some(row),
        _ => None,
    }
}

fn simplify_to_literal_with_result(expr: HirScalarExpr) -> Result<Row, PlanError> {
    use crate::plan::lowering::HirScalarExprLowering;
    let mut mir = expr
        .lower_uncorrelated(crate::plan::lowering::Config::default())
        .map_err(|err| {
            PlanError::ConstantExpressionSimplificationFailed(err.to_string_with_causes())
        })?;
    mir.reduce(&[]);
    match mir {
        mz_expr::MirScalarExpr::Literal(Ok(row), _) => Ok(row),
        mz_expr::MirScalarExpr::Literal(Err(err), _) => Err(
            PlanError::ConstantExpressionSimplificationFailed(err.to_string_with_causes()),
        ),
        _ => Err(PlanError::ConstantExpressionSimplificationFailed(
            "Not a constant".to_string(),
        )),
    }
}
