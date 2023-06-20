// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for side-effecting functions.
//!
//! In PostgreSQL, these functions can appear anywhere in a query:
//!
//! ```sql
//! SELECT 1 WHERE pg_cancel_backend(1234)
//! ```
//!
//! In Materialize, our compute layer cannot execute functions with side
//! effects. So we sniff out the common form of calls to side-effecting
//! functions, i.e. at the top level of a `SELECT`
//!
//! ```sql
//! SELECT side_effecting_function(...)
//! ```
//!
//! where all arguments are literals or bound parameters, and plan them
//! specially as a `Plan::SideEffectingFunc`. This gets us compatibility with
//! PostgreSQL for most real-world use cases, without causing stress for the
//! compute layer (optimizer, dataflow execution, etc.), as we can apply all the
//! side effects entirely in the adapter layer.

use std::collections::BTreeMap;

use enum_kinds::EnumKind;
use mz_ore::cast::ReinterpretCast;
use mz_ore::collections::CollectionExt;
use mz_ore::result::ResultExt;
use mz_repr::RelationType;
use mz_repr::{ColumnType, Datum, RelationDesc, RowArena, ScalarType};
use mz_sql_parser::ast::{CteBlock, Expr, Function, FunctionArgs, Select, SelectItem, SetExpr};
use once_cell::sync::Lazy;

use crate::ast::{Query, SelectStatement};
use crate::func::Func;
use crate::names::Aug;
use crate::plan::query::{self, ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::StatementContext;
use crate::plan::typeconv::CastContext;
use crate::plan::{self, HirScalarExpr, Params};
use crate::plan::{PlanError, QueryContext};

/// A side-effecting function is a function whose evaluation triggers side
/// effects.
///
/// See the module docs for details.
#[derive(Debug, EnumKind)]
#[enum_kind(SefKind)]
pub enum SideEffectingFunc {
    /// The `pg_cancel_backend` function, .
    PgCancelBackend {
        // The ID of the connection to cancel.
        connection_id: u32,
    },
}

/// Describes a `SELECT` if it contains calls to side-effecting functions.
///
/// See the module docs for details.
pub fn describe_select_if_side_effecting(
    scx: &StatementContext,
    select: &SelectStatement<Aug>,
) -> Result<Option<RelationDesc>, PlanError> {
    let Some(sef_call) = extract_sef_call(scx, select)? else {
        return Ok(None);
    };

    // We currently support only a single call to a side-effecting function
    // without an alias, so there is always a single output column is named
    // after the function.
    let desc =
        RelationDesc::empty().with_column(sef_call.imp.name, sef_call.imp.return_type.clone());

    Ok(Some(desc))
}

/// Plans the `SELECT` if it contains calls to side-effecting functions.
///
/// See the module docs for details.
pub fn plan_select_if_side_effecting(
    scx: &StatementContext,
    select: &SelectStatement<Aug>,
    params: &Params,
) -> Result<Option<SideEffectingFunc>, PlanError> {
    let Some(sef_call) = extract_sef_call(scx, select)? else {
        return Ok(None);
    };

    // Bind parameters and then eagerly evaluate each argument. Expressions that
    // cannot be eagerly evaluated should have been rejected by `extract_sef_call`.
    let temp_storage = RowArena::new();
    let mut args = vec![];
    for mut arg in sef_call.args {
        arg.bind_parameters(params)?;
        let arg = arg.lower_uncorrelated()?;
        args.push(arg);
    }
    let mut datums = vec![];
    for arg in &args {
        let datum = arg.eval(&[], &temp_storage)?;
        datums.push(datum);
    }

    let func = (sef_call.imp.plan_fn)(&datums);

    Ok(Some(func))
}

/// Helper function used in both describing and planning a side-effecting
/// `SELECT`.
fn extract_sef_call(
    scx: &StatementContext,
    select: &SelectStatement<Aug>,
) -> Result<Option<SefCall>, PlanError> {
    // First check if the `SELECT` contains exactly one function call.
    let SelectStatement {
        query:
            Query {
                ctes: CteBlock::Simple(ctes),
                body: SetExpr::Select(body),
                order_by,
                limit: None,
                offset: None,
            },
        as_of: None,
    } = select else {
        return Ok(None)
    };
    if !ctes.is_empty() || !order_by.is_empty() {
        return Ok(None);
    }
    let Select {
        distinct: None,
        projection,
        from,
        selection: None,
        group_by,
        having: None,
        options,
    } = &**body else {
        return Ok(None);
    };
    if !from.is_empty() || !group_by.is_empty() || !options.is_empty() || projection.len() != 1 {
        return Ok(None);
    }
    let [SelectItem::Expr {
        expr:
            Expr::Function(Function {
                name,
                args: FunctionArgs::Args { args, order_by },
                filter: None,
                over: None,
                distinct: false,
            }),
        alias: None,
    }] = &projection[..] else {
        return Ok(None);
    };
    if !order_by.is_empty() {
        return Ok(None);
    }

    // Check if the called function is a scalar function with exactly one
    // implementation. All side-effecting functions have only a single
    // implementation.
    let Ok(func) = scx.get_item_by_resolved_name(name).and_then(|item| item.func().err_into()) else {
        return Ok(None);
    };
    let func_impl = match func {
        Func::Scalar(impls) if impls.len() == 1 => impls.into_element(),
        _ => return Ok(None),
    };

    // Check whether the implementation is a known side-effecting function.
    let Some(sef_impl) = PG_CATALOG_SEF_BUILTINS.get(&func_impl.oid) else {
        return Ok(None);
    };

    // Check that the number of provided arguments matches the function
    // signature.
    if args.len() != sef_impl.param_types.len() {
        // We return `Ok(None)` instead of an error for the same reason to let
        // the function selection code produce the standard "no function matches
        // the given name and argument types" error.
        return Ok(None);
    }

    // Plan and coerce all argument expressions.
    let mut args_out = vec![];
    let pcx = plan::PlanContext::zero();
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(&pcx));
    let ecx = ExprContext {
        qcx: &qcx,
        name: sef_impl.name,
        scope: &Scope::empty(),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: true,
        allow_windows: false,
    };
    for (arg, ty) in args.iter().zip(sef_impl.param_types) {
        // If we encounter an error when planning the argument expression, that
        // error is unrelated to planning the function call and can be returned
        // directly to the user.
        let arg = query::plan_expr(&ecx, arg)?;

        // Implicitly cast the argument to the correct type. This matches what
        // the standard function selection code will do.
        //
        // If the cast fails, we give up on planning the side-effecting function but
        // intentionally do not produce an error. This way, we fall into the
        // standard function selection code, which will produce the correct "no
        // function matches the given name and argument types" error rather than a
        // "cast failed" error.
        let Ok(arg) = arg.cast_to(&ecx, CastContext::Implicit, ty) else {
            return Ok(None);
        };

        args_out.push(arg);
    }

    Ok(Some(SefCall {
        imp: sef_impl,
        args: args_out,
    }))
}

struct SefCall {
    imp: &'static SideEffectingFuncImpl,
    args: Vec<HirScalarExpr>,
}

/// Defines the implementation of a side-effecting function.
///
/// This is a very restricted subset of the [`Func`] struct (no overloads, no
/// variadic arguments, etc) to make side-effecting functions easier to plan.
pub struct SideEffectingFuncImpl {
    /// The name of the function.
    pub name: &'static str,
    /// The OID of the function.
    pub oid: u32,
    /// The parameter types for the function.
    pub param_types: &'static [ScalarType],
    /// The return type of the function.
    pub return_type: ColumnType,
    /// A function that will produce a `SideEffectingFunc` given arguments
    /// that have been evaluated to `Datum`s.
    pub plan_fn: fn(&[Datum]) -> SideEffectingFunc,
}

/// A map of the side-effecting functions in the `pg_catalog` schema, keyed by
/// OID.
pub static PG_CATALOG_SEF_BUILTINS: Lazy<BTreeMap<u32, SideEffectingFuncImpl>> = Lazy::new(|| {
    [PG_CANCEL_BACKEND]
        .into_iter()
        .map(|f| (f.oid, f))
        .collect()
});

// Implementations of each side-effecting function follow.
//
// If you add a new side-effecting function, be sure to add it to the map above.

const PG_CANCEL_BACKEND: SideEffectingFuncImpl = SideEffectingFuncImpl {
    name: "pg_cancel_backend",
    oid: 2171,
    param_types: &[ScalarType::Int32],
    return_type: ScalarType::Bool.nullable(false),
    plan_fn: |datums| -> SideEffectingFunc {
        SideEffectingFunc::PgCancelBackend {
            connection_id: u32::reinterpret_cast(datums[0].unwrap_int32()),
        }
    },
};
