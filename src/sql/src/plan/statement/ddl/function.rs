// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning for `CREATE FUNCTION`.

use std::sync::Arc;

use base64::Engine;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use mz_expr::func::{WasmLimits, WasmModuleHash};
use mz_repr::SqlScalarType;
use mz_repr::bytes::ByteSize;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateFunctionOption, CreateFunctionOptionName, CreateFunctionStatement, FunctionBody,
    FunctionVolatility, Statement,
};
use mz_wasm_udf_abi::{ModuleInfo, ScalarSignature, UdfType};

use crate::names::Aug;
use crate::normalize;
use crate::plan::query::scalar_type_from_sql;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{CreateFunctionPlan, Function, Plan, PlanError, WasmFunction};
use crate::session::vars;

/// The fuel one call gets unless the function sets `FUEL`. A limit applies to
/// a whole batched call, so this leaves about 100K instructions per row at
/// the default batch size of 1024 rows.
pub const DEFAULT_WASM_FUNCTION_FUEL: u64 = 100_000_000;

/// The memory limit of one call unless the function sets `MEMORY`. Sized for
/// the input and output buffers of a full batch plus working memory.
pub const DEFAULT_WASM_FUNCTION_MEMORY: ByteSize = ByteSize::mb(64);

generate_extracted_config!(
    CreateFunctionOption,
    (Export, String),
    (Fuel, u64),
    (Memory, ByteSize)
);

/// Standard base64 that accepts input with or without padding.
const BASE64: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

pub fn describe_create_function(
    _: &StatementContext,
    _: CreateFunctionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_function(
    scx: &StatementContext,
    stmt: CreateFunctionStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_WASM_FUNCTIONS)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateFunction(stmt.clone()))?;
    let CreateFunctionStatement {
        name,
        if_not_exists,
        args,
        returns,
        language,
        volatility,
        null_behavior,
        body,
        with_options,
    } = stmt;

    if language.as_str() != "wasm" {
        bail_unsupported!(format!("LANGUAGE {language}"));
    }
    match volatility {
        None | Some(FunctionVolatility::Immutable) => {}
        Some(volatility) => sql_bail!(
            "WebAssembly functions must be IMMUTABLE, not {volatility}: Materialize recomputes \
             a function's result whenever it maintains a view that uses it, so the result must \
             never change"
        ),
    }

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;
    let CreateFunctionOptionExtracted {
        export,
        fuel,
        memory,
        seen: _,
    } = with_options.try_into()?;

    let arg_types = args
        .iter()
        .map(|arg| scalar_type_from_sql(scx, &arg.data_type))
        .collect::<Result<Vec<_>, _>>()?;
    let return_type = scalar_type_from_sql(scx, &returns)?;
    if return_type != return_type.without_modifiers() {
        sql_bail!(
            "the return type of a WebAssembly function cannot have type modifiers, found {}",
            scx.humanize_sql_scalar_type(&return_type, false)
        );
    }
    let udf_type = |typ: &SqlScalarType| {
        UdfType::from_sql(typ).map_err(|_| {
            sql_err!(
                "type {} is not supported by WebAssembly functions",
                scx.humanize_sql_scalar_type(typ, false)
            )
        })
    };
    let signature = ScalarSignature {
        name: export.unwrap_or_else(|| name.item.clone()),
        args: arg_types.iter().map(udf_type).collect::<Result<_, _>>()?,
        ret: udf_type(&return_type)?,
    };

    let FunctionBody::Base64(encoded) = body;
    let encoded: String = encoded
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let module = BASE64
        .decode(&encoded)
        .map_err(|e| sql_err!("USING BASE64 is not valid base64: {e}"))?;
    let info = ModuleInfo::parse(&module).map_err(|e| sql_err!("invalid function module: {e}"))?;
    let export = info
        .scalar_export(&signature)
        .map_err(|e| sql_err!("invalid function module: {e}"))?;

    let wasm = WasmFunction {
        module: Arc::from(module),
        module_hash: WasmModuleHash(info.hash),
        export,
        signature: signature.to_string(),
        arg_names: args
            .iter()
            .map(|arg| arg.name.as_ref().map(|n| n.to_string()))
            .collect(),
        arg_types,
        return_type,
        strict: null_behavior.is_some_and(|b| b.is_strict()),
        limits: WasmLimits {
            fuel: fuel.unwrap_or(DEFAULT_WASM_FUNCTION_FUEL),
            memory_bytes: memory.unwrap_or(DEFAULT_WASM_FUNCTION_MEMORY).as_bytes(),
        },
    };

    Ok(Plan::CreateFunction(CreateFunctionPlan {
        name,
        function: Function { create_sql, wasm },
        if_not_exists,
    }))
}
