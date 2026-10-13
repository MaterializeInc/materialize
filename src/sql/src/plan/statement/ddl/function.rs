// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning for `CREATE WASM MODULE` and `CREATE FUNCTION ... LANGUAGE wasm`.
//!
//! Planning runs both when a statement executes and when the catalog
//! re-plans stored `create_sql` at boot. It therefore checks only what a
//! stored definition cannot invalidate: decoding, the declared types, and the
//! reference to the module. Validating a module and checking that it exports a
//! function's signature happen when the statement is sequenced, through
//! [`WasmModule::validate`] and [`WasmModule::check_export`], so a stricter
//! validator never stops an existing catalog from loading.

use std::sync::Arc;

use base64::Engine;
use base64::engine::DecodePaddingMode;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use mz_expr::func::{WasmLimits, WasmModuleHash};
use mz_ore::str::StrExt;
use mz_repr::bytes::ByteSize;
use mz_repr::{Datum, SqlScalarType};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    CreateFunctionOption, CreateFunctionOptionName, CreateFunctionStatement,
    CreateWasmModuleStatement, FunctionNullBehavior, FunctionVolatility, Statement, Value,
    WasmModuleBody, WithOptionValue,
};
use mz_wasm_udf_abi::{ModuleInfo, ScalarSignature, UdfType};

use crate::catalog::CatalogItemType;
use crate::func::{MZ_CATALOG_BUILTINS, PG_CATALOG_BUILTINS};
use crate::names::Aug;
use crate::normalize;
use crate::plan::query::scalar_type_from_sql;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{
    CreateFunctionPlan, CreateWasmModulePlan, Function, Params, Plan, PlanError, WasmFunction,
    WasmModule,
};
use crate::session::vars;

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

pub fn describe_create_wasm_module(
    scx: &StatementContext,
    stmt: CreateWasmModuleStatement,
) -> Result<StatementDesc, PlanError> {
    if let WasmModuleBody::Base64Parameter(n) = stmt.body {
        scx.param_types
            .borrow_mut()
            .insert(n, SqlScalarType::String);
    }
    Ok(StatementDesc::new(None))
}

pub fn plan_create_wasm_module(
    scx: &StatementContext,
    mut stmt: CreateWasmModuleStatement,
    params: &Params,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_WASM_FUNCTIONS)?;
    let encoded = match &stmt.body {
        WasmModuleBody::Base64(encoded) => encoded.clone(),
        WasmModuleBody::Base64Parameter(n) => {
            let datum = n
                .checked_sub(1)
                .and_then(|i| params.datums.iter().nth(i))
                .ok_or(PlanError::UnknownParameter(*n))?;
            let Datum::String(encoded) = datum else {
                sql_bail!("USING BASE64 ${n} must be a non-null text value");
            };
            encoded.to_owned()
        }
    };
    let encoded: String = encoded
        .chars()
        .filter(|c| !c.is_ascii_whitespace())
        .collect();
    let bytes = BASE64
        .decode(&encoded)
        .map_err(|e| sql_err!("USING BASE64 is not valid base64: {e}"))?;
    // The catalog re-plans the module from `create_sql`, so the stored
    // statement carries the module itself rather than a parameter, in
    // canonical form: whatever whitespace or padding the upload had, the
    // stored text is the size of the module and no more.
    stmt.body = WasmModuleBody::Base64(STANDARD.encode(&bytes));
    let create_sql = normalize::create_statement(scx, Statement::CreateWasmModule(stmt.clone()))?;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(stmt.name)?)?;
    let hash = WasmModuleHash(mz_wasm_udf_abi::module_hash(&bytes));
    Ok(Plan::CreateWasmModule(CreateWasmModulePlan {
        name,
        module: WasmModule {
            create_sql,
            bytes: Arc::from(bytes),
            hash,
        },
        if_not_exists: stmt.if_not_exists,
    }))
}

pub fn describe_create_function(
    _: &StatementContext,
    _: CreateFunctionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_function(
    scx: &StatementContext,
    mut stmt: CreateFunctionStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_WASM_FUNCTIONS)?;
    if stmt.language.as_str() != "wasm" {
        bail_unsupported!(format!("LANGUAGE {}", stmt.language));
    }
    match stmt.volatility {
        None | Some(FunctionVolatility::Immutable) => {}
        Some(volatility) => sql_bail!(
            "WebAssembly functions must be IMMUTABLE, not {volatility}: Materialize recomputes \
             a function's result whenever it maintains a view that uses it, so the result must \
             never change"
        ),
    }

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(stmt.name.clone())?)?;

    let module_item = scx.get_item_by_resolved_name(&stmt.module)?;
    let module_name = scx.catalog.resolve_full_name(module_item.name());
    let module = match (module_item.item_type(), module_item.wasm_module()) {
        (CatalogItemType::WasmModule, Some(module)) => module,
        _ => sql_bail!("{} is not a WebAssembly module", module_name),
    };

    let CreateFunctionOptionExtracted {
        export,
        fuel,
        memory,
        seen: _,
    } = stmt.with_options.clone().try_into()?;
    let export = export.unwrap_or_else(|| name.item.clone());
    let limits = WasmLimits {
        fuel: fuel.unwrap_or(mz_wasm_udf_abi::DEFAULT_FUEL),
        memory_bytes: memory.map_or(mz_wasm_udf_abi::DEFAULT_MEMORY_BYTES, |m| m.as_bytes()),
    };
    let strict = stmt.null_behavior.is_some_and(|b| b.is_strict());

    let arg_types = stmt
        .args
        .iter()
        .map(|arg| scalar_type_from_sql(scx, &arg.data_type))
        .collect::<Result<Vec<_>, _>>()?;
    let return_type = scalar_type_from_sql(scx, &stmt.returns)?;
    if has_modifier(&return_type) {
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
        name: export.clone(),
        args: arg_types.iter().map(udf_type).collect::<Result<_, _>>()?,
        ret: udf_type(&return_type)?,
    };

    // The stored definition spells out every option planning would
    // otherwise default, so that changing a default never changes an
    // existing function.
    stmt.volatility = Some(FunctionVolatility::Immutable);
    stmt.null_behavior = Some(if strict {
        FunctionNullBehavior::Strict
    } else {
        FunctionNullBehavior::CalledOnNullInput
    });
    stmt.with_options = vec![
        CreateFunctionOption {
            name: CreateFunctionOptionName::Export,
            value: Some(WithOptionValue::Value(Value::String(export))),
        },
        CreateFunctionOption {
            name: CreateFunctionOptionName::Fuel,
            value: Some(WithOptionValue::Value(Value::Number(
                limits.fuel.to_string(),
            ))),
        },
        CreateFunctionOption {
            name: CreateFunctionOptionName::Memory,
            value: Some(WithOptionValue::Value(Value::String(
                ByteSize::b(limits.memory_bytes).to_string(),
            ))),
        },
    ];
    let create_sql = normalize::create_statement(scx, Statement::CreateFunction(stmt.clone()))?;

    let wasm = WasmFunction {
        module_id: module_item.id(),
        module_hash: module.hash,
        export: signature.export_name(),
        signature: signature.to_string(),
        arg_names: stmt
            .args
            .iter()
            .map(|arg| arg.name.as_ref().map(|n| n.to_string()))
            .collect(),
        arg_types,
        return_type,
        strict,
        limits,
    };

    Ok(Plan::CreateFunction(CreateFunctionPlan {
        name,
        function: Function { create_sql, wasm },
        if_not_exists: stmt.if_not_exists,
    }))
}

/// Whether `typ`, or an element type inside it, carries a type modifier. The
/// codec does not apply modifiers to returned values, so a return type must
/// have none.
fn has_modifier(typ: &SqlScalarType) -> bool {
    match typ {
        SqlScalarType::Timestamp { precision } | SqlScalarType::TimestampTz { precision } => {
            precision.is_some()
        }
        SqlScalarType::Array(element_type) | SqlScalarType::List { element_type, .. } => {
            has_modifier(element_type)
        }
        typ => *typ != typ.without_modifiers(),
    }
}

impl CreateFunctionPlan {
    /// Checks that the function does not take a builtin function's name.
    /// Builtins are searched before user schemas, so such a function would be
    /// shadowed in every unqualified call.
    ///
    /// Called when `CREATE FUNCTION` is sequenced, not when the catalog
    /// re-plans the function: a later release may add a builtin with the name
    /// of an existing function, which must not stop the catalog from opening.
    pub fn check_not_builtin(&self) -> Result<(), PlanError> {
        let item = self.name.item.as_str();
        if MZ_CATALOG_BUILTINS.contains_key(item) || PG_CATALOG_BUILTINS.contains_key(item) {
            sql_bail!(
                "function {} has the name of a builtin function, which would hide it from \
                 unqualified calls",
                self.name.item.quoted()
            );
        }
        Ok(())
    }
}

impl WasmModule {
    /// Validates the module against the guest ABI. Called when `CREATE WASM
    /// MODULE` is sequenced, not when the catalog re-plans the module.
    pub fn validate(&self) -> Result<(), PlanError> {
        ModuleInfo::parse(&self.bytes)
            .map(|_| ())
            .map_err(|e| sql_err!("invalid WebAssembly module: {e}"))
    }

    /// Checks that the module exports the signature `function` binds.
    /// Called when `CREATE FUNCTION` is sequenced, not when the catalog
    /// re-plans the function.
    pub fn check_export(&self, function: &WasmFunction) -> Result<(), PlanError> {
        let info = ModuleInfo::parse(&self.bytes)
            .map_err(|e| sql_err!("invalid WebAssembly module: {e}"))?;
        let signature = ScalarSignature::parse(&function.signature)
            .map_err(|e| sql_err!("invalid function signature: {e}"))?;
        info.scalar_export(&signature)
            .map(|_| ())
            .map_err(|e| sql_err!("{e}"))
    }
}
