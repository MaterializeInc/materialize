// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL normalization routines.
//!
//! Normalization is the process of taking relatively unstructured types from
//! the [`ast`] module and converting them to more structured types.
//!
//! [`ast`]: crate::ast

use std::collections::BTreeMap;
use std::fmt;

use itertools::Itertools;

use mz_repr::{ColumnName, GlobalId};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    CreateConnectionStatement, CreateIndexStatement, CreateMaterializedViewStatement,
    CreateSecretStatement, CreateSinkStatement, CreateSourceStatement, CreateTableStatement,
    CreateTypeAs, CreateTypeStatement, CreateViewStatement, Function, FunctionArgs, Ident,
    IfExistsBehavior, Op, Query, Statement, TableFactor, TableFunction, UnresolvedObjectName,
    UnresolvedSchemaName, Value, ViewDefinition, WithOption, WithOptionValue,
};
use mz_storage::types::connections::aws::{AwsAssumeRole, AwsConfig, AwsCredentials, SerdeUri};

use crate::names::{
    Aug, FullObjectName, PartialObjectName, PartialSchemaName, RawDatabaseSpecifier,
    ResolvedObjectName,
};
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;

/// Normalizes a single identifier.
pub fn ident(ident: Ident) -> String {
    ident.as_str().into()
}

/// Normalizes a single identifier.
pub fn ident_ref(ident: &Ident) -> &str {
    ident.as_str()
}

/// Normalizes an identifier that represents a column name.
pub fn column_name(id: Ident) -> ColumnName {
    ColumnName::from(ident(id))
}

/// Normalizes an unresolved object name.
pub fn unresolved_object_name(
    mut name: UnresolvedObjectName,
) -> Result<PartialObjectName, PlanError> {
    if name.0.len() < 1 || name.0.len() > 3 {
        return Err(PlanError::MisqualifiedName(name.to_string()));
    }
    let out = PartialObjectName {
        item: ident(
            name.0
                .pop()
                .expect("name checked to have at least one component"),
        ),
        schema: name.0.pop().map(ident),
        database: name.0.pop().map(ident),
    };
    assert!(name.0.is_empty());
    Ok(out)
}

/// Normalizes an unresolved schema name.
pub fn unresolved_schema_name(
    mut name: UnresolvedSchemaName,
) -> Result<PartialSchemaName, PlanError> {
    if name.0.len() < 1 || name.0.len() > 2 {
        return Err(PlanError::MisqualifiedName(name.to_string()));
    }
    let out = PartialSchemaName {
        schema: ident(
            name.0
                .pop()
                .expect("name checked to have at least one component"),
        ),
        database: name.0.pop().map(ident),
    };
    assert!(name.0.is_empty());
    Ok(out)
}

/// Normalizes an operator reference.
///
/// Qualified operators outside of the pg_catalog schema are rejected.
pub fn op(op: &Op) -> Result<&str, PlanError> {
    if !op.namespace.is_empty()
        && (op.namespace.len() != 1 || op.namespace[0].as_str() != "pg_catalog")
    {
        sql_bail!(
            "operator does not exist: {}.{}",
            op.namespace.iter().map(|n| n.to_string()).join("."),
            op.op,
        )
    }
    Ok(&op.op)
}

#[derive(Debug, Clone)]
pub enum SqlValueOrSecret {
    Value(Value),
    Secret(GlobalId),
}

impl fmt::Display for SqlValueOrSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlValueOrSecret::Value(v) => write!(f, "{}", v),
            SqlValueOrSecret::Secret(id) => write!(f, "{}", id),
        }
    }
}

impl From<SqlValueOrSecret> for Option<Value> {
    fn from(s: SqlValueOrSecret) -> Self {
        match s {
            SqlValueOrSecret::Value(v) => Some(v),
            SqlValueOrSecret::Secret(_id) => None,
        }
    }
}

/// Normalizes a list of `WITH` options.
///
/// # Errors
/// - If any `WithOption`'s `value` is `None`. You can prevent generating these
///   values during parsing.
/// - If any `WithOption` has a value of type `WithOptionValue::Secret`.
pub fn options(
    options: &[WithOption<Aug>],
) -> Result<BTreeMap<String, SqlValueOrSecret>, PlanError> {
    let mut out = BTreeMap::new();
    for option in options {
        let value = match &option.value {
            Some(WithOptionValue::Value(value)) => SqlValueOrSecret::Value(value.clone()),
            Some(WithOptionValue::Ident(id)) => {
                SqlValueOrSecret::Value(Value::String(ident(id.clone())))
            }
            Some(WithOptionValue::DataType(data_type)) => {
                SqlValueOrSecret::Value(Value::String(data_type.to_ast_string()))
            }
            Some(WithOptionValue::Secret(ResolvedObjectName::Object { id, .. })) => {
                SqlValueOrSecret::Secret(*id)
            }
            Some(WithOptionValue::Secret(_)) => {
                panic!("SECRET option {} must be Object", option.key)
            }
            None => {
                sql_bail!("option {} requires a value", option.key);
            }
        };
        out.insert(option.key.to_string(), value);
    }
    Ok(out)
}

/// Normalizes `WITH` option keys without normalizing their corresponding
/// values.
///
/// # Panics
/// - If any `WithOption`'s `value` is `None`. You can prevent generating these
///   values during parsing.
pub fn option_objects(options: &[WithOption<Aug>]) -> BTreeMap<String, WithOptionValue<Aug>> {
    options
        .iter()
        .map(|o| {
            (
                ident(o.key.clone()),
                o.value
                    .as_ref()
                    .clone()
                    // The only places that generate options that do not require
                    // keys and values do not currently use this code path.
                    .expect("check that all entries have values before calling `option_objects`")
                    .clone(),
            )
        })
        .collect()
}

/// Unnormalizes an object name.
///
/// This is the inverse of the [`unresolved_object_name`] function.
pub fn unresolve(name: FullObjectName) -> UnresolvedObjectName {
    let mut out = vec![];
    if let RawDatabaseSpecifier::Name(n) = name.database {
        out.push(Ident::new(n));
    }
    out.push(Ident::new(name.schema));
    out.push(Ident::new(name.item));
    UnresolvedObjectName(out)
}

/// Converts an `UnresolvedObjectName` to a `FullObjectName` if the
/// `UnresolvedObjectName` is fully specified. Otherwise returns an error.
pub fn full_name(mut raw_name: UnresolvedObjectName) -> Result<FullObjectName, PlanError> {
    match raw_name.0.len() {
        3 => Ok(FullObjectName {
            item: ident(raw_name.0.pop().unwrap()),
            schema: ident(raw_name.0.pop().unwrap()),
            database: RawDatabaseSpecifier::Name(ident(raw_name.0.pop().unwrap())),
        }),
        2 => Ok(FullObjectName {
            item: ident(raw_name.0.pop().unwrap()),
            schema: ident(raw_name.0.pop().unwrap()),
            database: RawDatabaseSpecifier::Ambient,
        }),
        _ => sql_bail!("unresolved name {} not fully qualified", raw_name),
    }
}

/// Normalizes a `CREATE` statement.
///
/// The resulting statement will not depend upon any session parameters, nor
/// specify any non-default options (like `TEMPORARY`, `IF NOT EXISTS`, etc).
///
/// The goal is to construct a backwards-compatible description of the object.
/// SQL is the most stable part of Materialize, so SQL is used to describe the
/// objects that are persisted in the catalog.
pub fn create_statement(
    scx: &StatementContext,
    mut stmt: Statement<Aug>,
) -> Result<String, PlanError> {
    let allocate_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_full_name(
            unresolved_object_name(name.clone())?,
        )?))
    };

    let allocate_temporary_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_temporary_full_name(
            unresolved_object_name(name.clone())?,
        )))
    };

    fn normalize_function_name(
        scx: &StatementContext,
        name: &mut UnresolvedObjectName,
    ) -> Result<(), PlanError> {
        let item = scx.resolve_function(name.clone())?;
        *name = unresolve(scx.catalog.resolve_full_name(item.name()));
        Ok(())
    }

    struct QueryNormalizer<'a> {
        scx: &'a StatementContext<'a>,
        ctes: Vec<Ident>,
        err: Option<PlanError>,
    }

    impl<'a> QueryNormalizer<'a> {
        fn new(scx: &'a StatementContext<'a>) -> QueryNormalizer<'a> {
            QueryNormalizer {
                scx,
                ctes: vec![],
                err: None,
            }
        }
    }

    impl<'a, 'ast> VisitMut<'ast, Aug> for QueryNormalizer<'a> {
        fn visit_query_mut(&mut self, query: &'ast mut Query<Aug>) {
            let n = self.ctes.len();
            for cte in &query.ctes {
                self.ctes.push(cte.alias.name.clone());
            }
            visit_mut::visit_query_mut(self, query);
            self.ctes.truncate(n);
        }

        fn visit_function_mut(&mut self, func: &'ast mut Function<Aug>) {
            if let Err(e) = normalize_function_name(self.scx, &mut func.name) {
                self.err = Some(e);
                return;
            }

            match &mut func.args {
                FunctionArgs::Star => (),
                FunctionArgs::Args { args, order_by } => {
                    for arg in args {
                        self.visit_expr_mut(arg);
                    }
                    for expr in order_by {
                        self.visit_order_by_expr_mut(expr);
                    }
                }
            }
            if let Some(over) = &mut func.over {
                self.visit_window_spec_mut(over);
            }
        }

        fn visit_table_function_mut(&mut self, func: &'ast mut TableFunction<Aug>) {
            if let Err(e) = normalize_function_name(self.scx, &mut func.name) {
                self.err = Some(e);
                return;
            }

            match &mut func.args {
                FunctionArgs::Star => (),
                FunctionArgs::Args { args, order_by } => {
                    for arg in args {
                        self.visit_expr_mut(arg);
                    }
                    for expr in order_by {
                        self.visit_order_by_expr_mut(expr);
                    }
                }
            }
        }

        fn visit_table_factor_mut(&mut self, table_factor: &'ast mut TableFactor<Aug>) {
            match table_factor {
                TableFactor::Table { name, alias, .. } => {
                    self.visit_object_name_mut(name);
                    if let Some(alias) = alias {
                        self.visit_table_alias_mut(alias);
                    }
                }
                // We only need special behavior for `TableFactor::Table`.
                // Just visit the other types of table factors like normal.
                _ => visit_mut::visit_table_factor_mut(self, table_factor),
            }
        }
    }

    // Think very hard before changing any of the branches in this match
    // statement. All identifiers must be quoted. All object names must be
    // allocated or resolved, depending on whether they are the object created
    // by the statement or an object depended upon by the statement. All
    // non-default options must be disabled.
    //
    // Wildcard matches are explicitly avoided so that future additions to the
    // syntax cause compile errors here. Before you ignore a new field, triple
    // check that it does not need to be normalized according to the rules
    // above.
    match &mut stmt {
        Statement::CreateSource(CreateSourceStatement {
            name,
            col_names: _,
            connection: _,
            legacy_with_options: _,
            format: _,
            include_metadata: _,
            envelope: _,
            if_not_exists,
            key_constraint: _,
            with_options: _,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            constraints: _,
            with_options: _,
            if_not_exists,
            temporary,
        }) => {
            *name = if *temporary {
                allocate_temporary_name(name)?
            } else {
                allocate_name(name)?
            };
            let mut normalizer = QueryNormalizer::new(scx);
            for c in columns {
                normalizer.visit_column_def_mut(c);
            }
            if let Some(err) = normalizer.err {
                return Err(err);
            }
            *if_not_exists = false;
        }

        Statement::CreateSink(CreateSinkStatement {
            name,
            connection: _,
            with_options: _,
            format: _,
            envelope: _,
            with_snapshot: _,
            as_of: _,
            if_not_exists,
            ..
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        Statement::CreateView(CreateViewStatement {
            temporary,
            if_exists,
            definition:
                ViewDefinition {
                    name,
                    query,
                    columns: _,
                },
        }) => {
            *name = if *temporary {
                allocate_temporary_name(name)?
            } else {
                allocate_name(name)?
            };
            {
                let mut normalizer = QueryNormalizer::new(scx);
                normalizer.visit_query_mut(query);
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            *if_exists = IfExistsBehavior::Error;
        }

        Statement::CreateMaterializedView(CreateMaterializedViewStatement {
            if_exists,
            name,
            columns: _,
            in_cluster: _,
            query,
        }) => {
            *name = allocate_name(name)?;
            {
                let mut normalizer = QueryNormalizer::new(scx);
                normalizer.visit_query_mut(query);
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            *if_exists = IfExistsBehavior::Error;
        }

        Statement::CreateIndex(CreateIndexStatement {
            name: _,
            in_cluster: _,
            key_parts,
            with_options: _,
            if_not_exists,
            ..
        }) => {
            let mut normalizer = QueryNormalizer::new(scx);
            if let Some(key_parts) = key_parts {
                for key_part in key_parts {
                    normalizer.visit_expr_mut(key_part);
                    if let Some(err) = normalizer.err {
                        return Err(err);
                    }
                }
            }
            *if_not_exists = false;
        }

        Statement::CreateType(CreateTypeStatement { name, as_type, .. }) => match as_type {
            CreateTypeAs::List { with_options } | CreateTypeAs::Map { with_options } => {
                *name = allocate_name(name)?;
                let mut normalizer = QueryNormalizer::new(scx);
                for option in with_options {
                    match &mut option.value {
                        Some(WithOptionValue::DataType(ref mut data_type)) => {
                            normalizer.visit_data_type_mut(data_type);
                        }
                        _ => unreachable!(),
                    }
                }
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
            CreateTypeAs::Record { column_defs } => {
                let mut normalizer = QueryNormalizer::new(scx);
                for c in column_defs {
                    normalizer.visit_column_def_mut(c);
                }
                if let Some(err) = normalizer.err {
                    return Err(err);
                }
            }
        },
        Statement::CreateSecret(CreateSecretStatement {
            name,
            if_not_exists,
            value: _,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }
        Statement::CreateConnection(CreateConnectionStatement {
            name,
            connection: _,
            if_not_exists,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        _ => unreachable!(),
    }

    Ok(stmt.to_ast_string_stable())
}

/// Generates a struct capable of taking a `Vec` of types commonly used to
/// represent `WITH` options into useful data types, such as strings.
///
/// # Parameters
/// - `$option_ty`: Accepts a struct representing a set of `WITH` options, which
///     must contain the fields `name` and `value`.
///     - `name` must be of type `$option_tyName`, e.g. if `$option_ty` is
///       `FooOption`, then `name` must be of type `FooOptionName`.
///       `$option_tyName` must be an enum representing `WITH` option keys.
///     - `TryFromValue<value>` must be implemented for the type you want to
///       take the option to. The `sql::plan::with_option` module contains these
///       implementations.
/// - `$option_name` must be an element of `$option_tyName`
/// - `$t` is the type you want to convert the option's value to. If the
///   option's value is absent (i.e. the user only entered the option's key),
///   you can also define a default value.
/// - `Default($v)` is an optional parameter that sets the default value of the
///   field to `$v`. `$v` must be convertible to `$t` using `.into`. This also
///   converts the struct's type from `Option<$t>` to `<$t>`.
macro_rules! generate_extracted_config {
    // No default specified, have remaining options.
    ($option_ty:ty, [$($processed:tt)*], ($option_name:path, $t:ty), $($tail:tt),*) => {
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, Option<$t>, None)], $(
            $tail
        ),*);
    };
    // No default specified, no remaining options.
    ($option_ty:ty, [$($processed:tt)*], ($option_name:path, $t:ty)) => {
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, Option<$t>, None)]);
    };
    // Default specified, have remaining options.
    ($option_ty:ty, [$($processed:tt)*], ($option_name:path, $t:ty, Default($v:expr)), $($tail:tt),*) => {
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, $t, $v)], $(
            $tail
        ),*);
    };
    // Default specified, no remaining options.
    ($option_ty:ty, [$($processed:tt)*], ($option_name:path, $t:ty, Default($v:expr))) => {
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, $t, $v)]);
    };
    ($option_ty:ty, [$(($option_name:path, $t:ty, $v:expr))+]) => {
        paste::paste! {
            #[derive(Debug)]
            pub struct [<$option_ty Extracted>] {
                seen: HashSet::<[<$option_ty Name>]>,
                $(
                    [<$option_name:snake>]: $t,
                )*
            }

            impl std::default::Default for [<$option_ty Extracted>] {
                fn default() -> Self {
                    [<$option_ty Extracted>] {
                        seen: HashSet::<[<$option_ty Name>]>::new(),
                        $(
                            [<$option_name:snake>]: $v.into(),
                        )*
                    }
                }
            }

            impl std::convert::TryFrom<Vec<$option_ty<Aug>>> for [<$option_ty Extracted>] {
                type Error = $crate::plan::PlanError;
                fn try_from(v: Vec<$option_ty<Aug>>) -> Result<[<$option_ty Extracted>], Self::Error> {
                    use [<$option_ty Name>]::*;
                    let mut extracted = [<$option_ty Extracted>]::default();
                    for option in v {
                        if !extracted.seen.insert(option.name.clone()) {
                            sql_bail!("{} specified more than once", option.name.to_ast_string());
                        }
                        match option.name {
                            $(
                                $option_name => {
                                    extracted.[<$option_name:snake>] =
                                        <$t>::try_from_value(option.value)
                                            .map_err(|e| sql_err!("invalid {}: {}", option.name.to_ast_string(), e))?;
                                }
                            )*
                        }
                    }
                    Ok(extracted)
                }
            }
        }
    };
    ($option_ty:ty, $($h:tt),+) => {
        generate_extracted_config!{$option_ty, [], $($h),+}
    };
}

pub(crate) use generate_extracted_config;

/// Ensures that the given set of options are empty, useful for validating that
/// `WITH` options are all real, used options
pub(crate) fn ensure_empty_options<V>(
    with_options: &BTreeMap<String, V>,
    context: &str,
) -> Result<(), PlanError> {
    if !with_options.is_empty() {
        sql_bail!(
            "unexpected parameters for {}: {}",
            context,
            with_options.keys().join(",")
        )
    }
    Ok(())
}

/// Normalizes option values that contain AWS connection parameters.
pub fn aws_config(
    options: &mut BTreeMap<String, SqlValueOrSecret>,
    region: Option<String>,
    credentials: AwsCredentials,
) -> Result<AwsConfig, PlanError> {
    let mut extract = |key| match options.remove(key) {
        // TODO: support secrets in S3
        Some(SqlValueOrSecret::Value(Value::String(key))) => {
            if !key.is_empty() {
                Ok(Some(key))
            } else {
                Ok(None)
            }
        }
        Some(_) => sql_bail!("{} must be a string", key),
        _ => Ok(None),
    };

    let region = match region {
        Some(region) => Some(region),
        None => extract("region")?,
    };
    let endpoint = match extract("endpoint")? {
        None => None,
        Some(endpoint) => Some(SerdeUri(
            endpoint
                .parse()
                .map_err(|e| sql_err!("parsing AWS endpoint: {e}"))?,
        )),
    };
    let arn = extract("role_arn")?;
    Ok(AwsConfig {
        credentials,
        region,
        endpoint,
        role: arn.map(|arn| AwsAssumeRole { arn }),
    })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use mz_ore::collections::CollectionExt;

    use super::*;
    use crate::catalog::DummyCatalog;
    use crate::names;

    #[test]
    fn normalized_create() -> Result<(), Box<dyn Error>> {
        let scx = &mut StatementContext::new(None, &DummyCatalog);

        let parsed = mz_sql_parser::parser::parse_statements("create view foo as select 1 as bar")?
            .into_element();

        let (stmt, _) = names::resolve(scx.catalog, parsed)?;

        // Ensure that all identifiers are quoted.
        assert_eq!(
            r#"CREATE VIEW "dummy"."public"."foo" AS SELECT 1 AS "bar""#,
            create_statement(scx, stmt)?,
        );

        Ok(())
    }
}
