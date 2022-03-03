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

use anyhow::{bail, Context};
use itertools::Itertools;

use mz_dataflow_types::sources::{AwsAssumeRole, AwsConfig, AwsCredentials, SerdeUri};
use mz_repr::ColumnName;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    AstInfo, CreateIndexStatement, CreateSinkStatement, CreateSourceStatement,
    CreateTableStatement, CreateTypeAs, CreateTypeStatement, CreateViewStatement, Function,
    FunctionArgs, Ident, IfExistsBehavior, Op, Query, Raw, SqlOption, Statement, TableFactor,
    TableFunction, UnresolvedObjectName, Value, ViewDefinition,
};

use crate::names::{resolve_names_stmt, Aug, DatabaseSpecifier, FullName, PartialName};
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;

/// Normalizes a single identifier.
pub fn ident(ident: Ident) -> String {
    ident.as_str().into()
}

/// Normalizes an identifier that represents a column name.
pub fn column_name(id: Ident) -> ColumnName {
    ColumnName::from(ident(id))
}

/// Normalizes an unresolved object name.
pub fn unresolved_object_name(mut name: UnresolvedObjectName) -> Result<PartialName, PlanError> {
    if name.0.len() < 1 || name.0.len() > 3 {
        return Err(PlanError::MisqualifiedName(name.to_string()));
    }
    let out = PartialName {
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

/// Normalizes a list of `WITH` options.
pub fn options<T: AstInfo>(options: &[SqlOption<T>]) -> BTreeMap<String, Value> {
    options
        .iter()
        .map(|o| match o {
            SqlOption::Value { name, value } => (ident(name.clone()), value.clone()),
            SqlOption::ObjectName { name, object_name } => (
                ident(name.clone()),
                Value::String(object_name.to_ast_string()),
            ),
            SqlOption::DataType { name, data_type } => (
                ident(name.clone()),
                Value::String(data_type.to_ast_string()),
            ),
        })
        .collect()
}

/// Normalizes `WITH` option keys without normalizing their corresponding
/// values.
pub fn option_objects(options: &[SqlOption<Raw>]) -> BTreeMap<String, SqlOption<Raw>> {
    options
        .iter()
        .map(|o| (ident(o.name().clone()), o.clone()))
        .collect()
}

/// Unnormalizes an object name.
///
/// This is the inverse of the [`unresolved_object_name`] function.
pub fn unresolve(name: FullName) -> UnresolvedObjectName {
    let mut out = vec![];
    if let DatabaseSpecifier::Name(n) = name.database {
        out.push(Ident::new(n));
    }
    out.push(Ident::new(name.schema));
    out.push(Ident::new(name.item));
    UnresolvedObjectName(out)
}

/// Normalizes a `CREATE` statement.
///
/// The resulting statement will not depend upon any session parameters, nor
/// specify any non-default options (like `MATERIALIZED`, `IF NOT EXISTS`, etc).
///
/// The goal is to construct a backwards-compatible description of the object.
/// SQL is the most stable part of Materialize, so SQL is used to describe the
/// objects that are persisted in the catalog.
pub fn create_statement(
    scx: &StatementContext,
    stmt: Statement<Raw>,
) -> Result<String, anyhow::Error> {
    let mut stmt = resolve_names_stmt(scx.catalog, stmt)?;

    let allocate_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(unresolve(
            scx.allocate_name(unresolved_object_name(name.clone())?),
        ))
    };

    let allocate_temporary_name = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_temporary_name(
            unresolved_object_name(name.clone())?,
        )))
    };

    let resolve_item = |name: &UnresolvedObjectName| -> Result<_, PlanError> {
        let item = scx.resolve_item(name.clone())?;
        Ok(unresolve(item.name().clone()))
    };

    fn normalize_function_name(
        scx: &StatementContext,
        name: &mut UnresolvedObjectName,
    ) -> Result<(), PlanError> {
        let full_name = scx.resolve_function(name.clone())?;
        *name = unresolve(full_name.name().clone());
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

        fn visit_unresolved_object_name_mut(
            &mut self,
            unresolved_object_name: &'ast mut UnresolvedObjectName,
        ) {
            // Single-part object names can refer to CTEs in addition to
            // catalog objects.
            if let [ident] = unresolved_object_name.0.as_slice() {
                if self.ctes.contains(ident) {
                    return;
                }
            }
            match self.scx.resolve_item(unresolved_object_name.clone()) {
                Ok(full_name) => *unresolved_object_name = unresolve(full_name.name().clone()),
                Err(e) => self.err = Some(e),
            };
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
            connector: _,
            with_options: _,
            format: _,
            include_metadata: _,
            envelope: _,
            if_not_exists,
            materialized,
            key_constraint: _,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
            *materialized = false;
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
                return Err(err.into());
            }
            *if_not_exists = false;
        }

        Statement::CreateSink(CreateSinkStatement {
            name,
            from,
            connector: _,
            with_options: _,
            format: _,
            envelope: _,
            with_snapshot: _,
            as_of: _,
            if_not_exists,
        }) => {
            *name = allocate_name(name)?;
            *from = resolve_item(from)?;
            *if_not_exists = false;
        }

        Statement::CreateView(CreateViewStatement {
            temporary,
            materialized,
            if_exists,
            definition:
                ViewDefinition {
                    name,
                    query,
                    columns: _,
                    with_options: _,
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
                    return Err(err.into());
                }
            }
            *materialized = false;
            *if_exists = IfExistsBehavior::Error;
        }

        Statement::CreateIndex(CreateIndexStatement {
            name: _,
            on_name,
            key_parts,
            with_options: _,
            if_not_exists,
        }) => {
            *on_name = resolve_item(on_name)?;
            let mut normalizer = QueryNormalizer::new(scx);
            if let Some(key_parts) = key_parts {
                for key_part in key_parts {
                    normalizer.visit_expr_mut(key_part);
                    if let Some(err) = normalizer.err {
                        return Err(err.into());
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
                    match option {
                        SqlOption::DataType { data_type, .. } => {
                            normalizer.visit_data_type_mut(data_type);
                        }
                        _ => unreachable!(),
                    }
                }
                if let Some(err) = normalizer.err {
                    return Err(err.into());
                }
            }
            CreateTypeAs::Record { column_defs } => {
                let mut normalizer = QueryNormalizer::new(scx);
                for c in column_defs {
                    normalizer.visit_column_def_mut(c);
                }
                if let Some(err) = normalizer.err {
                    return Err(err.into());
                }
            }
        },

        _ => unreachable!(),
    }

    Ok(stmt.to_ast_string_stable())
}

macro_rules! with_option_type {
    ($name:ident, String) => {
        match $name {
            Some(crate::ast::WithOptionValue::Value(crate::ast::Value::String(value))) => value,
            Some(crate::ast::WithOptionValue::ObjectName(name)) => {
                crate::ast::display::AstDisplay::to_ast_string(&name)
            }
            _ => ::anyhow::bail!("expected String"),
        }
    };
    ($name:ident, bool) => {
        match $name {
            Some(crate::ast::WithOptionValue::Value(crate::ast::Value::Boolean(value))) => value,
            // Bools, if they have no '= value', are true.
            None => true,
            _ => ::anyhow::bail!("expected bool"),
        }
    };
    ($name:ident, Interval) => {
        match $name {
            Some(crate::ast::WithOptionValue::Value(Value::String(value))) => {
                mz_repr::strconv::parse_interval(&value)?
            }
            Some(crate::ast::WithOptionValue::Value(Value::Interval(interval))) => {
                mz_repr::strconv::parse_interval(&interval.value)?
            }
            _ => ::anyhow::bail!("expected Interval"),
        }
    };
}

/// Ensures that the given set of options are empty, useful for validating that
/// `WITH` options are all real, used options
pub(crate) fn ensure_empty_options<V>(
    with_options: &BTreeMap<String, V>,
    context: &str,
) -> Result<(), anyhow::Error> {
    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for {}: {}",
            context,
            with_options.keys().join(",")
        )
    }
    Ok(())
}

/// This macro accepts a struct definition and will generate it and a `try_from`
/// method that takes a `Vec<WithOption>` which will extract and type check
/// options based on the struct field names and types.
///
/// The macro wraps all field types in an `Option` in the generated struct. The
/// `TryFrom` implementation sets fields to `None` if they are not present in
/// the provided `WITH` options.
///
/// Field names must match exactly the lowercased option name. Supported types
/// are:
///
/// - `String`: expects a SQL string (`WITH (name = "value")`) or identifier
///   (`WITH (name = text)`).
/// - `bool`: expects either a SQL bool (`WITH (name = true)`) or a valueless
///   option which will be interpreted as true: (`WITH (name)`.
/// - `Interval`: expects either a SQL interval or string that can be parsed as
///   an interval.
macro_rules! with_options {
  (struct $name:ident {
        $($field_name:ident: $field_type:ident,)*
    }) => {
        #[derive(Debug)]
        pub struct $name {
            pub $($field_name: Option<$field_type>,)*
        }

        impl ::std::convert::TryFrom<Vec<crate::ast::WithOption>> for $name {
            type Error = anyhow::Error;

            fn try_from(mut options: Vec<crate::ast::WithOption>) -> Result<Self, Self::Error> {
                let v = Self {
                    $($field_name: {
                        match options.iter().position(|opt| opt.key.as_str() == stringify!($field_name)) {
                            None => None,
                            Some(pos) => {
                                let value: Option<crate::ast::WithOptionValue> = options.swap_remove(pos).value;
                                let value: $field_type = with_option_type!(value, $field_type);
                                Some(value)
                            },
                        }
                    },
                    )*
                };
                if !options.is_empty() {
                    ::anyhow::bail!("unexpected options");
                }
                Ok(v)
            }
        }
    }
}

/// Normalizes option values that contain AWS connection parameters.
pub fn aws_config(
    options: &mut BTreeMap<String, Value>,
    region: Option<String>,
) -> Result<AwsConfig, anyhow::Error> {
    let mut extract = |key| match options.remove(key) {
        Some(Value::String(key)) => {
            if !key.is_empty() {
                Ok(Some(key))
            } else {
                Ok(None)
            }
        }
        Some(_) => bail!("{} must be a string", key),
        _ => Ok(None),
    };

    let credentials = match extract("profile")? {
        Some(profile_name) => {
            for name in &["access_key_id", "secret_access_key", "token"] {
                let extracted = extract(name);
                if matches!(extracted, Ok(Some(_)) | Err(_)) {
                    bail!(
                        "AWS profile cannot be set in combination with '{0}', \
                         configure '{0}' inside the profile file",
                        name
                    );
                }
            }
            AwsCredentials::Profile { profile_name }
        }
        None => {
            let access_key_id = extract("access_key_id")?;
            let secret_access_key = extract("secret_access_key")?;
            let session_token = extract("token")?;
            let credentials = match (access_key_id, secret_access_key, session_token) {
                (None, None, None) => AwsCredentials::Default,
                (Some(access_key_id), Some(secret_access_key), session_token) => {
                    AwsCredentials::Static {
                        access_key_id,
                        secret_access_key,
                        session_token,
                    }
                }
                (Some(_), None, _) => {
                    bail!("secret_acccess_key must be specified if access_key_id is specified")
                }
                (None, Some(_), _) => {
                    bail!("secret_acccess_key cannot be specified without access_key_id")
                }
                (None, None, Some(_)) => bail!("token cannot be specified without access_key_id"),
            };

            credentials
        }
    };

    let region = match region {
        Some(region) => Some(region),
        None => extract("region")?,
    };
    let endpoint = match extract("endpoint")? {
        None => None,
        Some(endpoint) => Some(SerdeUri(endpoint.parse().context("parsing AWS endpoint")?)),
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

    #[test]
    fn normalized_create() -> Result<(), Box<dyn Error>> {
        let scx = &StatementContext::new(None, &DummyCatalog);

        let parsed = mz_sql_parser::parser::parse_statements(
            "create materialized view foo as select 1 as bar",
        )?
        .into_element();

        // Ensure that all identifiers are quoted.
        assert_eq!(
            r#"CREATE VIEW "dummy"."public"."foo" AS SELECT 1 AS "bar""#,
            create_statement(scx, parsed)?,
        );

        Ok(())
    }
}
