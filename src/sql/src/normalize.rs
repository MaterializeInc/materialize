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

use std::fmt;

use itertools::Itertools;

use mz_repr::{ColumnName, GlobalId};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    CreateConnectionStatement, CreateIndexStatement, CreateMaterializedViewStatement,
    CreateSecretStatement, CreateSinkStatement, CreateSourceStatement, CreateSubsourceStatement,
    CreateTableStatement, CreateTypeStatement, CreateViewStatement, CteBlock, Function,
    FunctionArgs, Ident, IfExistsBehavior, MutRecBlock, Op, Query, Statement, TableFactor,
    TableFunction, UnresolvedItemName, UnresolvedSchemaName, Value, ViewDefinition,
};

use crate::names::{Aug, FullItemName, PartialItemName, PartialSchemaName, RawDatabaseSpecifier};
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
pub fn unresolved_item_name(mut name: UnresolvedItemName) -> Result<PartialItemName, PlanError> {
    if name.0.len() < 1 || name.0.len() > 3 {
        return Err(PlanError::MisqualifiedName(name.to_string()));
    }
    let out = PartialItemName {
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

/// Unnormalizes an item name.
///
/// This is the inverse of the [`unresolved_item_name`] function.
pub fn unresolve(name: FullItemName) -> UnresolvedItemName {
    let mut out = vec![];
    if let RawDatabaseSpecifier::Name(n) = name.database {
        out.push(Ident::new(n));
    }
    out.push(Ident::new(name.schema));
    out.push(Ident::new(name.item));
    UnresolvedItemName(out)
}

/// Converts an `UnresolvedItemName` to a `FullItemName` if the
/// `UnresolvedItemName` is fully specified. Otherwise returns an error.
pub fn full_name(mut raw_name: UnresolvedItemName) -> Result<FullItemName, PlanError> {
    match raw_name.0.len() {
        3 => Ok(FullItemName {
            item: ident(raw_name.0.pop().unwrap()),
            schema: ident(raw_name.0.pop().unwrap()),
            database: RawDatabaseSpecifier::Name(ident(raw_name.0.pop().unwrap())),
        }),
        2 => Ok(FullItemName {
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
/// The goal is to construct a backwards-compatible description of the item.
/// SQL is the most stable part of Materialize, so SQL is used to describe the
/// items that are persisted in the catalog.
pub fn create_statement(
    scx: &StatementContext,
    mut stmt: Statement<Aug>,
) -> Result<String, PlanError> {
    let allocate_name = |name: &UnresolvedItemName| -> Result<_, PlanError> {
        Ok(unresolve(
            scx.allocate_full_name(unresolved_item_name(name.clone())?)?,
        ))
    };

    let allocate_temporary_name = |name: &UnresolvedItemName| -> Result<_, PlanError> {
        Ok(unresolve(scx.allocate_temporary_full_name(
            unresolved_item_name(name.clone())?,
        )))
    };

    struct QueryNormalizer {
        ctes: Vec<Ident>,
        err: Option<PlanError>,
    }

    impl QueryNormalizer {
        fn new() -> QueryNormalizer {
            QueryNormalizer {
                ctes: vec![],
                err: None,
            }
        }
    }

    impl<'ast> VisitMut<'ast, Aug> for QueryNormalizer {
        fn visit_query_mut(&mut self, query: &'ast mut Query<Aug>) {
            let n = self.ctes.len();
            match &query.ctes {
                CteBlock::Simple(ctes) => {
                    for cte in ctes.iter() {
                        self.ctes.push(cte.alias.name.clone());
                    }
                }
                CteBlock::MutuallyRecursive(MutRecBlock { options: _, ctes }) => {
                    for cte in ctes.iter() {
                        self.ctes.push(cte.name.clone());
                    }
                }
            }
            visit_mut::visit_query_mut(self, query);
            self.ctes.truncate(n);
        }

        fn visit_function_mut(&mut self, func: &'ast mut Function<Aug>) {
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
                    self.visit_item_name_mut(name);
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
            in_cluster: _,
            col_names: _,
            connection: _,
            format: _,
            include_metadata: _,
            envelope: _,
            if_not_exists,
            key_constraint: _,
            with_options: _,
            referenced_subsources: _,
            progress_subsource: _,
        }) => {
            *name = allocate_name(name)?;
            *if_not_exists = false;
        }

        Statement::CreateSubsource(CreateSubsourceStatement {
            name,
            columns,
            constraints: _,
            if_not_exists,
            with_options: _,
        }) => {
            *name = allocate_name(name)?;
            let mut normalizer = QueryNormalizer::new();
            for c in columns {
                normalizer.visit_column_def_mut(c);
            }
            if let Some(err) = normalizer.err {
                return Err(err);
            }
            *if_not_exists = false;
        }

        Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            constraints: _,
            if_not_exists,
            temporary,
        }) => {
            *name = if *temporary {
                allocate_temporary_name(name)?
            } else {
                allocate_name(name)?
            };
            let mut normalizer = QueryNormalizer::new();
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
            in_cluster: _,
            connection: _,
            format: _,
            envelope: _,
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
                let mut normalizer = QueryNormalizer::new();
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
                let mut normalizer = QueryNormalizer::new();
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
            let mut normalizer = QueryNormalizer::new();
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

        Statement::CreateType(CreateTypeStatement { name, as_type }) => {
            *name = allocate_name(name)?;
            let mut normalizer = QueryNormalizer::new();
            normalizer.visit_create_type_as_mut(as_type);
            if let Some(err) = normalizer.err {
                return Err(err);
            }
        }
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
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, Option::<$t>, None)], $(
            $tail
        ),*);
    };
    // No default specified, no remaining options.
    ($option_ty:ty, [$($processed:tt)*], ($option_name:path, $t:ty)) => {
        generate_extracted_config!($option_ty, [$($processed)* ($option_name, Option::<$t>, None)]);
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
                seen: ::std::collections::BTreeSet::<[<$option_ty Name>]>,
                $(
                    pub(crate) [<$option_name:snake>]: $t,
                )*
            }

            impl std::default::Default for [<$option_ty Extracted>] {
                fn default() -> Self {
                    [<$option_ty Extracted>] {
                        seen: ::std::collections::BTreeSet::<[<$option_ty Name>]>::new(),
                        $(
                            [<$option_name:snake>]: $t::from($v),
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
