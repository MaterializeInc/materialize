// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conversions between project AST/types and the in-memory catalog's SQL form.
//!
//! Pure transformations: no catalog state, no I/O. Used by `bootstrap` (Phase 1)
//! and the per-task work closure (Phase 2) to bridge between the project's
//! compiled AST, the parser AST that `mz_sql::names::resolve` accepts, and the
//! column-map representation stored in the build artifact.

use crate::client::quote_identifier;
use crate::project::ast::Statement as ProjectStatement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::object_id::ObjectId;
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::types::ColumnType;
use mz_repr::{RelationDesc, SqlColumnType, SqlScalarType};
use mz_sql_parser::ast::ColumnOption;
use std::collections::BTreeMap;

/// Build the stub table statement used to restore a cached dependency.
pub(super) fn create_stub_table_sql(
    object_id: &ObjectId,
    columns: &BTreeMap<String, ColumnType>,
) -> String {
    let mut col_defs = Vec::new();
    for (col_name, col_type) in columns {
        let nullable = if col_type.nullable { "" } else { " NOT NULL" };
        col_defs.push(format!(
            "{} {}{}",
            quote_identifier(col_name),
            col_type.r#type,
            nullable
        ));
    }

    let prefix = match object_id.database() {
        Some(db) => format!(
            "{}.{}.{}",
            quote_identifier(db),
            quote_identifier(object_id.schema()),
            quote_identifier(object_id.object()),
        ),
        None => format!(
            "{}.{}",
            quote_identifier(object_id.schema()),
            quote_identifier(object_id.object()),
        ),
    };
    format!("CREATE TABLE {} ({})", prefix, col_defs.join(", "))
}

/// Transform a compiled statement into SQL for the private catalog workspace.
pub(super) fn create_catalog_item_sql(
    stmt: &ProjectStatement,
    fqn: &FullyQualifiedName,
) -> Option<String> {
    create_catalog_item_statement(stmt, fqn).map(|stmt| stmt.to_string())
}

/// Transform a compiled statement into a parser AST for the private catalog
/// workspace, skipping the SQL render+reparse round-trip.
pub(super) fn create_catalog_item_ast(
    stmt: &ProjectStatement,
    fqn: &FullyQualifiedName,
) -> Option<mz_sql_parser::ast::Statement<mz_sql_parser::ast::Raw>> {
    create_catalog_item_statement(stmt, fqn).map(|s| s.into_parser_statement())
}

/// Normalize a compiled SQL statement for the in-memory catalog.
///
/// Strips properties irrelevant to typechecking (cluster assignments, table
/// constraints, options). Returns `None` for statement types that don't
/// produce typecheckable items (e.g., `CREATE TABLE FROM SOURCE`).
fn create_catalog_item_statement(
    stmt: &ProjectStatement,
    fqn: &FullyQualifiedName,
) -> Option<ProjectStatement> {
    let mut visitor = NormalizingVisitor::fully_qualifying(fqn);

    match stmt {
        ProjectStatement::CreateView(view) => {
            let mut view = view.clone();
            view.temporary = false;

            Some(
                ProjectStatement::CreateView(view)
                    .normalize_name_with(&visitor, &fqn.to_item_name())
                    .normalize_dependencies_with(&mut visitor),
            )
        }
        ProjectStatement::CreateMaterializedView(mv) => {
            let mut mv = mv.clone();
            mv.in_cluster = None;
            mv.in_cluster_replica = None;

            Some(
                ProjectStatement::CreateMaterializedView(mv)
                    .normalize_name_with(&visitor, &fqn.to_item_name())
                    .normalize_dependencies_with(&mut visitor),
            )
        }
        ProjectStatement::CreateTable(table) => {
            let mut table = table.clone();
            table.temporary = false;
            table.constraints.clear();
            table.with_options.clear();
            for col in &mut table.columns {
                col.options.retain(|opt| {
                    !matches!(
                        opt.option,
                        ColumnOption::ForeignKey { .. } | ColumnOption::Check(_)
                    )
                });
            }
            Some(
                ProjectStatement::CreateTable(table)
                    .normalize_name_with(&visitor, &fqn.to_item_name()),
            )
        }
        ProjectStatement::CreateTableFromSource(_) => None,
        _ => None,
    }
}

/// Convert a relation description into the column map stored in the build artifact database.
pub(super) fn relation_desc_to_columns(desc: &RelationDesc) -> BTreeMap<String, ColumnType> {
    desc.iter()
        .enumerate()
        .map(|(position, (name, col_type))| {
            (
                name.as_str().to_string(),
                ColumnType {
                    r#type: sql_column_type_to_sql(col_type),
                    nullable: col_type.nullable,
                    position,
                    comment: None,
                },
            )
        })
        .collect()
}

/// Convert a column type to its SQL type name string.
fn sql_column_type_to_sql(column_type: &SqlColumnType) -> String {
    sql_scalar_type_to_sql(&column_type.scalar_type)
}

/// Convert a Materialize scalar type to its SQL type name string.
/// Handles parameterized types (precision, length, element types).
fn sql_scalar_type_to_sql(scalar_type: &SqlScalarType) -> String {
    match scalar_type {
        SqlScalarType::Bool => "bool".into(),
        SqlScalarType::Int16 => "int2".into(),
        SqlScalarType::Int32 => "int4".into(),
        SqlScalarType::Int64 => "int8".into(),
        SqlScalarType::UInt16 => "uint2".into(),
        SqlScalarType::UInt32 => "uint4".into(),
        SqlScalarType::UInt64 => "uint8".into(),
        SqlScalarType::Float32 => "float4".into(),
        SqlScalarType::Float64 => "float8".into(),
        SqlScalarType::Numeric { max_scale } => match max_scale {
            None => "numeric".into(),
            Some(max_scale) => format!("numeric({})", max_scale.into_u8()),
        },
        SqlScalarType::Date => "date".into(),
        SqlScalarType::Time => "time".into(),
        SqlScalarType::Timestamp { precision } => match precision {
            None => "timestamp".into(),
            Some(precision) => format!("timestamp({})", precision.into_u8()),
        },
        SqlScalarType::TimestampTz { precision } => match precision {
            None => "timestamptz".into(),
            Some(precision) => format!("timestamptz({})", precision.into_u8()),
        },
        SqlScalarType::Interval => "interval".into(),
        SqlScalarType::PgLegacyChar => "\"char\"".into(),
        SqlScalarType::PgLegacyName => "name".into(),
        SqlScalarType::Bytes => "bytea".into(),
        SqlScalarType::String => "text".into(),
        SqlScalarType::Char { length } => match length {
            None => "char".into(),
            Some(length) => format!("char({})", length.into_u32()),
        },
        SqlScalarType::VarChar { max_length } => match max_length {
            None => "varchar".into(),
            Some(length) => format!("varchar({})", length.into_u32()),
        },
        SqlScalarType::Jsonb => "jsonb".into(),
        SqlScalarType::Uuid => "uuid".into(),
        SqlScalarType::Array(element_type) => format!("{}[]", sql_scalar_type_to_sql(element_type)),
        SqlScalarType::List { element_type, .. } => {
            format!("{} list", sql_scalar_type_to_sql(element_type))
        }
        SqlScalarType::Map { value_type, .. } => {
            format!("map[text=>{}]", sql_scalar_type_to_sql(value_type))
        }
        SqlScalarType::Oid => "oid".into(),
        SqlScalarType::RegProc => "regproc".into(),
        SqlScalarType::RegType => "regtype".into(),
        SqlScalarType::RegClass => "regclass".into(),
        SqlScalarType::Int2Vector => "int2vector".into(),
        SqlScalarType::MzTimestamp => "mz_timestamp".into(),
        SqlScalarType::Range { element_type } => {
            format!("range({})", sql_scalar_type_to_sql(element_type))
        }
        SqlScalarType::MzAclItem => "mz_aclitem".into(),
        SqlScalarType::AclItem => "aclitem".into(),
        SqlScalarType::Record { .. } => "record".into(),
    }
}
