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
use crate::types::stub::{StubError, StubTarget, build_stub_statements};
use crate::types::{ColumnType, DataType, RecordField};
use mz_repr::adt::numeric::NUMERIC_DATUM_MAX_PRECISION;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql_parser::ast::ColumnOption;
use std::collections::BTreeMap;

/// The statements that restore a cached dependency as a relation.
///
/// Helper relations come first and the stub itself is last; execute them in
/// order.
pub(super) fn create_stub_statements(
    object_id: &ObjectId,
    columns: &BTreeMap<String, ColumnType>,
) -> Result<Vec<String>, StubError> {
    let qualification = match object_id.database() {
        Some(db) => format!(
            "{}.{}.",
            quote_identifier(db),
            quote_identifier(object_id.schema()),
        ),
        None => format!("{}.", quote_identifier(object_id.schema())),
    };
    let target = StubTarget {
        name: format!("{}{}", qualification, quote_identifier(object_id.object())),
        helper_prefix: qualification,
        // Helpers share the stub's schema. The suffix is what keeps them from
        // colliding with a project object, which is named after a `.sql` file
        // stem.
        helper_stem: format!("{}_mz_deploy_stub", object_id.object()),
    };
    build_stub_statements(object_id, &target, columns)
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

/// Convert a relation description into the column map stored in the build
/// artifact database.
pub(super) fn relation_desc_to_columns(desc: &RelationDesc) -> BTreeMap<String, ColumnType> {
    desc.iter()
        .enumerate()
        .map(|(position, (name, col_type))| {
            (
                name.as_str().to_string(),
                ColumnType {
                    r#type: sql_scalar_type_to_data_type(&col_type.scalar_type),
                    nullable: col_type.nullable,
                    position,
                    comment: None,
                },
            )
        })
        .collect()
}

/// Convert a Materialize scalar type into the contract's structured form.
///
/// NOTE: user-defined types are not represented. A named composite becomes the
/// `record` pseudo-token rather than an anonymous record with the same fields,
/// because `SqlScalarType::base_eq` treats the two as different types and
/// substituting one would change what typechecks. A named list or map is still
/// expanded structurally, which has the same flaw.
/// TODO: record user-defined types in the contract and recreate them.
fn sql_scalar_type_to_data_type(scalar_type: &SqlScalarType) -> DataType {
    let named = |name: &str| DataType::named(name);
    match scalar_type {
        SqlScalarType::Bool => named("bool"),
        SqlScalarType::Int16 => named("int2"),
        SqlScalarType::Int32 => named("int4"),
        SqlScalarType::Int64 => named("int8"),
        SqlScalarType::UInt16 => named("uint2"),
        SqlScalarType::UInt32 => named("uint4"),
        SqlScalarType::UInt64 => named("uint8"),
        SqlScalarType::Float32 => named("float4"),
        SqlScalarType::Float64 => named("float8"),
        SqlScalarType::Numeric { max_scale } => match max_scale {
            None => named("numeric"),
            Some(max_scale) => DataType::Named(format!(
                "numeric({},{})",
                NUMERIC_DATUM_MAX_PRECISION,
                max_scale.into_u8()
            )),
        },
        SqlScalarType::Date => named("date"),
        SqlScalarType::Time => named("time"),
        SqlScalarType::Timestamp { precision } => match precision {
            None => named("timestamp"),
            Some(precision) => DataType::Named(format!("timestamp({})", precision.into_u8())),
        },
        SqlScalarType::TimestampTz { precision } => match precision {
            None => named("timestamptz"),
            Some(precision) => DataType::Named(format!("timestamptz({})", precision.into_u8())),
        },
        SqlScalarType::Interval => named("interval"),
        SqlScalarType::PgLegacyChar => named("\"char\""),
        SqlScalarType::PgLegacyName => named("name"),
        SqlScalarType::Bytes => named("bytea"),
        SqlScalarType::String => named("text"),
        SqlScalarType::Char { length } => match length {
            None => named("char"),
            Some(length) => DataType::Named(format!("char({})", length.into_u32())),
        },
        SqlScalarType::VarChar { max_length } => match max_length {
            None => named("varchar"),
            Some(length) => DataType::Named(format!("varchar({})", length.into_u32())),
        },
        SqlScalarType::Jsonb => named("jsonb"),
        SqlScalarType::Uuid => named("uuid"),
        SqlScalarType::Array(element_type) => {
            DataType::Array(Box::new(sql_scalar_type_to_data_type(element_type)))
        }
        SqlScalarType::List { element_type, .. } => {
            DataType::List(Box::new(sql_scalar_type_to_data_type(element_type)))
        }
        SqlScalarType::Map { value_type, .. } => {
            DataType::Map(Box::new(sql_scalar_type_to_data_type(value_type)))
        }
        SqlScalarType::Oid => named("oid"),
        SqlScalarType::RegProc => named("regproc"),
        SqlScalarType::RegType => named("regtype"),
        SqlScalarType::RegClass => named("regclass"),
        SqlScalarType::Int2Vector => named("int2vector"),
        SqlScalarType::MzTimestamp => named("mz_timestamp"),
        SqlScalarType::Range { element_type } => DataType::Named(format!(
            "range({})",
            sql_scalar_type_to_data_type(element_type)
        )),
        SqlScalarType::MzAclItem => named("mz_aclitem"),
        SqlScalarType::AclItem => named("aclitem"),
        SqlScalarType::Record { fields, .. } => DataType::Record(
            fields
                .iter()
                .map(|(name, col_type)| RecordField {
                    name: name.as_str().to_string(),
                    r#type: sql_scalar_type_to_data_type(&col_type.scalar_type),
                    nullable: col_type.nullable,
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::SqlColumnType;
    use mz_repr::adt::numeric::NumericMaxScale;

    fn record(fields: &[(&str, DataType, bool)]) -> DataType {
        DataType::Record(
            fields
                .iter()
                .map(|(name, r#type, nullable)| RecordField {
                    name: (*name).to_string(),
                    r#type: r#type.clone(),
                    nullable: *nullable,
                })
                .collect(),
        )
    }

    #[mz_ore::test]
    fn numeric_renders_scale_in_the_scale_position() {
        let render = |t| sql_scalar_type_to_data_type(&t).to_string();
        assert_eq!(
            render(SqlScalarType::Numeric { max_scale: None }),
            "numeric"
        );
        assert_eq!(
            render(SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO)
            }),
            "numeric(39,0)"
        );
        assert_eq!(
            render(SqlScalarType::Numeric {
                max_scale: Some(NumericMaxScale::try_from(2i64).unwrap())
            }),
            "numeric(39,2)"
        );
    }

    #[mz_ore::test]
    fn record_keeps_field_names_types_and_nullability() {
        let scalar = SqlScalarType::Record {
            fields: [
                (
                    "a".into(),
                    SqlColumnType {
                        scalar_type: SqlScalarType::Int32,
                        nullable: false,
                    },
                ),
                (
                    "n".into(),
                    SqlColumnType {
                        scalar_type: SqlScalarType::Record {
                            fields: [(
                                "x".into(),
                                SqlColumnType {
                                    scalar_type: SqlScalarType::String,
                                    nullable: true,
                                },
                            )]
                            .into(),
                            custom_id: None,
                        },
                        nullable: true,
                    },
                ),
            ]
            .into(),
            custom_id: None,
        };
        assert_eq!(
            sql_scalar_type_to_data_type(&scalar),
            record(&[
                ("a", DataType::named("int4"), false),
                ("n", record(&[("x", DataType::named("text"), true)]), true),
            ])
        );
    }

    #[mz_ore::test]
    fn anonymous_list_keeps_its_element_type() {
        let scalar = SqlScalarType::List {
            element_type: Box::new(SqlScalarType::Int64),
            custom_id: None,
        };
        assert_eq!(
            sql_scalar_type_to_data_type(&scalar).to_string(),
            "int8 list"
        );
    }

    #[mz_ore::test]
    fn stub_sql_preserves_column_order() {
        let mut columns = BTreeMap::new();
        // Deliberately non-alphabetical by position so the wrong (BTreeMap)
        // order would emit `apple` before `zebra`.
        columns.insert(
            "zebra".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        columns.insert(
            "apple".to_string(),
            ColumnType {
                r#type: DataType::named("text"),
                nullable: true,
                position: 1,
                comment: None,
            },
        );

        let id = ObjectId::new("db".to_string(), "public".to_string(), "dep".to_string());
        let statements = create_stub_statements(&id, &columns).expect("stub builds");
        let sql = statements.join("\n");
        assert_eq!(statements.len(), 1, "no record means a plain table: {sql}");
        let zebra = sql.find("zebra").expect("zebra column present");
        let apple = sql.find("apple").expect("apple column present");
        assert!(
            zebra < apple,
            "stub columns must follow schema position order; got: {sql}"
        );
    }

    #[mz_ore::test]
    fn record_column_becomes_a_view_over_helpers() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "payload".to_string(),
            ColumnType {
                r#type: record(&[
                    ("a", DataType::named("integer"), false),
                    ("b", DataType::named("text"), true),
                ]),
                nullable: true,
                position: 0,
                comment: None,
            },
        );

        let id = ObjectId::new("db".to_string(), "public".to_string(), "dep".to_string());
        let statements = create_stub_statements(&id, &columns).expect("stub builds");
        assert_eq!(
            statements.len(),
            2,
            "helper table then view: {statements:?}"
        );
        assert!(
            statements[0].starts_with(r#"CREATE TABLE "db"."public"."dep_mz_deploy_stub_h0""#),
            "helper table first: {}",
            statements[0]
        );
        assert!(
            statements[0].contains(r#""a" integer NOT NULL"#)
                && statements[0].contains(r#""b" text"#),
            "helper carries the record's field nullability: {}",
            statements[0]
        );
        assert!(
            statements[1].starts_with(r#"CREATE VIEW "db"."public"."dep" AS"#),
            "stub is the last statement: {}",
            statements[1]
        );
    }

    #[mz_ore::test]
    fn pseudo_token_is_reported_against_its_column() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "payload".to_string(),
            ColumnType {
                r#type: DataType::named("record"),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        let id = ObjectId::new("db".to_string(), "public".to_string(), "dep".to_string());
        let err = create_stub_statements(&id, &columns).expect_err("pseudo type is rejected");
        let message = err.to_string();
        assert!(
            message.contains("payload") && message.contains("mz-deploy lock"),
            "error should name the column and the fix: {message}"
        );
    }

    #[mz_ore::test]
    fn record_inside_a_container_is_rejected() {
        let mut columns = BTreeMap::new();
        columns.insert(
            "payloads".to_string(),
            ColumnType {
                r#type: DataType::List(Box::new(record(&[(
                    "a",
                    DataType::named("integer"),
                    false,
                )]))),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        let id = ObjectId::new("db".to_string(), "public".to_string(), "dep".to_string());
        let err = create_stub_statements(&id, &columns).expect_err("nested record is rejected");
        assert!(
            err.to_string().contains("not supported"),
            "unexpected error: {err}"
        );
    }
}
