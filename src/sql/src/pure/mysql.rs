// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! MySQL utilities for SQL purification.

use std::collections::{BTreeMap, BTreeSet};

use mz_mysql_util::{
    MySqlError, MySqlTableDesc, QualifiedTableRef, SYSTEM_SCHEMAS, validate_source_privileges,
};
use mz_proto::RustType;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    ExternalReferences, Ident, MySqlConfigOptionName, TableConstraint, WithOptionValue,
};
use mz_sql_parser::ast::{UnresolvedItemName, Value};
use mz_storage_types::sources::{SourceExportStatementDetails, SourceReferenceResolver};
use prost::Message;

use crate::names::Aug;
use crate::plan::{PlanError, StatementContext};
use crate::pure::{MySqlSourcePurificationError, ResolvedItemName};

use super::references::RetrievedSourceReferences;
use super::{
    PurifiedExportDetails, PurifiedSourceExport, RequestedSourceExport, SourceReferencePolicy,
};

/// The name of the fake database that we use for MySQL sources
/// to fit our model of a 3-layer catalog. MySQL doesn't have a concept
/// of databases AND schemas, it treats both as the same thing.
pub(crate) static MYSQL_DATABASE_FAKE_NAME: &str = "mysql";

/// Convert an unresolved item name to a qualified table reference.
pub(super) fn external_reference_to_table(
    name: &UnresolvedItemName,
) -> Result<QualifiedTableRef<'_>, MySqlSourcePurificationError> {
    if name.0.len() != 2 {
        Err(MySqlSourcePurificationError::InvalidTableReference(
            name.to_string(),
        ))?
    }
    Ok(QualifiedTableRef {
        schema_name: name.0[0].as_str(),
        table_name: name.0[1].as_str(),
    })
}

pub fn generate_create_subsource_statements(
    scx: &StatementContext,
    source_name: ResolvedItemName,
    requested_subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    let mut subsources = Vec::with_capacity(requested_subsources.len());

    for (subsource_name, purified_export) in requested_subsources {
        let MySqlExportStatementValues {
            columns,
            constraints,
            text_columns,
            exclude_columns,
            details,
            external_reference,
        } = generate_source_export_statement_values(scx, purified_export)?;

        let mut with_options = vec![
            CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExternalReference,
                value: Some(WithOptionValue::UnresolvedItemName(external_reference)),
            },
            CreateSubsourceOption {
                name: CreateSubsourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            },
        ];

        if let Some(text_columns) = text_columns {
            with_options.push(CreateSubsourceOption {
                name: CreateSubsourceOptionName::TextColumns,
                value: Some(WithOptionValue::Sequence(text_columns)),
            });
        }

        if let Some(exclude_columns) = exclude_columns {
            with_options.push(CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExcludeColumns,
                value: Some(WithOptionValue::Sequence(exclude_columns)),
            });
        }

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            of_source: Some(source_name.clone()),
            constraints,
            if_not_exists: false,
            with_options,
        };
        subsources.push(subsource);
    }

    Ok(subsources)
}

pub(super) struct MySqlExportStatementValues {
    pub(super) columns: Vec<ColumnDef<Aug>>,
    pub(super) constraints: Vec<TableConstraint<Aug>>,
    pub(super) text_columns: Option<Vec<WithOptionValue<Aug>>>,
    pub(super) exclude_columns: Option<Vec<WithOptionValue<Aug>>>,
    pub(super) details: SourceExportStatementDetails,
    pub(super) external_reference: UnresolvedItemName,
}

pub(super) fn generate_source_export_statement_values(
    scx: &StatementContext,
    purified_export: PurifiedSourceExport,
) -> Result<MySqlExportStatementValues, PlanError> {
    let PurifiedExportDetails::MySql {
        table,
        text_columns,
        exclude_columns,
        initial_gtid_set,
    } = purified_export.details
    else {
        unreachable!("purified export details must be mysql")
    };

    // Figure out the schema of the subsource
    let mut columns = vec![];
    for c in table.columns.iter() {
        match c.column_type {
            // This column is intentionally ignored, so we don't generate a column for it in
            // the subsource.
            None => {}
            Some(ref column_type) => {
                let name = Ident::new(&c.name)?;

                let ty = mz_pgrepr::Type::from(&column_type.scalar_type);
                let data_type = scx.resolve_type(ty)?;
                let mut col_options = vec![];

                if !column_type.nullable {
                    col_options.push(mz_sql_parser::ast::ColumnOptionDef {
                        name: None,
                        option: mz_sql_parser::ast::ColumnOption::NotNull,
                    });
                }
                columns.push(ColumnDef {
                    name,
                    data_type,
                    collation: None,
                    options: col_options,
                });
            }
        }
    }

    let mut constraints = vec![];
    for key in table.keys.iter() {
        let columns: Result<Vec<Ident>, _> = key.columns.iter().map(Ident::new).collect();

        let constraint = mz_sql_parser::ast::TableConstraint::Unique {
            name: Some(Ident::new(&key.name)?),
            columns: columns?,
            is_primary: key.is_primary,
            // MySQL always permits multiple nulls values in unique indexes.
            nulls_not_distinct: false,
        };

        // We take the first constraint available to be the primary key.
        if key.is_primary {
            constraints.insert(0, constraint);
        } else {
            constraints.push(constraint);
        }
    }

    let details = SourceExportStatementDetails::MySql {
        table,
        initial_gtid_set,
    };

    let text_columns = text_columns.map(|mut columns| {
        columns.sort();
        columns
            .into_iter()
            .map(WithOptionValue::Ident::<Aug>)
            .collect()
    });

    let exclude_columns = exclude_columns.map(|mut columns| {
        columns.sort();
        columns
            .into_iter()
            .map(WithOptionValue::Ident::<Aug>)
            .collect()
    });
    Ok(MySqlExportStatementValues {
        columns,
        constraints,
        text_columns,
        exclude_columns,
        details,
        external_reference: purified_export.external_reference,
    })
}

/// Map a list of column references to a map of table references to column names.
pub(super) fn map_column_refs<'a>(
    cols: &'a [UnresolvedItemName],
    option_type: MySqlConfigOptionName,
) -> Result<BTreeMap<QualifiedTableRef<'a>, BTreeSet<&'a str>>, PlanError> {
    let mut table_to_cols = BTreeMap::new();
    for name in cols.iter() {
        // We only support fully qualified references for now (e.g. `schema_name.table_name.column_name`)
        if name.0.len() == 3 {
            let key = mz_mysql_util::QualifiedTableRef {
                schema_name: name.0[0].as_str(),
                table_name: name.0[1].as_str(),
            };
            let new = table_to_cols
                .entry(key)
                .or_insert_with(BTreeSet::new)
                .insert(name.0[2].as_str());
            if !new {
                return Err(PlanError::InvalidOptionValue {
                    option_name: option_type.to_ast_string_simple(),
                    err: Box::new(PlanError::UnexpectedDuplicateReference { name: name.clone() }),
                });
            }
        } else {
            return Err(PlanError::InvalidOptionValue {
                option_name: option_type.to_ast_string_simple(),
                err: Box::new(PlanError::UnderqualifiedColumnName(name.to_string())),
            });
        }
    }
    Ok(table_to_cols)
}

/// Normalize column references to a sorted, deduplicated options list of column names.
pub(super) fn normalize_column_refs(
    cols: Vec<UnresolvedItemName>,
    reference_resolver: &SourceReferenceResolver,
    tables: &[RequestedSourceExport<MySqlTableDesc>],
    option_name: &str,
) -> Result<Vec<WithOptionValue<Aug>>, MySqlSourcePurificationError> {
    let (seq, unknown): (Vec<_>, Vec<_>) = cols.into_iter().partition(|name| {
        let (column_name, qual) = name.0.split_last().expect("non-empty");
        match reference_resolver.resolve_idx(qual) {
            // TODO: this needs to also introduce the maximum qualification on
            // the columns, i.e. ensure they have the schema name.
            Ok(idx) => tables[idx]
                .meta
                .columns
                .iter()
                .any(|n| &n.name == column_name.as_str()),
            Err(_) => false,
        }
    });

    if !unknown.is_empty() {
        return Err(MySqlSourcePurificationError::DanglingColumns {
            option_name: option_name.to_string(),
            items: unknown,
        });
    }

    let mut seq: Vec<_> = seq
        .into_iter()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();
    seq.sort();
    seq.dedup();
    Ok(seq)
}

pub(super) async fn validate_requested_references_privileges(
    requested_external_references: impl Iterator<Item = &UnresolvedItemName>,
    conn: &mut mz_mysql_util::MySqlConn,
) -> Result<(), PlanError> {
    // Ensure that we have correct privileges on all tables; we have to do this before we
    // start snapshotting because if we discover we cannot `SELECT` from a table while
    // snapshotting, we break the entire source.
    let tables_to_check_permissions = requested_external_references
        .map(external_reference_to_table)
        .collect::<Result<Vec<_>, _>>()?;

    validate_source_privileges(&mut *conn, &tables_to_check_permissions)
        .await
        .map_err(|err| match err {
            MySqlError::MissingPrivileges(missing_privileges) => {
                MySqlSourcePurificationError::UserLacksPrivileges(
                    missing_privileges
                        .into_iter()
                        .map(|mp| (mp.privilege, mp.qualified_table_name))
                        .collect(),
                )
                .into()
            }
            _ => PlanError::MySqlConnectionErr { cause: err.into() },
        })?;

    Ok(())
}

pub(super) struct PurifiedSourceExports {
    /// map of source export names to the details of the export
    pub(super) source_exports: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    // NOTE(roshan): The text columns and exclude columns are already part of their
    // appropriate `source_exports` above, but these are returned to allow
    // round-tripping a `CREATE SOURCE` statement while we still allow creating
    // implicit subsources from `CREATE SOURCE`. Remove once
    // fully deprecating that feature and forcing users to use explicit
    // `CREATE TABLE .. FROM SOURCE` statements.
    pub(super) normalized_text_columns: Vec<WithOptionValue<Aug>>,
    pub(super) normalized_exclude_columns: Vec<WithOptionValue<Aug>>,
}

// Purify the requested external references, returning a set of purified
// source exports corresponding to external tables, and and additional
// fields necessary to generate relevant statements and update statement options
pub(super) async fn purify_source_exports(
    conn: &mut mz_mysql_util::MySqlConn,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    text_columns: Vec<UnresolvedItemName>,
    exclude_columns: Vec<UnresolvedItemName>,
    unresolved_source_name: &UnresolvedItemName,
    initial_gtid_set: String,
    reference_policy: &SourceReferencePolicy,
) -> Result<PurifiedSourceExports, PlanError> {
    let requested_exports = match requested_references.as_ref() {
        Some(requested) if matches!(reference_policy, SourceReferencePolicy::NotAllowed) => {
            Err(PlanError::UseTablesForSources(requested.to_string()))?
        }
        Some(requested) => retrieved_references
            .requested_source_exports(Some(requested), unresolved_source_name)?,
        None => {
            if matches!(reference_policy, SourceReferencePolicy::Required) {
                Err(MySqlSourcePurificationError::RequiresExternalReferences)?
            }

            // If no external reference is specified, it does not make sense to include
            // or text or exclude columns.
            if !text_columns.is_empty() {
                Err(
                    MySqlSourcePurificationError::UnnecessaryOptionsWithoutReferences(
                        "TEXT COLUMNS".to_string(),
                    ),
                )?
            }
            if !exclude_columns.is_empty() {
                Err(
                    MySqlSourcePurificationError::UnnecessaryOptionsWithoutReferences(
                        "EXCLUDE COLUMNS".to_string(),
                    ),
                )?
            }

            return Ok(PurifiedSourceExports {
                source_exports: BTreeMap::new(),
                normalized_text_columns: vec![],
                normalized_exclude_columns: vec![],
            });
        }
    };

    if requested_exports.is_empty() {
        sql_bail!(
            "MySQL source must ingest at least one table, but {} matched none",
            requested_references
                .as_ref()
                .unwrap()
                .to_ast_string_simple()
        );
    }

    super::validate_source_export_names(&requested_exports)?;

    let text_cols_map = map_column_refs(&text_columns, MySqlConfigOptionName::TextColumns)?;
    let exclude_columns_map =
        map_column_refs(&exclude_columns, MySqlConfigOptionName::ExcludeColumns)?;

    // Convert the raw tables into a format that we can use to generate source exports
    // using any applicable text_columns and exclude_columns.
    let tables = requested_exports
        .into_iter()
        .map(|requested_export| {
            let table = requested_export.meta.mysql_table().expect("is mysql");
            let table_ref = table.table_ref();
            // we are cloning the BTreeSet<&str> so we can avoid a borrow on `table` here
            let text_cols = text_cols_map.get(&table_ref).map(|s| s.clone());
            let exclude_columns = exclude_columns_map.get(&table_ref).map(|s| s.clone());
            let parsed_table = table
                .clone()
                .to_desc(text_cols.as_ref(), exclude_columns.as_ref())
                .map_err(|err| match err {
                    mz_mysql_util::MySqlError::UnsupportedDataTypes { columns } => {
                        PlanError::from(MySqlSourcePurificationError::UnrecognizedTypes {
                            cols: columns
                                .into_iter()
                                .map(|c| (c.qualified_table_name, c.column_name, c.column_type))
                                .collect(),
                        })
                    }
                    mz_mysql_util::MySqlError::DuplicatedColumnNames {
                        qualified_table_name,
                        columns,
                    } => PlanError::from(MySqlSourcePurificationError::DuplicatedColumnNames(
                        qualified_table_name,
                        columns,
                    )),
                    _ => err.into(),
                })?;
            Ok(requested_export.change_meta(parsed_table))
        })
        .collect::<Result<Vec<_>, PlanError>>()?;

    if tables.is_empty() {
        Err(MySqlSourcePurificationError::EmptyDatabase)?;
    }

    validate_requested_references_privileges(tables.iter().map(|t| &t.external_reference), conn)
        .await?;

    let reference_resolver = SourceReferenceResolver::new(
        MYSQL_DATABASE_FAKE_NAME,
        &tables.iter().map(|r| &r.meta).collect::<Vec<_>>(),
    )?;

    // Normalize column options and remove unused column references.
    let normalized_text_columns = normalize_column_refs(
        text_columns.clone(),
        &reference_resolver,
        &tables,
        "TEXT COLUMNS",
    )?;
    let normalized_exclude_columns = normalize_column_refs(
        exclude_columns.clone(),
        &reference_resolver,
        &tables,
        "EXCLUDE COLUMNS",
    )?;

    let source_exports = tables
        .into_iter()
        .map(|r| {
            let table_ref = mz_mysql_util::QualifiedTableRef {
                schema_name: r.meta.schema_name.as_str(),
                table_name: r.meta.name.as_str(),
            };
            (
                r.name,
                PurifiedSourceExport {
                    external_reference: r.external_reference,
                    details: PurifiedExportDetails::MySql {
                        table: r.meta.clone(),
                        text_columns: text_cols_map.get(&table_ref).map(|cols| {
                            cols.iter()
                                .map(|c| Ident::new(*c).expect("validated above"))
                                .collect()
                        }),
                        exclude_columns: exclude_columns_map.get(&table_ref).map(|cols| {
                            cols.iter()
                                .map(|c| Ident::new(*c).expect("validated above"))
                                .collect()
                        }),
                        initial_gtid_set: initial_gtid_set.clone(),
                    },
                },
            )
        })
        .collect();

    Ok(PurifiedSourceExports {
        source_exports,
        normalized_text_columns,
        normalized_exclude_columns,
    })
}

/// Checks if the requested external references contain any explicit references
/// to system schemas, since these are otherwise default excluded when retrieving
/// tables from MySQL.
pub(super) fn references_system_schemas(requested_references: &Option<ExternalReferences>) -> bool {
    match requested_references {
        Some(requested) => match requested {
            ExternalReferences::All => false,
            ExternalReferences::SubsetSchemas(schemas) => schemas
                .iter()
                .any(|schema| SYSTEM_SCHEMAS.contains(&schema.as_str())),
            ExternalReferences::SubsetTables(tables) => tables.iter().any(|table| {
                SYSTEM_SCHEMAS.contains(
                    &external_reference_to_table(&table.reference)
                        .map(|t| t.schema_name)
                        .unwrap_or_default(),
                )
            }),
        },
        None => false,
    }
}
