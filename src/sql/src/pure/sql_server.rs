// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_ore::collections::CollectionExt;
use mz_proto::RustType;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    ExternalReferences, Ident, SqlServerConfigOptionName, TableConstraint, UnresolvedItemName,
    Value, WithOptionValue,
};
use mz_sql_server_util::desc::{SqlServerQualifiedTableName, SqlServerTableDesc};
use mz_storage_types::sources::{SourceExportStatementDetails, SourceReferenceResolver};
use prost::Message;

use crate::names::{Aug, ResolvedItemName};
use crate::plan::{PlanError, StatementContext};
use crate::pure::{
    PurifiedExportDetails, PurifiedSourceExport, RequestedSourceExport, RetrievedSourceReferences,
    SourceReferencePolicy, SqlServerSourcePurificationError,
};

pub(super) struct PurifiedSourceExports {
    /// Map of source export names to details of the export.
    pub(super) source_exports: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    /// Normalized list of columns that we'll decode as text.
    ///
    /// Note: Each source export (i.e. subsource) also tracks which of its
    /// columns we decode as text, but we also return this normalized list so
    /// we can roundtrip a `CREATE SOURCE` statement.
    pub(super) normalized_text_columns: Vec<WithOptionValue<Aug>>,
    /// Normalized list of columns that we'll exclude.
    ///
    /// Note: Each source export (i.e. subsource) also tracks which of its
    /// columns we should exlude, but we also return this normalized list so
    /// we can roundtrip a `CREATE SOURCE` statement.
    pub(super) normalized_excl_columns: Vec<WithOptionValue<Aug>>,
}

/// Purify the requested [`ExternalReferences`] from the provided
/// [`RetrievedSourceReferences`].
#[allow(clippy::unused_async)]
pub(super) async fn purify_source_exports(
    database: &str,
    _client: &mut mz_sql_server_util::Client,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    text_columns: &[UnresolvedItemName],
    excl_columns: &[UnresolvedItemName],
    unresolved_source_name: &UnresolvedItemName,
    initial_lsn: mz_sql_server_util::cdc::Lsn,
    reference_policy: &SourceReferencePolicy,
) -> Result<PurifiedSourceExports, PlanError> {
    let requested_exports = match requested_references.as_ref() {
        Some(requested) => {
            if *reference_policy == SourceReferencePolicy::NotAllowed {
                return Err(PlanError::UseTablesForSources(requested.to_string()));
            } else {
                let exports = retrieved_references
                    .requested_source_exports(Some(requested), unresolved_source_name)?;
                exports
            }
        }
        None => {
            if *reference_policy == SourceReferencePolicy::Required {
                return Err(SqlServerSourcePurificationError::RequiresExternalReferences.into());
            }

            // If no external reference is specified, it does not make sense to include
            // or text or exclude columns.
            if !text_columns.is_empty() {
                Err(
                    SqlServerSourcePurificationError::UnnecessaryOptionsWithoutReferences(
                        "TEXT COLUMNS".to_string(),
                    ),
                )?
            }
            if !excl_columns.is_empty() {
                Err(
                    SqlServerSourcePurificationError::UnnecessaryOptionsWithoutReferences(
                        "EXCLUDE COLUMNS".to_string(),
                    ),
                )?
            }

            return Ok(PurifiedSourceExports {
                source_exports: BTreeMap::default(),
                normalized_text_columns: Vec::default(),
                normalized_excl_columns: Vec::default(),
            });
        }
    };

    // TODO(sql_server2): Should we check if these have overlapping columns?
    // What do our other sources do?
    let text_cols_map = map_column_refs(text_columns, SqlServerConfigOptionName::TextColumns)?;
    let excl_cols_map = map_column_refs(excl_columns, SqlServerConfigOptionName::ExcludeColumns)?;

    if requested_exports.is_empty() {
        sql_bail!(
            "SQL Server source must ingest at least one table, but {} matched none",
            requested_references
                .as_ref()
                .expect("checked above")
                .to_ast_string_simple()
        )
    }

    // Ensure the columns we intend to include are supported.
    //
    // For example, we don't support `text` columns in SQL Server because they
    // do not include the "before" value when an `UPDATE` occurs. When parsing
    // the raw metadata for this type we mark the column as "unsupported" but
    // don't return an error, in case later (here) they intend to exclude that
    // column.
    for export in &requested_exports {
        let table = export.meta.sql_server_table().expect("sql server source");
        let maybe_excl_cols = excl_cols_map.get(&table.qualified_name());
        let is_excluded = |name| {
            maybe_excl_cols
                .as_ref()
                .map(|cols| cols.contains(name))
                .unwrap_or(false)
        };

        let maybe_bad_column = table
            .columns
            .iter()
            .find(|col| !col.is_supported() && !is_excluded(col.name.as_ref()));
        if let Some(bad_column) = maybe_bad_column {
            Err(SqlServerSourcePurificationError::UnsupportedColumn {
                schema_name: Arc::clone(&table.schema_name),
                tbl_name: Arc::clone(&table.name),
                col_name: Arc::clone(&bad_column.name),
                col_type: Arc::clone(&bad_column.raw_type),
            })?
        }
    }

    // TODO(sql_server2): Validate permissions on upstream tables.

    let capture_instances: BTreeMap<_, _> = requested_exports
        .iter()
        .map(|requested| {
            let table = requested
                .meta
                .sql_server_table()
                .expect("sql server source");
            let capture_instance = requested
                .meta
                .sql_server_capture_instance()
                .expect("sql server source");

            (table.qualified_name(), Arc::clone(capture_instance))
        })
        .collect();

    let mut tables = vec![];
    for requested in requested_exports {
        let mut table = requested
            .meta
            .sql_server_table()
            .expect("sql server source")
            .clone();

        let maybe_text_cols = text_cols_map.get(&table.qualified_name());
        let maybe_excl_cols = excl_cols_map.get(&table.qualified_name());

        if let Some(text_cols) = maybe_text_cols {
            table.apply_text_columns(text_cols);
        }

        if let Some(excl_cols) = maybe_excl_cols {
            table.apply_excl_columns(excl_cols);
        }

        if table.columns.iter().all(|c| c.is_excluded()) {
            Err(SqlServerSourcePurificationError::AllColumnsExcluded {
                tbl_name: Arc::clone(&table.name),
            })?
        }

        tables.push(requested.change_meta(table));
    }

    if tables.is_empty() {
        Err(SqlServerSourcePurificationError::NoTables)?;
    }

    let reference_resolver = SourceReferenceResolver::new(
        database,
        &tables.iter().map(|r| &r.meta).collect::<Vec<_>>(),
    )?;

    // Normalize column options and remove unused column references.
    let normalized_text_columns = normalize_column_refs(
        text_columns,
        &reference_resolver,
        &tables,
        SqlServerConfigOptionName::TextColumns,
    )?;
    let normalized_excl_columns = normalize_column_refs(
        excl_columns,
        &reference_resolver,
        &tables,
        SqlServerConfigOptionName::ExcludeColumns,
    )?;

    let exports = tables
        .into_iter()
        .map(|reference| {
            let table_reference = reference.meta.qualified_name();
            let text_columns = text_cols_map.get(&table_reference).map(|cols| {
                cols.iter()
                    .map(|c| Ident::new(*c).expect("validated above"))
                    .collect()
            });
            let excl_columns = excl_cols_map.get(&table_reference).map(|cols| {
                cols.iter()
                    .map(|c| Ident::new(*c).expect("validated above"))
                    .collect()
            });
            let capture_instance = capture_instances
                .get(&reference.meta.qualified_name())
                .expect("capture instance should exist");

            let export = PurifiedSourceExport {
                external_reference: reference.external_reference,
                details: PurifiedExportDetails::SqlServer {
                    table: reference.meta,
                    text_columns,
                    excl_columns,
                    capture_instance: Arc::clone(capture_instance),
                    initial_lsn: initial_lsn.clone(),
                },
            };

            (reference.name, export)
        })
        .collect();

    Ok(PurifiedSourceExports {
        source_exports: exports,
        normalized_text_columns,
        normalized_excl_columns,
    })
}

pub fn generate_create_subsource_statements(
    scx: &StatementContext,
    source_name: ResolvedItemName,
    requested_subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    let mut subsources = Vec::with_capacity(requested_subsources.len());

    for (subsource_name, purified_export) in requested_subsources {
        let SqlServerExportStatementValues {
            columns,
            constraints,
            text_columns,
            excl_columns,
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
        if let Some(excl_columns) = excl_columns {
            with_options.push(CreateSubsourceOption {
                name: CreateSubsourceOptionName::ExcludeColumns,
                value: Some(WithOptionValue::Sequence(excl_columns)),
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

pub(super) struct SqlServerExportStatementValues {
    pub(super) columns: Vec<ColumnDef<Aug>>,
    pub(super) constraints: Vec<TableConstraint<Aug>>,
    pub(super) text_columns: Option<Vec<WithOptionValue<Aug>>>,
    pub(super) excl_columns: Option<Vec<WithOptionValue<Aug>>>,
    pub(super) details: SourceExportStatementDetails,
    pub(super) external_reference: UnresolvedItemName,
}

pub(super) fn generate_source_export_statement_values(
    scx: &StatementContext,
    purified_export: PurifiedSourceExport,
) -> Result<SqlServerExportStatementValues, PlanError> {
    let PurifiedExportDetails::SqlServer {
        table,
        text_columns,
        excl_columns,
        capture_instance,
        initial_lsn,
    } = purified_export.details
    else {
        unreachable!("purified export details must be SQL Server")
    };

    // Filter out columns that the user wanted to exclude.
    let included_columns = table.columns.iter().filter_map(|c| {
        c.column_type
            .as_ref()
            .map(|ct| (c.name.as_ref(), ct, c.primary_key_constraint.clone()))
    });

    let mut primary_keys: BTreeMap<Arc<str>, Vec<Ident>> = BTreeMap::new();
    let mut column_defs = vec![];
    let mut constraints = vec![];

    for (col_name, col_type, col_primary_key_constraint) in included_columns {
        let name = Ident::new(col_name)?;
        let ty = mz_pgrepr::Type::from(&col_type.scalar_type);
        let data_type = scx.resolve_type(ty)?;
        let mut col_options = vec![];

        if let Some(constraint) = col_primary_key_constraint {
            let columns = primary_keys.entry(constraint).or_default();
            columns.push(name.clone());
        }
        if !col_type.nullable {
            col_options.push(mz_sql_parser::ast::ColumnOptionDef {
                name: None,
                option: mz_sql_parser::ast::ColumnOption::NotNull,
            });
        }

        column_defs.push(ColumnDef {
            name,
            data_type,
            collation: None,
            options: col_options,
        });
    }

    match primary_keys.len() {
        // No primary key.
        0 => (),
        1 => {
            let (constraint_name, columns) = primary_keys.into_element();
            constraints.push(mz_sql_parser::ast::TableConstraint::Unique {
                name: Some(Ident::new_lossy(&*constraint_name)),
                columns,
                is_primary: true,
                nulls_not_distinct: false,
            });
        }
        // Multiple primary keys..?
        2.. => {
            let constraint_names = primary_keys.into_keys().collect();
            return Err(PlanError::SqlServerSourcePurificationError(
                SqlServerSourcePurificationError::MultiplePrimaryKeys { constraint_names },
            ));
        }
    }

    let details = SourceExportStatementDetails::SqlServer {
        table,
        capture_instance,
        initial_lsn,
    };
    let text_columns = text_columns.map(|mut columns| {
        columns.sort();
        columns
            .into_iter()
            .map(WithOptionValue::Ident::<Aug>)
            .collect()
    });
    let excl_columns = excl_columns.map(|mut columns| {
        columns.sort();
        columns
            .into_iter()
            .map(WithOptionValue::Ident::<Aug>)
            .collect()
    });

    let values = SqlServerExportStatementValues {
        columns: column_defs,
        constraints,
        text_columns,
        excl_columns,
        details,
        external_reference: purified_export.external_reference,
    };
    Ok(values)
}

/// Create a [`BTreeMap`] of [`SqlServerQualifiedTableName`] to a [`BTreeSet`] of column names.
fn map_column_refs<'a>(
    cols: &'a [UnresolvedItemName],
    option_type: SqlServerConfigOptionName,
) -> Result<BTreeMap<SqlServerQualifiedTableName, BTreeSet<&'a str>>, PlanError> {
    let mut table_to_cols: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for name in cols.iter() {
        // We only support fully qualified references for now (e.g. `schema_name.table_name.column_name`)
        if name.0.len() == 3 {
            let key = mz_sql_server_util::desc::SqlServerQualifiedTableName {
                schema_name: name.0[0].as_str().into(),
                table_name: name.0[1].as_str().into(),
            };
            let new = table_to_cols
                .entry(key)
                .or_default()
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

/// Normalize the provided `cols` references to a sorted and deduplicated list of
/// [`WithOptionValue`]s.
///
/// Returns an error if any column reference in `cols` is not part of a table in the
/// provided `tables`.
fn normalize_column_refs(
    cols: &[UnresolvedItemName],
    reference_resolver: &SourceReferenceResolver,
    tables: &[RequestedSourceExport<SqlServerTableDesc>],
    option_name: SqlServerConfigOptionName,
) -> Result<Vec<WithOptionValue<Aug>>, SqlServerSourcePurificationError> {
    let (seq, unknown): (Vec<_>, Vec<_>) = cols.into_iter().partition(|name| {
        let (column_name, qual) = name.0.split_last().expect("non-empty");
        match reference_resolver.resolve_idx(qual) {
            // TODO(sql_server3): This needs to also introduce the maximum qualification
            // on  the columns, i.e. ensure they have the schema name.
            Ok(idx) => tables[idx]
                .meta
                .columns
                .iter()
                .any(|n| &*n.name == column_name.as_str()),
            Err(_) => false,
        }
    });

    if !unknown.is_empty() {
        return Err(SqlServerSourcePurificationError::DanglingColumns {
            option_name: option_name.to_string(),
            items: unknown.into_iter().cloned().collect(),
        });
    }

    let mut seq: Vec<_> = seq
        .into_iter()
        .cloned()
        .map(WithOptionValue::UnresolvedItemName)
        .collect();

    seq.sort();
    seq.dedup();
    Ok(seq)
}
