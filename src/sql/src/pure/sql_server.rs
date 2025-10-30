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
use std::time::Duration;

use mz_proto::RustType;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    ExternalReferences, Ident, SqlServerConfigOptionName, TableConstraint, UnresolvedItemName,
    Value, WithOptionValue,
};
use mz_sql_server_util::desc::{
    SqlServerColumnDecodeType, SqlServerColumnDesc, SqlServerQualifiedTableName,
    SqlServerTableConstraintType, SqlServerTableDesc,
};
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
    client: &mut mz_sql_server_util::Client,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    text_columns: &[UnresolvedItemName],
    excl_columns: &[UnresolvedItemName],
    unresolved_source_name: &UnresolvedItemName,
    timeout: Duration,
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

    if requested_exports.is_empty() {
        sql_bail!(
            "SQL Server source must ingest at least one table, but {} matched none",
            requested_references
                .as_ref()
                .expect("checked above")
                .to_ast_string_simple()
        )
    }

    super::validate_source_export_names(&requested_exports)?;

    // TODO(sql_server2): Should we check if these have overlapping columns?
    // What do our other sources do?
    let text_cols_map = map_column_refs(text_columns, SqlServerConfigOptionName::TextColumns)?;
    let excl_cols_map = map_column_refs(excl_columns, SqlServerConfigOptionName::ExcludeColumns)?;

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

        let capture_instance = export
            .meta
            .sql_server_capture_instance()
            .expect("sql server capture instance");
        let mut cdc_columns =
            mz_sql_server_util::inspect::get_cdc_table_columns(client, capture_instance).await?;
        let mut mismatched_columns = vec![];

        for column in table.columns.iter() {
            let col_excluded = is_excluded(column.name.as_ref());
            if let SqlServerColumnDecodeType::Unsupported { context } = &column.decode_type
                && !col_excluded
            {
                Err(SqlServerSourcePurificationError::UnsupportedColumn {
                    schema_name: Arc::clone(&table.schema_name),
                    tbl_name: Arc::clone(&table.name),
                    col_name: Arc::clone(&column.name),
                    col_type: Arc::clone(&column.raw_type),
                    context: context.clone(),
                })?
            }

            // Columns in the upstream table must be in the CDC table, unless excluded.
            let cdc_column = cdc_columns.remove(&column.name);
            if col_excluded {
                continue;
            }
            let Some(cdc_column) = cdc_column else {
                mismatched_columns.push(Arc::clone(&column.name));
                continue;
            };

            let cdc_column = SqlServerColumnDesc::new(&cdc_column);
            match (cdc_column.column_type.as_ref(), column.column_type.as_ref()) {
                (None, None) => (),
                (Some(cdc_type), Some(tbl_type)) => {
                    // Nullability needs to be excluded in this check as the CDC table column
                    // must be nullable to support deleted columns from the upstream table.
                    if cdc_type.scalar_type != tbl_type.scalar_type {
                        mismatched_columns.push(Arc::clone(&column.name));
                    }
                }
                (Some(_), None) | (None, Some(_)) => {
                    mismatched_columns.push(Arc::clone(&column.name));
                }
            }
        }

        // We only need to error for columns that exist in the upstream table and not in the CDC
        // table.  SQL Server decodes based on column name, so any columns missing from the table
        // description are ignored when reading from CDC.
        if !mismatched_columns.is_empty() {
            Err(SqlServerSourcePurificationError::CdcMissingColumns {
                capture_instance: Arc::clone(capture_instance),
                col_names: mismatched_columns,
            })?
        }
    }

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

    mz_sql_server_util::inspect::validate_source_privileges(
        client,
        capture_instances.values().map(|instance| instance.as_ref()),
    )
    .await?;

    // If CDC is freshly enabled for a table, it has been observed that
    // the `start_lsn`` from `cdc.change_tables` can be ahead of the LSN
    // returned by `sys.fn_cdc_get_max_lsn`.  Eventually, the LSN returned
    // by `sys.fn_cdc_get_max_lsn` will surpass `start_lsn`. For this
    // reason, we choose the initial_lsn to be
    // `max(start_lsns, sys.fn_cdc_get_max_lsn())``.
    let mut initial_lsns = mz_sql_server_util::inspect::get_min_lsns(
        client,
        capture_instances.values().map(|instance| instance.as_ref()),
    )
    .await?;

    let max_lsn = mz_sql_server_util::inspect::get_max_lsn_retry(client, timeout).await?;
    tracing::debug!(?initial_lsns, %max_lsn, "retrieved start LSNs");
    for lsn in initial_lsns.values_mut() {
        *lsn = std::cmp::max(*lsn, max_lsn);
    }

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

            let initial_lsn = *initial_lsns.get(capture_instance).ok_or_else(|| {
                SqlServerSourcePurificationError::NoStartLsn(capture_instance.to_string())
            })?;

            let export = PurifiedSourceExport {
                external_reference: reference.external_reference,
                details: PurifiedExportDetails::SqlServer {
                    table: reference.meta,
                    text_columns,
                    excl_columns,
                    capture_instance: Arc::clone(capture_instance),
                    initial_lsn,
                },
            };

            Ok::<_, SqlServerSourcePurificationError>((reference.name, export))
        })
        .collect::<Result<_, _>>()?;

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
    let included_columns = table
        .columns
        .iter()
        .filter_map(|c| c.column_type.as_ref().map(|ct| (c.name.as_ref(), ct)));

    let mut column_defs = vec![];
    let mut constraints = vec![];

    // there are likely less excluded columns than included
    let excluded_columns: BTreeSet<_> = table
        .columns
        .iter()
        .filter_map(|c| match &c.column_type {
            None => Some(c.name.as_ref()),
            Some(_) => None,
        })
        .collect();
    for constraint in table.constraints.iter() {
        // if one or more columns of the constraint are excluded, do not record the constraint
        // in materialize
        if constraint
            .column_names
            .iter()
            .any(|col| excluded_columns.contains(col.as_str()))
        {
            tracing::debug!(
                "skipping constraint {name} due to excluded column",
                name = &constraint.constraint_name
            );
            continue;
        }

        constraints.push(mz_sql_parser::ast::TableConstraint::Unique {
            name: Some(Ident::new_lossy(constraint.constraint_name.clone())),
            columns: constraint
                .column_names
                .iter()
                .map(Ident::new)
                .collect::<Result<_, _>>()?,
            is_primary: matches!(
                constraint.constraint_type,
                SqlServerTableConstraintType::PrimaryKey
            ),
            nulls_not_distinct: false,
        });
    }

    tracing::debug!(
        "source export constraints for {schema_name}.{table_name}: {constraints:?}",
        schema_name = &table.schema_name,
        table_name = &table.name
    );

    for (col_name, col_type) in included_columns {
        let name = Ident::new(col_name)?;
        let ty = mz_pgrepr::Type::from(&col_type.scalar_type);
        let data_type = scx.resolve_type(ty)?;
        let mut col_options = vec![];

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
