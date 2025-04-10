// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_proto::RustType;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CreateSubsourceStatement,
    ExternalReferences, Ident, UnresolvedItemName, Value, WithOptionValue,
};
use mz_storage_types::sources::SourceExportStatementDetails;
use prost::Message;

use crate::names::{Aug, ResolvedItemName};
use crate::plan::{PlanError, StatementContext};
use crate::pure::{
    PurifiedExportDetails, PurifiedSourceExport, RetrievedSourceReferences, SourceReferencePolicy,
    SqlServerSourcePurificationError,
};

/// Purify the requested [`ExternalReferences`] from the provided
/// [`RetrievedSourceReferences`].
#[allow(clippy::unused_async)]
pub(super) async fn purify_source_exports(
    _client: &mut mz_sql_server_util::Client,
    retrieved_references: &RetrievedSourceReferences,
    requested_references: &Option<ExternalReferences>,
    _text_columns: &[UnresolvedItemName],
    _exclude_columns: &[UnresolvedItemName],
    unresolved_source_name: &UnresolvedItemName,
    reference_policy: &SourceReferencePolicy,
) -> Result<BTreeMap<UnresolvedItemName, PurifiedSourceExport>, PlanError> {
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

            // TODO(sql_server1): Check text columns and exclude columns here.
            // If source references are empty it doesn't make sense to have
            // those specified.

            return Ok(BTreeMap::default());
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

    // TODO(sql_server1): Handle text and exclude columns.
    // TODO(sql_server1): Validate permissions on upstream tables.

    let exports = requested_exports
        .into_iter()
        .map(|requested| {
            let table = requested
                .meta
                .sql_server_table()
                .expect("sql server source")
                .clone();
            let capture_instance = requested
                .meta
                .sql_server_capture_instance()
                .expect("sql server source");
            let export = PurifiedSourceExport {
                external_reference: requested.external_reference,
                details: PurifiedExportDetails::SqlServer {
                    table,
                    capture_instance: Arc::clone(capture_instance),
                },
            };

            (requested.name, export)
        })
        .collect();

    Ok(exports)
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
            details,
            external_reference,
        } = generate_source_export_statement_values(scx, purified_export)?;

        let with_options = vec![
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

        // Create the subsource statement
        let subsource = CreateSubsourceStatement {
            name: subsource_name,
            columns,
            of_source: Some(source_name.clone()),
            constraints: vec![],
            if_not_exists: false,
            with_options,
        };
        subsources.push(subsource);
    }

    Ok(subsources)
}

struct SqlServerExportStatementValues {
    pub columns: Vec<ColumnDef<Aug>>,
    pub details: SourceExportStatementDetails,
    pub external_reference: UnresolvedItemName,
}

fn generate_source_export_statement_values(
    scx: &StatementContext,
    purified_export: PurifiedSourceExport,
) -> Result<SqlServerExportStatementValues, PlanError> {
    let PurifiedExportDetails::SqlServer {
        table,
        capture_instance,
    } = purified_export.details
    else {
        unreachable!("purified export details must be SQL Server")
    };

    let mut columns = vec![];
    for c in table.columns.iter() {
        let name = Ident::new(c.name.as_ref())?;
        let ty = mz_pgrepr::Type::from(&c.column_type.scalar_type);
        let data_type = scx.resolve_type(ty)?;
        let mut col_options = vec![];

        if !c.column_type.nullable {
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

    let values = SqlServerExportStatementValues {
        columns,
        details: SourceExportStatementDetails::SqlServer {
            table,
            capture_instance,
        },
        external_reference: purified_export.external_reference,
    };
    Ok(values)
}
