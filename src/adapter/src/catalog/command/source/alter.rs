// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_catalog::memory::objects::{DataSourceDesc, Source};
use mz_ore::collections::CollectionExt;
use mz_repr::CatalogItemId;
use mz_sql::catalog::CatalogItem;
use mz_sql::names::Aug;
use mz_sql::plan::{Params, Plan};
use mz_sql_parser::ast::{
    AlterSourceAddSubsourceOption, CreateSourceConnection, MySqlConfigOption,
    MySqlConfigOptionName, PgConfigOption, PgConfigOptionName, SqlServerConfigOption,
    SqlServerConfigOptionName, Statement, UnresolvedItemName, WithOptionValue,
};
use mz_storage_types::AlterCompatible;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::controller::StorageError;

use crate::AdapterError;
use crate::catalog::ConnCatalog;

/// Rebuilds the current source definition with the supplied export options.
pub(crate) fn rebuild_source(
    catalog: &ConnCatalog<'_>,
    item_id: CatalogItemId,
    options: Vec<AlterSourceAddSubsourceOption<Aug>>,
) -> Result<Source, AdapterError> {
    let state = catalog.state();
    let cur_entry = state.get_entry(&item_id);
    let cur_source = cur_entry.source().expect("known to be source");
    const ALTER_SOURCE: &str = "ALTER SOURCE...ADD SUBSOURCES";

    let mz_sql::plan::AlterSourceAddSubsourceOptionExtracted {
        text_columns: mut new_text_columns,
        exclude_columns: mut new_exclude_columns,
        ..
    } = options.try_into()?;

    let create_source_stmt = match mz_sql::parse::parse(cur_entry.create_sql())
        .expect("invalid create sql persisted to catalog")
        .into_element()
        .ast
    {
        Statement::CreateSource(stmt) => stmt,
        _ => unreachable!("proved type is source"),
    };
    let (mut create_source_stmt, resolved_ids) =
        mz_sql::names::resolve(&state.for_system_session(), create_source_stmt)
            .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?;

    let curr_references: BTreeSet<_> = state
        .get_entry(&item_id)
        .used_by()
        .into_iter()
        .filter_map(|subsource| {
            state
                .get_entry(subsource)
                .subsource_details()
                .map(|(_id, reference, _details)| reference)
        })
        .collect();

    let purification_err =
        || AdapterError::internal(ALTER_SOURCE, "error in subsource purification");

    match &mut create_source_stmt.connection {
        CreateSourceConnection::Postgres {
            options: curr_options,
            ..
        } => {
            let mz_sql::plan::PgConfigOptionExtracted {
                mut text_columns, ..
            } = curr_options.clone().try_into()?;

            curr_options.retain(|o| !matches!(o.name, PgConfigOptionName::TextColumns));

            text_columns.retain(|column_qualified_reference| {
                mz_ore::soft_assert_eq_or_log!(
                    column_qualified_reference.0.len(),
                    4,
                    "all TEXT COLUMNS values must be column-qualified references"
                );
                let mut table = column_qualified_reference.clone();
                table.0.truncate(3);
                curr_references.contains(&table)
            });

            new_text_columns.extend(text_columns);

            if !new_text_columns.is_empty() {
                new_text_columns.sort();
                let new_text_columns = new_text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                curr_options.push(PgConfigOption {
                    name: PgConfigOptionName::TextColumns,
                    value: Some(WithOptionValue::Sequence(new_text_columns)),
                });
            }
        }
        CreateSourceConnection::MySql {
            options: curr_options,
            ..
        } => {
            let mz_sql::plan::MySqlConfigOptionExtracted {
                mut text_columns,
                mut exclude_columns,
                ..
            } = curr_options.clone().try_into()?;

            curr_options.retain(|o| {
                !matches!(
                    o.name,
                    MySqlConfigOptionName::TextColumns | MySqlConfigOptionName::ExcludeColumns
                )
            });

            let column_referenced = |column_qualified_reference: &UnresolvedItemName| {
                mz_ore::soft_assert_eq_or_log!(
                    column_qualified_reference.0.len(),
                    3,
                    "all TEXT COLUMNS & EXCLUDE COLUMNS values must be column-qualified references"
                );
                let mut table = column_qualified_reference.clone();
                table.0.truncate(2);
                curr_references.contains(&table)
            };
            text_columns.retain(column_referenced);
            exclude_columns.retain(column_referenced);

            new_text_columns.extend(text_columns);
            new_exclude_columns.extend(exclude_columns);

            if !new_text_columns.is_empty() {
                new_text_columns.sort();
                let new_text_columns = new_text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                curr_options.push(MySqlConfigOption {
                    name: MySqlConfigOptionName::TextColumns,
                    value: Some(WithOptionValue::Sequence(new_text_columns)),
                });
            }
            if !new_exclude_columns.is_empty() {
                new_exclude_columns.sort();
                let new_exclude_columns = new_exclude_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                curr_options.push(MySqlConfigOption {
                    name: MySqlConfigOptionName::ExcludeColumns,
                    value: Some(WithOptionValue::Sequence(new_exclude_columns)),
                });
            }
        }
        CreateSourceConnection::SqlServer {
            options: curr_options,
            ..
        } => {
            let mz_sql::plan::SqlServerConfigOptionExtracted {
                mut text_columns,
                mut exclude_columns,
                ..
            } = curr_options.clone().try_into()?;

            curr_options.retain(|o| {
                !matches!(
                    o.name,
                    SqlServerConfigOptionName::TextColumns
                        | SqlServerConfigOptionName::ExcludeColumns
                )
            });

            // SQL Server text/exclude column refs are 3-part (schema.table.col),
            // which truncate to 2-part (schema.table). But external references
            // are 3-part (database.schema.table). Use suffix matching since
            // a SQL Server source connects to a single database.
            let column_referenced = |column_qualified_reference: &UnresolvedItemName| {
                mz_ore::soft_assert_eq_or_log!(
                    column_qualified_reference.0.len(),
                    3,
                    "all TEXT COLUMNS & EXCLUDE COLUMNS values must be column-qualified references"
                );
                let mut table = column_qualified_reference.clone();
                table.0.truncate(2);
                curr_references.iter().any(|r| r.0.ends_with(&table.0))
            };
            text_columns.retain(column_referenced);
            exclude_columns.retain(column_referenced);

            new_text_columns.extend(text_columns);
            new_exclude_columns.extend(exclude_columns);

            if !new_text_columns.is_empty() {
                new_text_columns.sort();
                let new_text_columns = new_text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                curr_options.push(SqlServerConfigOption {
                    name: SqlServerConfigOptionName::TextColumns,
                    value: Some(WithOptionValue::Sequence(new_text_columns)),
                });
            }
            if !new_exclude_columns.is_empty() {
                new_exclude_columns.sort();
                let new_exclude_columns = new_exclude_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                curr_options.push(SqlServerConfigOption {
                    name: SqlServerConfigOptionName::ExcludeColumns,
                    value: Some(WithOptionValue::Sequence(new_exclude_columns)),
                });
            }
        }
        _ => return Err(purification_err()),
    };

    let mut catalog = state.for_system_session();
    catalog.mark_id_unresolvable_for_replanning(cur_entry.id());

    let planned = mz_sql::plan::plan(
        None,
        &catalog,
        Statement::CreateSource(create_source_stmt),
        &Params::empty(),
        &resolved_ids,
    )
    .map_err(|e| AdapterError::internal(ALTER_SOURCE, e))?;
    let plan = match planned {
        (Plan::CreateSource(plan), _sql_impl_ids) => plan,
        (p, _) => {
            unreachable!("create source plan is only valid response, got {:?}", p)
        }
    };

    let source = Source::new(
        plan,
        cur_source.global_id,
        resolved_ids,
        cur_source.custom_logical_compaction_window,
        cur_source.is_retained_metrics_object,
    );

    let old_desc = cur_entry
        .source_desc()
        .map_err(|error| AdapterError::internal(ALTER_SOURCE, error))?
        .expect("ingestion source")
        .clone()
        .into_inline_connection(state);
    let new_desc = match &source.data_source {
        DataSourceDesc::Ingestion { desc, .. }
        | DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
            desc.clone().into_inline_connection(state)
        }
        _ => unreachable!("replanned ingestion source"),
    };
    old_desc
        .alter_compatible(cur_source.global_id(), &new_desc)
        .map_err(StorageError::from)
        .map_err(|error| AdapterError::internal(ALTER_SOURCE, error))?;
    Ok(source)
}
