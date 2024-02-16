// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mysql_common::binlog::events::QueryEvent;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::CapabilitySet;
use tracing::trace;

use mz_mysql_util::{Config, MySqlTableDesc};
use mz_repr::Row;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::UnresolvedItemName;
use mz_storage_types::sources::mysql::GtidPartition;
use mz_timely_util::builder_async::AsyncOutputHandle;

use crate::source::RawSourceCreationConfig;

use super::super::schemas::verify_schemas;
use super::super::{table_name, DefiniteError, TransientError};

/// Returns the UnresolvedItemName for the given table name referenced in a
/// SQL statement, using the current schema if the table name is unqualified.
fn table_ident(name: &str, current_schema: &str) -> Result<UnresolvedItemName, TransientError> {
    let stripped = name.replace('`', "");
    let mut name_iter = stripped.split('.');
    match (name_iter.next(), name_iter.next()) {
        (Some(t_name), None) => table_name(current_schema, t_name),
        (Some(schema_name), Some(t_name)) => table_name(schema_name, t_name),
        _ => Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent: {}",
            name
        ))),
    }
}

/// Handles QueryEvents from the MySQL replication stream. Since we only use
/// row-based replication, we only expect to see QueryEvents for DDL changes.
///
/// From the MySQL docs: 'A Query_event is created for each query that modifies
/// the database, unless the query is logged row-based.' This means that we can
/// expect any DDL changes to be represented as QueryEvents, which we must parse
/// to figure out if any of the tables we care about have been affected.
pub(super) async fn handle_query_event(
    event: QueryEvent<'_>,
    config: &RawSourceCreationConfig,
    connection_config: &Config,
    table_info: &BTreeMap<UnresolvedItemName, (usize, MySqlTableDesc)>,
    next_gtid: &Option<GtidPartition>,
    errored_tables: &mut BTreeSet<UnresolvedItemName>,
    data_output: &mut AsyncOutputHandle<
        GtidPartition,
        Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>,
        TeeCore<GtidPartition, Vec<((usize, Result<Row, DefiniteError>), GtidPartition, i64)>>,
    >,
    data_cap_set: &CapabilitySet<GtidPartition>,
) -> Result<(), TransientError> {
    let (id, worker_id) = (config.id, config.worker_id);

    let query = event.query();
    let current_schema = event.schema();

    let new_gtid = next_gtid.as_ref().expect("gtid cap should be set");

    // MySQL does not permit transactional DDL, so luckily we don't need to
    // worry about tracking BEGIN/COMMIT query events. We only need to look
    // for DDL changes that affect the schema of the tables we care about.
    let mut query_iter = query.split_ascii_whitespace();
    let first = query_iter.next();
    let second = query_iter.next();
    if let (Some(first), Some(second)) = (first, second) {
        // Detect `ALTER TABLE <tbl>`, `RENAME TABLE <tbl>` statements
        if (first.eq_ignore_ascii_case("alter") | first.eq_ignore_ascii_case("rename"))
            && second.eq_ignore_ascii_case("table")
        {
            let table = table_ident(
                query_iter.next().ok_or_else(|| {
                    TransientError::Generic(anyhow::anyhow!("Invalid DDL query: {}", query))
                })?,
                &current_schema,
            )?;
            if table_info.contains_key(&table) {
                trace!(%id, "timely-{worker_id} DDL change detected \
                                                 for {table:?}");
                let (output_index, table_desc) = &table_info[&table];
                let mut conn = connection_config
                    .connect(
                        &format!("timely-{worker_id} MySQL "),
                        &config.config.connection_context.ssh_tunnel_manager,
                    )
                    .await?;
                if let Some((err_table, err)) = verify_schemas(&mut *conn, &[(&table, table_desc)])
                    .await?
                    .drain(..)
                    .next()
                {
                    assert_eq!(err_table, &table, "Unexpected table verification error");
                    trace!(%id, "timely-{worker_id} DDL change \
                           verification error for {table:?}: {err:?}");
                    let gtid_cap = data_cap_set.delayed(new_gtid);
                    data_output
                        .give(&gtid_cap, ((*output_index, Err(err)), new_gtid.clone(), 1))
                        .await;
                    errored_tables.insert(table.clone());
                }
            }
        // Detect `DROP TABLE [IF EXISTS] <tbl>, <tbl>` statements. Since
        // this can drop multiple tables we just check all tables we care about
        } else if first.eq_ignore_ascii_case("drop") && second.eq_ignore_ascii_case("table") {
            let mut conn = connection_config
                .connect(
                    &format!("timely-{worker_id} MySQL "),
                    &config.config.connection_context.ssh_tunnel_manager,
                )
                .await?;
            let expected = table_info
                .iter()
                .filter(|(t, _)| !errored_tables.contains(t))
                .map(|(t, d)| (t, &d.1))
                .collect::<Vec<_>>();
            let schema_errors = verify_schemas(&mut *conn, &expected).await?;
            for (dropped_table, err) in schema_errors {
                if table_info.contains_key(dropped_table) && !errored_tables.contains(dropped_table)
                {
                    trace!(%id, "timely-{worker_id} DDL change \
                           dropped table: {dropped_table:?}: {err:?}");
                    let (output_index, _) = &table_info[&dropped_table];
                    let gtid_cap = data_cap_set.delayed(new_gtid);
                    data_output
                        .give(&gtid_cap, ((*output_index, Err(err)), new_gtid.clone(), 1))
                        .await;
                    errored_tables.insert(dropped_table.clone());
                }
            }

        // Detect `TRUNCATE [TABLE] <tbl>` statements
        } else if first.eq_ignore_ascii_case("truncate") {
            let table = if second.eq_ignore_ascii_case("table") {
                table_ident(
                    query_iter.next().ok_or_else(|| {
                        TransientError::Generic(anyhow::anyhow!("Invalid DDL query: {}", query))
                    })?,
                    &current_schema,
                )?
            } else {
                table_ident(second, &current_schema)?
            };
            if table_info.contains_key(&table) {
                trace!(%id, "timely-{worker_id} TRUNCATE detected \
                       for {table:?}");
                let (output_index, _) = &table_info[&table];
                let gtid_cap = data_cap_set.delayed(new_gtid);
                data_output
                    .give(
                        &gtid_cap,
                        (
                            (
                                *output_index,
                                Err(DefiniteError::TableTruncated(table.to_ast_string())),
                            ),
                            new_gtid.clone(),
                            1,
                        ),
                    )
                    .await;
                errored_tables.insert(table);
            }
        }
    };

    Ok(())
}
