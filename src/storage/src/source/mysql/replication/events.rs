// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use sqlparser::ast::{ObjectName, ObjectNamePart, ObjectType, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;

use maplit::btreemap;
use mysql_async::binlog::events::OptionalMetaExtractor;
use mysql_common::binlog::events::{QueryEvent, RowsEventData};
use mz_mysql_util::{MySqlError, pack_mysql_row};
use mz_ore::iter::IteratorExt;
use mz_repr::{Diff, Row};
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::mysql::GtidPartition;
use timely::progress::Timestamp;
use tracing::trace;

use crate::source::mysql::SourceOutputInfo;
use crate::source::types::{FuelSize, SourceMessage};

use super::super::schemas::verify_schemas;
use super::super::{DefiniteError, MySqlTableName, TransientError};
use super::context::ReplContext;

const DIALECT: MySqlDialect = MySqlDialect {};

/// Returns the MySqlTableName for the given table name referenced in a
/// SQL statement, using the current schema if the table name is unqualified.
fn table_ident(name: &str, current_schema: &str) -> Result<MySqlTableName, TransientError> {
    let stripped = name.replace('`', "");
    let mut name_iter = stripped.split('.');
    mysql_table_name(name_iter.next(), name_iter.next(), current_schema, name)
}

fn mysql_table_name(
    first_component: Option<&str>,
    second_component: Option<&str>,
    current_schema: &str,
    name: &str,
) -> Result<MySqlTableName, TransientError> {
    match (first_component, second_component) {
        (Some(t_name), None) => Ok(MySqlTableName::new(current_schema, t_name)),
        (Some(schema_name), Some(t_name)) => Ok(MySqlTableName::new(schema_name, t_name)),
        _ => Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent: {}",
            name
        ))),
    }
}

/// This function has the same intent as table_ident except handles the object name type parsed out of the sql by sqlparser rather than a raw string.
/// The ObjectName type is more flexible than the constraints for an identifier in mysql. i.e. it has a function type, which appears to only be supported
/// in snowflake. It also has a vector for identifier components, however a table identifier from the binlog should only have 1 or 2 components - never 0 or more than 2
fn table_ident_from_object_name(
    name: &ObjectName,
    current_schema: &str,
) -> Result<MySqlTableName, TransientError> {
    let processed_name_parts: Vec<String> = name.0.iter().map(|part| match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        // Functions for table name identifiers are a snowflake-specific concept, unexpected for mysql so we should fail hard.
        ObjectNamePart::Function(_) => Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent, function identifiers not supported in mysql: {}", name
        ))),
    }).collect::<Result<_, _>>()?;
    if processed_name_parts.len() != 1 && processed_name_parts.len() != 2 {
        return Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent: {}",
            name
        )));
    }

    let mut name_parts_iter = processed_name_parts.iter().map(|x| x.as_str());
    mysql_table_name(
        name_parts_iter.next(),
        name_parts_iter.next(),
        current_schema,
        &name.to_string(),
    )
}

/// Handles QueryEvents from the MySQL replication stream. Since we only use
/// row-based replication, we only expect to see QueryEvents for DDL changes.
///
/// From the MySQL docs: 'A Query_event is created for each query that modifies
/// the database, unless the query is logged row-based.' This means that we can
/// expect any DDL changes to be represented as QueryEvents, which we must parse
/// to figure out if any of the tables we care about have been affected.
///
/// This function returns a bool to represent whether the event that was handled
/// represents a 'complete' event that should cause the frontier to advance beyond
/// the current GTID.
pub(super) async fn handle_query_event(
    event: QueryEvent<'_>,
    ctx: &mut ReplContext<'_>,
    new_gtid: &GtidPartition,
) -> Result<bool, TransientError> {
    let (id, worker_id) = (ctx.config.id, ctx.config.worker_id);

    let query = event.query();
    let current_schema = event.schema();
    let mut is_complete_event = false;

    // MySQL does not permit transactional DDL, so luckily we don't need to
    // worry about tracking BEGIN/COMMIT query events. We only need to look
    // for DDL changes that affect the schema of the tables we care about.
    let mut query_iter = query.split_ascii_whitespace();
    let first = query_iter.next();
    let second = query_iter.next();
    match (
        first.map(str::to_ascii_lowercase).as_deref(),
        second.map(str::to_ascii_lowercase).as_deref(),
    ) {
        // Detect `ALTER TABLE <tbl>`, `RENAME TABLE <tbl>` statements
        (Some("alter") | Some("rename"), Some("table")) => {
            let table = table_ident(
                query_iter.next().ok_or_else(|| {
                    TransientError::Generic(anyhow::anyhow!("Invalid DDL query: {}", query))
                })?,
                &current_schema,
            )?;
            is_complete_event = true;
            if ctx.table_info.contains_key(&table) {
                trace!(%id, "timely-{worker_id} DDL change detected \
                       for {table:?}");
                let info = &ctx.table_info[&table];
                let mut conn = ctx
                    .connection_config
                    .connect(
                        &format!("timely-{worker_id} MySQL "),
                        &ctx.config.config.connection_context.ssh_tunnel_manager,
                    )
                    .await?;
                for (err_output, err) in
                    verify_schemas(&mut *conn, btreemap! { &table => info }).await?
                {
                    tracing::warn!(%id, "timely-{worker_id} DDL change \
                           verification error for {table:?}[{}]: {err:?}",
                           err_output.output_index);
                    let gtid_cap = ctx.data_cap_set.delayed(new_gtid);
                    let update = (
                        (err_output.output_index, Err(err.into())),
                        new_gtid.clone(),
                        Diff::ONE,
                    );
                    let size = update.fuel_size();
                    ctx.data_output.give_fueled(&gtid_cap, update, size).await;
                    ctx.errored_outputs.insert(err_output.output_index);
                }
            }
        }
        // Detect `DROP TABLE [IF EXISTS] <tbl>, <tbl>` statements.
        (Some("drop"), Some("table")) => {
            let dropped_tables = drop_table_identifiers(&current_schema, &query)?;

            // Sources referencing the dropped table name that were created before the table was dropped. Before is determined
            // by looking at the initial gtid set for the source and ensuring that's before the new gtid.
            let sources_to_drop: BTreeMap<&MySqlTableName, Vec<&SourceOutputInfo>> = dropped_tables
                .iter()
                .filter_map(|table_name| {
                    ctx.table_info
                        .get_key_value(table_name)
                        .map(|(name, info)| {
                            let kept = info
                                .iter()
                                .filter(|output| {
                                    !ctx.errored_outputs.contains(&output.output_index)
                                    // Only drop sources that were created before the table was dropped.
                                    && output.initial_gtid_set.less_equal(new_gtid)
                                })
                                .collect();
                            (name, kept)
                        })
                })
                .collect();
            is_complete_event = true;
            for (table_name, outputs) in sources_to_drop {
                tracing::info!(%id, "timely-{worker_id} DDL change dropped outputs: {outputs:?}");
                for output in outputs {
                    let err = DefiniteError::TableDropped(table_name.to_string());
                    let gtid_cap = ctx.data_cap_set.delayed(new_gtid);
                    let update = (
                        (output.output_index, Err(err.into())),
                        new_gtid.clone(),
                        Diff::ONE,
                    );
                    let size = std::mem::size_of_val(&update);
                    ctx.data_output.give_fueled(&gtid_cap, update, size).await;
                    ctx.errored_outputs.insert(output.output_index);
                }
            }
        }
        // Detect `TRUNCATE [TABLE] <tbl>` statements
        (Some("truncate"), Some(_)) => {
            // We need the original un-lowercased version of 'second' since it might be a table ref
            let second = second.expect("known to be Some");
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
            is_complete_event = true;
            if ctx.table_info.contains_key(&table) {
                trace!(%id, "timely-{worker_id} TRUNCATE detected \
                       for {table:?}");
                if let Some(info) = ctx.table_info.get(&table) {
                    let gtid_cap = ctx.data_cap_set.delayed(new_gtid);
                    for output in info {
                        let update = (
                            (
                                output.output_index,
                                Err(DataflowError::from(DefiniteError::TableTruncated(
                                    table.to_string(),
                                ))),
                            ),
                            new_gtid.clone(),
                            Diff::ONE,
                        );
                        let size = update.fuel_size();
                        ctx.data_output.give_fueled(&gtid_cap, update, size).await;
                        ctx.errored_outputs.insert(output.output_index);
                    }
                }
            }
        }
        // Detect `COMMIT` statements which signify the end of a transaction on non-XA capable
        // storage engines
        (Some("commit"), None) => {
            is_complete_event = true;
        }
        // Detect `CREATE TABLE <tbl>` statements which don't affect existing tables but do
        // signify a complete event (e.g. for the purposes of advancing the GTID)
        (Some("create"), Some("table")) => {
            // CREATE TABLE ... SELECT will have subsequent `RowEvent`s, to account for this, the statement contains the clause "START TRANSACTION".
            // https://dev.mysql.com/worklog/task/?id=13355

            let mut peek_stream = query_iter.peekable();
            let mut ctas = false;
            while let Some(token) = peek_stream.next() {
                if token.eq_ignore_ascii_case("start")
                    && peek_stream
                        .peek()
                        .is_some_and(|t| t.eq_ignore_ascii_case("transaction"))
                {
                    ctas = true;
                    break;
                }
            }
            is_complete_event = !ctas;
        }
        _ => {}
    }

    Ok(is_complete_event)
}

/// Handles parsing table names from "DROP TABLE [IF EXISTS] table1, table2, table3".
fn drop_table_identifiers(
    current_schema: &str,
    query: &str,
) -> Result<Vec<MySqlTableName>, TransientError> {
    let invalid =
        |msg| TransientError::Generic(anyhow::anyhow!("Invalid DDL query, {msg}, got: {query}"));

    let parse_result = Parser::parse_sql(&DIALECT, query)
        .map_err(|e| TransientError::Generic(anyhow::anyhow!(e)))?;
    let stmt = parse_result
        .iter()
        .exactly_one()
        .map_err(|_| invalid("expected only a single statement from the binlog"))?;

    let Statement::Drop {
        object_type,
        temporary,
        names,
        ..
    } = stmt
    else {
        return Err(invalid("expected DROP statement"));
    };

    if *object_type != ObjectType::Table || *temporary {
        return Err(invalid("expected DROP TABLE statement"));
    }
    let table_identifiers: Vec<MySqlTableName> = names
        .iter()
        .map(|name| table_ident_from_object_name(name, current_schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(table_identifiers)
}

/// Handles RowsEvents from the MySQL replication stream. These events contain
/// insert/update/delete events for a single transaction or committed statement.
///
/// We use these events to update the dataflow with the new rows, and return a new
/// frontier with which to advance the dataflow's progress.
pub(super) async fn handle_rows_event(
    event: RowsEventData<'_>,
    ctx: &mut ReplContext<'_>,
    new_gtid: &GtidPartition,
    event_buffer: &mut Vec<(
        (usize, Result<SourceMessage, DataflowError>),
        GtidPartition,
        mz_repr::Diff,
    )>,
) -> Result<(), TransientError> {
    let (id, worker_id) = (ctx.config.id, ctx.config.worker_id);

    // Find the relevant table
    let binlog_table_id = event.table_id();
    let table_map_event = ctx
        .stream
        .get_ref()
        .get_tme(binlog_table_id)
        .ok_or_else(|| TransientError::Generic(anyhow::anyhow!("Table map event not found")))?;
    let table = MySqlTableName::new(
        &*table_map_event.database_name(),
        &*table_map_event.table_name(),
    );

    let outputs = ctx.table_info.get(&table).map(|outputs| {
        outputs
            .into_iter()
            .filter(|output| !ctx.errored_outputs.contains(&output.output_index))
            .collect::<Vec<_>>()
    });
    let outputs = match outputs {
        Some(outputs) => outputs,
        None => {
            // We don't know about this table, or there are no un-errored outputs for it.
            return Ok(());
        }
    };

    trace!(%id, "timely-{worker_id} handling RowsEvent for {table:?}");

    // Capability for this event.
    let gtid_cap = ctx.data_cap_set.delayed(new_gtid);

    // We can check here if the binlog has full row metadata by looking at the column name optional
    // metadata, which is only present if full metadata is enabled.
    let optional_metadata = OptionalMetaExtractor::new(table_map_event.iter_optional_meta())?;
    let has_full_metadata = optional_metadata.iter_column_name().next().is_some();

    // Iterate over the rows in this RowsEvent. Each row is a pair of 'before_row', 'after_row',
    // to accomodate for updates and deletes (which include a before_row),
    // and updates and inserts (which inclued an after row).
    let mut final_row = Row::default();
    let mut rows_iter = event.rows(table_map_event);
    let mut rewind_count = 0;
    let mut additions = 0;
    let mut retractions = 0;
    while let Some(Ok((before_row, after_row))) = rows_iter.next() {
        // Update metrics for updates/inserts/deletes
        match (&before_row, &after_row) {
            (None, None) => {}
            (Some(_), Some(_)) => {
                ctx.metrics.updates.inc();
            }
            (None, Some(_)) => {
                ctx.metrics.inserts.inc();
            }
            (Some(_), None) => {
                ctx.metrics.deletes.inc();
            }
        }

        let updates = [
            before_row.map(|r| (r, Diff::MINUS_ONE)),
            after_row.map(|r| (r, Diff::ONE)),
        ];
        let gtid_str = format!("{new_gtid:?}");
        for (binlog_row, diff) in updates.into_iter().flatten() {
            let row = mysql_async::Row::try_from(binlog_row)?;
            for (output, row_val) in outputs.iter().repeat_clone(row) {
                let event = if !has_full_metadata && output.binlog_full_metadata {
                    ctx.errored_outputs.insert(output.output_index);
                    Err(DataflowError::from(DefiniteError::ValueDecodeError(
                        format!(
                            "Table {0} was created with binlog_row_metadata=FULL but binlog_row_metadata has since been set to a different value, meaning we cannot reliably decode the columns",
                            output.table_name
                        ),
                    )))
                } else {
                    match pack_mysql_row(
                        &mut final_row,
                        row_val,
                        &output.desc,
                        Some(&gtid_str),
                        output.binlog_full_metadata,
                    ) {
                        Ok(row) => Ok(SourceMessage {
                            key: Row::default(),
                            value: row,
                            metadata: Row::default(),
                        }),
                        // Produce a DefiniteError in the stream for any rows that fail to decode
                        Err(err @ MySqlError::ValueDecodeError { .. }) => Err(DataflowError::from(
                            DefiniteError::ValueDecodeError(err.to_string()),
                        )),
                        Err(err) => Err(err)?,
                    }
                };

                let data = (output.output_index, event);

                // Rewind this update if it was already present in the snapshot
                if let Some((_rewind_data_cap, rewind_req)) = ctx.rewinds.get(&output.output_index)
                {
                    if !rewind_req.snapshot_upper.less_equal(new_gtid) {
                        rewind_count += 1;
                        event_buffer.push((data.clone(), GtidPartition::minimum(), -diff));
                    }
                }
                if diff.is_positive() {
                    additions += 1;
                } else {
                    retractions += 1;
                }
                let update = (data, new_gtid.clone(), diff);
                let size = update.fuel_size();
                ctx.data_output.give_fueled(&gtid_cap, update, size).await;
            }
        }
    }

    // We want to emit data in individual pieces to allow timely to break large chunks of data into
    // containers. Naively interleaving new data and rewinds in the loop above defeats a timely
    // optimization that caches push buffers if the `.give()` time has not changed.
    //
    // Instead, we buffer rewind events into a reusable buffer, and emit all at once here at the end.

    if !event_buffer.is_empty() {
        for d in event_buffer.drain(..) {
            let (rewind_data_cap, _) = ctx.rewinds.get(&d.0.0).unwrap();
            let size = d.fuel_size();
            ctx.data_output.give_fueled(rewind_data_cap, d, size).await;
        }
    }

    trace!(
        %id,
        "timely-{worker_id} sent updates for {new_gtid:?} \
            with {} updates ({} additions, {} retractions) and {} \
            rewinds",
        additions + retractions,
        additions,
        retractions,
        rewind_count,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str) -> MySqlTableName {
        MySqlTableName::new(schema, name)
    }

    fn parse_drop(
        query: &str,
        current_schema: &str,
    ) -> Result<Vec<MySqlTableName>, TransientError> {
        let mut tokens = query.split_ascii_whitespace();
        let first = tokens.next();
        let second = tokens.next();
        match (
            first.map(str::to_ascii_lowercase).as_deref(),
            second.map(str::to_ascii_lowercase).as_deref(),
        ) {
            (Some("drop"), Some("table")) => drop_table_identifiers(current_schema, query),
            _ => Ok(Vec::new()),
        }
    }

    #[mz_ore::test]
    fn table_ident_unqualified_uses_current_schema() {
        assert_eq!(
            table_ident("orders", "shop").unwrap(),
            table("shop", "orders")
        );
    }

    #[mz_ore::test]
    fn table_ident_qualified_overrides_current_schema() {
        assert_eq!(
            table_ident("inventory.orders", "shop").unwrap(),
            table("inventory", "orders"),
        );
    }

    #[mz_ore::test]
    fn table_ident_strips_backtick_quoting() {
        assert_eq!(
            table_ident("`inventory`.`orders`", "shop").unwrap(),
            table("inventory", "orders"),
        );
    }

    #[mz_ore::test]
    fn drop_parses_single_unqualified_table() {
        assert_eq!(
            parse_drop("DROP TABLE orders", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_parses_qualified_table() {
        assert_eq!(
            parse_drop("DROP TABLE inventory.orders", "shop").unwrap(),
            vec![table("inventory", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_parses_backtick_quoted_table() {
        assert_eq!(
            parse_drop("DROP TABLE `inventory`.`orders`", "shop").unwrap(),
            vec![table("inventory", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_parses_if_exists_clause() {
        assert_eq!(
            parse_drop("DROP TABLE IF EXISTS orders", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
        assert_eq!(
            parse_drop("DROP TABLE if exists orders", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_rejects_if_without_exists() {
        assert!(parse_drop("DROP TABLE IF orders", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_rejects_missing_table_name() {
        assert!(parse_drop("DROP TABLE", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_parses_multiple_space_separated_tables() {
        assert_eq!(
            parse_drop("DROP TABLE orders, customers, items", "shop").unwrap(),
            vec![
                table("shop", "orders"),
                table("shop", "customers"),
                table("shop", "items"),
            ],
        );
    }

    #[mz_ore::test]
    fn drop_parses_comma_joined_tables_without_spaces() {
        assert_eq!(
            parse_drop("DROP TABLE `shop`.`orders`,`shop`.`customers`", "shop").unwrap(),
            vec![table("shop", "orders"), table("shop", "customers")],
        );
    }

    #[mz_ore::test]
    fn drop_does_not_treat_comment_shaped_text_in_identifier_as_comment() {
        assert_eq!(
            parse_drop("DROP TABLE `tbl /* not a comment */`", "shop").unwrap(),
            vec![table("shop", "tbl /* not a comment */")],
        );
    }

    #[mz_ore::test]
    fn drop_parses_table_with_restrict() {
        assert_eq!(
            parse_drop("DROP TABLE orders RESTRICT", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_parses_multiple_tables_with_cascade() {
        assert_eq!(
            parse_drop("DROP TABLE orders, customers CASCADE", "shop").unwrap(),
            vec![table("shop", "orders"), table("shop", "customers")],
        );
    }

    #[mz_ore::test]
    fn drop_of_multiple_statements_errors() {
        // Defensive only: a real `Query_event` never packs two top-level
        // statements together.
        assert!(parse_drop("DROP TABLE orders; DROP TABLE customers", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_temporary_table_is_not_detected() {
        assert_eq!(
            parse_drop("DROP TEMPORARY TABLE orders", "shop").unwrap(),
            Vec::<MySqlTableName>::new(),
        );
    }

    #[mz_ore::test]
    fn drop_quoted_identifier_with_dot_is_a_single_table() {
        assert_eq!(
            parse_drop("DROP TABLE `weird.name`", "shop").unwrap(),
            vec![table("shop", "weird.name")],
        );
    }

    #[mz_ore::test]
    fn drop_escaped_backtick_identifier_is_preserved() {
        assert_eq!(
            parse_drop("DROP TABLE `a``b`", "shop").unwrap(),
            vec![table("shop", "a`b")],
        );
    }

    #[mz_ore::test]
    fn identifier_with_multiple_dots_errors() {
        assert!(parse_drop("DROP TABLE def.shop.customer", "shop").is_err());
    }

    #[mz_ore::test]
    fn ansi_quotes_parsed_properly() {
        assert_eq!(
            parse_drop("DROP TABLE \"customer\"", "shop").unwrap(),
            vec![table("shop", "customer")],
        );
    }
}
