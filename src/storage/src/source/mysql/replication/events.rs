// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

fn table_ident_from_object_name(
    name: ObjectName,
    current_schema: &str,
) -> Result<MySqlTableName, TransientError> {
    let processed_name_parts: Vec<String> = name.0.iter().map(|part| match part {
        ObjectNamePart::Identifier(ident) => Ok(ident.value.clone()),
        // Functions for table name identifiers are a snowflake-specific concept, unexpected for mysql so we should fail hard.
        ObjectNamePart::Function(_) => Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent, function identifiers not supported in mysql: {}", name
        ))),
    }).collect::<Result<_, _>>()?;
    if !(processed_name_parts.len() > 0 && processed_name_parts.len() <= 2) {
        return Err(TransientError::Generic(anyhow::anyhow!(
            "Invalid table name from QueryEvent: {}",
            name
        )));
    }

    let mut name_parts_iter = processed_name_parts.iter();
    mysql_table_name(
        name_parts_iter.next().map(|x| x.as_str()),
        name_parts_iter.next().map(|x| x.as_str()),
        current_schema,
        &format!("{name}"),
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
        // Detect `DROP TABLE [IF EXISTS] <tbl>, <tbl>` statements. Since
        // this can drop multiple tables we just check all tables we care about
        (Some("drop"), Some("table")) => {
            let dropped_tables = drop_table_identifiers(&current_schema, &query)?;

            let tables_to_drop: BTreeMap<&MySqlTableName, Vec<&SourceOutputInfo>> = dropped_tables
                .iter()
                .filter_map(|table_name| {
                    ctx.table_info
                        .get_key_value(table_name)
                        .map(|(name, info)| {
                            let kept = info
                                .iter()
                                .filter(|output| {
                                    !ctx.errored_outputs.contains(&output.output_index)
                                })
                                .collect();
                            (name, kept)
                        })
                })
                .collect();
            is_complete_event = true;
            for (table_name, outputs) in tables_to_drop {
                for output in outputs {
                    let err = DefiniteError::TableDropped(table_name.to_string());
                    tracing::info!(%id, "timely-{worker_id} DDL change \
                            dropped output: {output:?}: {err:?}");
                    let gtid_cap = ctx.data_cap_set.delayed(new_gtid);
                    let update = (
                        (output.output_index, Err(err.into())),
                        new_gtid.clone(),
                        Diff::ONE,
                    );
                    let size = update.fuel_size();
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
    let invalid = || TransientError::Generic(anyhow::anyhow!("Invalid DDL query: {}", query));

    let parse_result = match Parser::parse_sql(&DIALECT, query) {
        Ok(result) => result,
        Err(e) => return Err(TransientError::Generic(anyhow::anyhow!(e))),
    };

    if parse_result.len() != 1 {
        return Err(invalid());
    }
    let stmt = parse_result.get(0).unwrap();
    let table_identifiers: Vec<MySqlTableName> = match stmt {
        Statement::Drop {
            object_type,
            temporary,
            names,
            ..
        } => match object_type {
            ObjectType::Table => {
                if *temporary {
                    return Err(invalid());
                }
                names
                    .iter()
                    .map(|name| table_ident_from_object_name(name.clone(), current_schema))
                    .collect::<Result<Vec<_>, _>>()?
            }
            _ => return Err(invalid()),
        },
        _ => return Err(invalid()),
    };
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

    /// Convenience constructor mirroring how the production code builds names.
    fn table(schema: &str, name: &str) -> MySqlTableName {
        MySqlTableName::new(schema, name)
    }

    /// Resolve which tables a binlog statement drops, mirroring
    /// [`handle_query_event`] end to end. Dispatch keys off the first two
    /// whitespace tokens, and only a leading `DROP TABLE` is handed to
    /// [`drop_table_identifiers`] (which re-parses the full statement with
    /// sqlparser). Anything the dispatch does not recognize as `DROP TABLE`
    /// yields no dropped tables, exactly as the production `_ => {}` arm does.
    ///
    /// Testing through this gate matters: a statement can parse fine on its own
    /// yet never reach the parser because the dispatch tokens don't line up
    /// (e.g. a comment sits between `DROP` and `TABLE`).
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
    fn drop_parses_if_exists_clause_case_insensitively() {
        // The optional `IF EXISTS` clause must be skipped before the table name.
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
        // A lone `IF` is malformed; it must error rather than be parsed as a
        // table named "if".
        assert!(parse_drop("DROP TABLE IF orders", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_rejects_missing_table_name() {
        // `DROP TABLE` with no table list cannot be parsed.
        assert!(parse_drop("DROP TABLE", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_parses_multiple_space_separated_tables() {
        // MySQL allows dropping several tables in one statement. Each tracked
        // table must be recognized so every affected output gets a
        // `TableDropped` error. Here the names are separated by ", ", so each
        // arrives as its own whitespace token with a trailing comma.
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

    // --- Comments in the statement -----------------------------------------
    //
    // sqlparser's MySqlDialect lexer strips comments, so a comment anywhere the
    // dispatch still recognizes as `DROP TABLE ...` is ignored and the table is
    // parsed normally.

    #[mz_ore::test]
    fn drop_ignores_block_comment_before_name() {
        assert_eq!(
            parse_drop("DROP TABLE /* a comment */ orders", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_ignores_trailing_block_comment() {
        assert_eq!(
            parse_drop("DROP TABLE orders /* trailing */", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_ignores_double_dash_line_comment() {
        assert_eq!(
            parse_drop("DROP TABLE orders -- line comment", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_ignores_hash_line_comment() {
        // `#` is a MySQL-specific line comment; the MySqlDialect lexer handles it.
        assert_eq!(
            parse_drop("DROP TABLE orders # mysql line comment", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_treats_executable_comment_body_as_sql() {
        // MySQL executable comments `/*!NNNNN ... */` are conditionally executed
        // SQL, not inert text. sqlparser parses their contents, so the table
        // inside is recognized.
        assert_eq!(
            parse_drop("DROP TABLE /*!40000 orders */", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_with_comment_between_keywords_is_not_detected() {
        // A comment between `DROP` and `TABLE` shifts the dispatch's second
        // token off `table`, so the statement never reaches the parser and the
        // drop is silently missed. This is a gap in the whitespace-based
        // dispatch in `handle_query_event`, not in `drop_table_identifiers`.
        assert_eq!(
            parse_drop("DROP /* mid */ TABLE orders", "shop").unwrap(),
            Vec::<MySqlTableName>::new(),
        );
    }

    #[mz_ore::test]
    fn drop_wrapped_entirely_in_executable_comment_is_not_detected() {
        // `/*!40000 DROP TABLE orders */` is a drop to MySQL, but the dispatch's
        // first token is the comment, so it is never recognized. Same dispatch
        // gap as above.
        assert_eq!(
            parse_drop("/*!40000 DROP TABLE orders */", "shop").unwrap(),
            Vec::<MySqlTableName>::new(),
        );
    }

    // --- Comment-shaped text inside identifiers ----------------------------

    #[mz_ore::test]
    fn drop_does_not_treat_comment_shaped_text_in_identifier_as_comment() {
        // Comment delimiters inside a backtick-quoted identifier are part of the
        // name, not a comment, and must be preserved verbatim.
        assert_eq!(
            parse_drop("DROP TABLE `tbl /* not a comment */`", "shop").unwrap(),
            vec![table("shop", "tbl /* not a comment */")],
        );
    }

    // --- Optional DROP clauses ---------------------------------------------

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

    // --- Statement / delimiter shape ---------------------------------------
    //
    // A `Query_event` carries exactly one statement (MySQL logs one statement
    // per event; `DROP TABLE a, b` is a single multi-name statement, not two),
    // so the realistic shapes here are "one statement, maybe with a trailing
    // delimiter". The multi-statement case is exercised only as defensive
    // documentation -- it should not arise from a real binlog.
    //
    // Note on the error path: when `drop_table_identifiers` returns `Err`, it
    // propagates as a `TransientError` out of `handle_query_event`, which
    // restarts replication from the last committed GTID -- re-reading the same
    // event and failing again, stalling the source. The genuinely reachable way
    // to hit that is sqlparser rejecting an unusual-but-valid *single* DROP
    // (a dialect gap), not the contrived multi-statement input below.

    #[mz_ore::test]
    fn drop_ignores_trailing_semicolon() {
        // A trailing statement delimiter must not look like a second (empty)
        // statement: sqlparser skips empty statements, so this stays a single
        // `DROP` and parses normally.
        assert_eq!(
            parse_drop("DROP TABLE orders;", "shop").unwrap(),
            vec![table("shop", "orders")],
        );
    }

    #[mz_ore::test]
    fn drop_of_multiple_statements_errors() {
        // Defensive only: a real `Query_event` never packs two top-level
        // statements together. If it somehow did, the `len() != 1` check rejects
        // it (which, per the note above, would stall rather than crash).
        assert!(parse_drop("DROP TABLE orders; DROP TABLE customers", "shop").is_err());
    }

    #[mz_ore::test]
    fn drop_temporary_table_is_not_detected() {
        // `DROP TEMPORARY TABLE` has `TEMPORARY` as its second token, so the
        // dispatch never enters the `DROP TABLE` arm: the explicit `temporary`
        // rejection inside `drop_table_identifiers` is unreachable via the
        // production path, and the statement is simply ignored (temporary tables
        // are never tracked anyway).
        assert_eq!(
            parse_drop("DROP TEMPORARY TABLE orders", "shop").unwrap(),
            Vec::<MySqlTableName>::new(),
        );
    }

    // --- Quoted-identifier edge cases --------------------------------------
    //
    // Because table names are taken from sqlparser's structured `Ident` values
    // rather than re-rendered SQL, characters that are only special at the SQL
    // level -- the `.` separator and backtick quoting -- are handled correctly
    // inside a quoted identifier.

    #[mz_ore::test]
    fn drop_quoted_identifier_with_dot_is_a_single_table() {
        // `` `weird.name` `` is one table named "weird.name" in the current
        // schema; the dot is part of the name, not a schema/table separator.
        assert_eq!(
            parse_drop("DROP TABLE `weird.name`", "shop").unwrap(),
            vec![table("shop", "weird.name")],
        );
    }

    #[mz_ore::test]
    fn drop_escaped_backtick_identifier_is_preserved() {
        // ``` `a``b` ``` is one table named "a`b" (a doubled backtick escapes a
        // literal backtick); the escape is decoded and the backtick kept.
        assert_eq!(
            parse_drop("DROP TABLE `a``b`", "shop").unwrap(),
            vec![table("shop", "a`b")],
        );
    }

    #[mz_ore::test]
    fn identifier_with_multiple_dots_errors() {
        assert!(parse_drop("DROP TABLE def.shop.customer", "shop").is_err());
    }
}
