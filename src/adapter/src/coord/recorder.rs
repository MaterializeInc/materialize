// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prototype runtime for `RECORDER`s (design doc `20260604_recorders.md`).
//!
//! A recorder binds named relations (dTVCs, typically over `CHANGES(...)`)
//! and a set of actions over them. This prototype takes the largest shortcut
//! available: instead of a standing dataflow plus an OCC commit at a target
//! `T`, the coordinator spawns a task that periodically re-executes the
//! actions through an internal SQL self-connection.
//!
//! Per tick, each action becomes one SQL statement (named relations are
//! prefixed as CTEs):
//!
//! * `RECORD r INTO t` —
//!   ```sql
//!   INSERT INTO t WITH <rels...>, __record AS (SELECT * FROM r)
//!   SELECT * FROM __record
//!   WHERE COALESCE(__record.mz_timestamp >
//!       (SELECT MAX(mz_timestamp) FROM t), TRUE);
//!   ```
//!   The `max(mz_timestamp)` filter is the recorder's "consumed-through"
//!   frontier (the design's "resume from the output's upper"): a one-off
//!   `CHANGES` read at time `T` returns all deltas `<= T`, so every poll
//!   records exactly the deltas that arrived since the previous poll,
//!   idempotently and restart-safe. Freeze-by-typing falls out: a bare TVC
//!   reference joined in the body is evaluated at the poll's processing time,
//!   and re-evaluations of old driver deltas (after the reference changed)
//!   are discarded by the frontier filter.
//! * `INTEGRATE r AS v` — a one-time `CREATE VIEW v AS WITH <rels...>
//!   SELECT * FROM r` (a maintained view over the durable delta table; the
//!   design's "recomputes and self-corrects" restart behavior for free).
//! * `DELETE r FROM t` — `DELETE FROM t WHERE (<cols>) IN (SELECT <cols>
//!   FROM ...)`, with `t`'s column list fetched from the catalog. `r` must
//!   select full rows of `t`.
//!
//! Shortcuts taken (prototype only):
//! * Recorders are in-memory coordinator state, not catalog items; they do
//!   not survive a restart (the recorded data does, and a re-created recorder
//!   resumes from the target's contents).
//! * Actions run sequentially per tick, not as one atomic bundle at a chosen
//!   `T`; cross-action consistency is eventual (the design's `T+1` visibility
//!   rule, stretched to one tick).
//! * Reads of the recorder's own outputs observe the previously committed
//!   state (the design's pre-commit snapshot rule, for free).

use std::time::Duration;

use mz_sql::plan::{CreateRecorderPlan, DropRecorderPlan, RecorderActionPlan};
use tracing::{info, warn};

use crate::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;

/// How often a recorder re-executes its actions.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

impl Coordinator {
    /// Creates a recorder: spawns its polling task and registers it.
    pub(crate) fn sequence_create_recorder(
        &mut self,
        plan: CreateRecorderPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateRecorderPlan {
            name,
            rels,
            actions,
        } = plan;
        if self.recorders.contains_key(&name) {
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "recorder {name} already exists"
            )));
        }
        let task_name = format!("recorder-{name}");
        let recorder_name = name.clone();
        let handle = mz_ore::task::spawn(|| task_name, async move {
            recorder_loop(recorder_name, rels, actions).await;
        });
        self.recorders.insert(name, handle.abort_on_drop());
        Ok(ExecuteResponse::CreatedRecorder)
    }

    /// Drops a recorder: deregisters it, aborting its task.
    pub(crate) fn sequence_drop_recorder(
        &mut self,
        plan: DropRecorderPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        match self.recorders.remove(&plan.name) {
            // Dropping the handle aborts the task.
            Some(_handle) => Ok(ExecuteResponse::DroppedRecorder),
            None => Err(AdapterError::Unstructured(anyhow::anyhow!(
                "unknown recorder {}",
                plan.name
            ))),
        }
    }
}

/// Builds the `WITH <name> AS (<body>), ...` prefix containing the relations
/// the relation `root` (transitively) references, in declaration order.
///
/// References are detected textually (word match) — fine for the prototype,
/// where relation names are simple identifiers.
fn cte_prefix(rels: &[(String, String)], root: &str) -> String {
    let references = |sql: &str, name: &str| {
        sql.split(|c: char| !(c.is_alphanumeric() || c == '_'))
            .any(|word| word == name)
    };
    // Iterate to a fixpoint: a relation is needed if the root or any needed
    // relation references it.
    let mut needed = vec![false; rels.len()];
    loop {
        let mut changed = false;
        for (i, (name, _)) in rels.iter().enumerate() {
            if needed[i] {
                continue;
            }
            let root_refs = root == name;
            let rel_refs = rels
                .iter()
                .zip(needed.iter())
                .any(|((_, sql), n)| *n && references(sql, name));
            if root_refs || rel_refs {
                needed[i] = true;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let ctes: Vec<String> = rels
        .iter()
        .zip(needed.iter())
        .filter(|(_, n)| **n)
        .map(|((name, sql), _)| format!("{name} AS ({sql})"))
        .collect();
    if ctes.is_empty() {
        String::new()
    } else {
        format!("WITH {} ", ctes.join(", "))
    }
}

/// The SQL for one tick of a `RECORD` action.
fn record_sql(rels: &[(String, String)], action: &RecorderActionPlan) -> Option<String> {
    let RecorderActionPlan::Record {
        rel,
        query_sql,
        into,
    } = action
    else {
        return None;
    };
    let (mut prefix, body) = match (rel, query_sql) {
        (Some(rel), _) => (cte_prefix(rels, rel), format!("SELECT * FROM {rel}")),
        (None, Some(sql)) => (String::new(), sql.clone()),
        (None, None) => return None,
    };
    if prefix.is_empty() {
        prefix = "WITH ".into();
    } else {
        prefix.truncate(prefix.len() - 1);
        prefix.push_str(", ");
    }
    // The COALESCE makes the empty-target case (MAX is NULL) record
    // everything, without needing a literal of type mz_timestamp.
    Some(format!(
        "INSERT INTO {into} {prefix}__record AS ({body}) \
         SELECT * FROM __record \
         WHERE COALESCE(__record.mz_timestamp > \
         (SELECT MAX(mz_timestamp) FROM {into}), TRUE)"
    ))
}

/// The recorder's polling loop: (re)connect to the internal SQL listener,
/// perform one-time setup (`INTEGRATE` views, `DELETE` column lists), and
/// re-execute the `RECORD`/`DELETE` actions every [`TICK_INTERVAL`].
async fn recorder_loop(
    name: String,
    rels: Vec<(String, String)>,
    actions: Vec<RecorderActionPlan>,
) {
    // Run as the default user on the external listener, so objects the
    // recorder creates (INTEGRATE views) and reads share ownership with the
    // user's objects. (Local bin/environmentd has no authentication.)
    let addr = std::env::var("MZ_RECORDER_SQL_ADDR")
        .unwrap_or_else(|_| "postgres://materialize@127.0.0.1:6875/materialize".into());
    info!(recorder = %name, "recorder starting");
    'reconnect: loop {
        let client = match tokio_postgres::connect(&addr, tokio_postgres::NoTls).await {
            Ok((client, conn)) => {
                let conn_task = format!("recorder-{name}-conn");
                mz_ore::task::spawn(|| conn_task, async move {
                    // Terminates when the client is dropped; an error here
                    // surfaces as a failed query in the tick loop.
                    let _ = conn.await;
                });
                client
            }
            Err(err) => {
                warn!(recorder = %name, "recorder connect failed: {err}");
                tokio::time::sleep(TICK_INTERVAL).await;
                continue;
            }
        };

        // One-time setup, idempotent across reconnects: INTEGRATE views and
        // the per-tick statements for RECORD/DELETE actions.
        let mut tick_sqls = Vec::new();
        for action in &actions {
            match action {
                RecorderActionPlan::Record { .. } => {
                    tick_sqls.extend(record_sql(&rels, action));
                }
                RecorderActionPlan::Integrate { rel, view } => {
                    let prefix = cte_prefix(&rels, rel);
                    let sql = format!("CREATE VIEW {view} AS {prefix}SELECT * FROM {rel}");
                    match client.execute(&sql, &[]).await {
                        Ok(_) => info!(recorder = %name, view, "recorder created integrate view"),
                        // "already exists" is fine (re-create or reconnect).
                        Err(err) if err.to_string().contains("already exists") => {}
                        Err(err) => {
                            warn!(recorder = %name, "recorder integrate failed: {err}");
                            tokio::time::sleep(TICK_INTERVAL).await;
                            continue 'reconnect;
                        }
                    }
                }
                RecorderActionPlan::Delete { rel, from } => {
                    // Fetch the target's column list; `rel` must select full
                    // rows of `from`.
                    let bare = from.rsplit('.').next().unwrap_or(from).replace('"', "");
                    let cols_query = "SELECT column_name FROM information_schema.columns \
                         WHERE table_name = $1 ORDER BY ordinal_position";
                    let cols: Vec<String> = match client.query(cols_query, &[&bare]).await {
                        Ok(rows) => rows.iter().map(|r| r.get::<_, String>(0)).collect(),
                        Err(err) => {
                            warn!(recorder = %name, "recorder column fetch failed: {err}");
                            tokio::time::sleep(TICK_INTERVAL).await;
                            continue 'reconnect;
                        }
                    };
                    if cols.is_empty() {
                        warn!(recorder = %name, from, "recorder delete target has no columns");
                        tokio::time::sleep(TICK_INTERVAL).await;
                        continue 'reconnect;
                    }
                    let col_list = cols.join(", ");
                    let prefix = cte_prefix(&rels, rel);
                    tick_sqls.push(format!(
                        "DELETE FROM {from} WHERE ({col_list}) IN \
                         (SELECT {col_list} FROM ({prefix}SELECT * FROM {rel}) AS __delete)"
                    ));
                }
            }
        }
        for sql in &tick_sqls {
            info!(recorder = %name, sql = %sql, "recorder tick statement");
        }

        loop {
            tokio::time::sleep(TICK_INTERVAL).await;
            for sql in &tick_sqls {
                match client.execute(sql.as_str(), &[]).await {
                    Ok(rows) if rows > 0 => {
                        info!(recorder = %name, rows, "recorder applied action");
                    }
                    Ok(_) => {}
                    Err(err) => {
                        warn!(recorder = %name, "recorder tick failed: {err}");
                        if client.is_closed() {
                            continue 'reconnect;
                        }
                    }
                }
            }
        }
    }
}
