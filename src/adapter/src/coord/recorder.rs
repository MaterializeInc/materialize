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
//! A recorder runs a body query (a dTVC, typically over `CHANGES(...)`) and
//! records its deltas into a `DELTA TABLE`. This prototype takes the largest
//! shortcut available: instead of a standing dataflow plus an OCC commit at a
//! target `T`, the coordinator spawns a task that periodically re-executes the
//! body through an internal SQL self-connection as
//!
//! ```sql
//! INSERT INTO <target>
//! SELECT * FROM (<body>) AS __record
//! WHERE COALESCE(__record.mz_timestamp >
//!     (SELECT MAX(mz_timestamp) FROM <target>), TRUE);
//! ```
//!
//! The `max(mz_timestamp)` filter is the recorder's "consumed-through" frontier
//! (the design's "resume from the output's upper"): a one-off `CHANGES` read at
//! time `T` returns all deltas `<= T`, so every poll records exactly the deltas
//! that arrived since the previous poll, idempotently and restart-safe.
//! Freeze-by-typing falls out: a bare TVC reference joined in the body is
//! evaluated at the poll's processing time, and re-evaluations of old driver
//! deltas (after the reference changed) are discarded by the frontier filter.
//!
//! Shortcuts taken (prototype only):
//! * Recorders are in-memory coordinator state, not catalog items; they do not
//!   survive a restart (the recorded data does, and a re-created recorder
//!   resumes from the target's contents).
//! * Commits ride the ordinary table write path at an oracle-chosen timestamp;
//!   there is no atomic multi-action bundle commit at a chosen `T`.
//! * One action (`RECORD ... INTO`); no `INTEGRATE`/`DELETE` actions.

use std::time::Duration;

use mz_sql::plan::{CreateRecorderPlan, DropRecorderPlan};
use tracing::{info, warn};

use crate::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;

/// How often a recorder re-executes its body.
const TICK_INTERVAL: Duration = Duration::from_secs(1);

impl Coordinator {
    /// Creates a recorder: spawns its polling task and registers it.
    pub(crate) fn sequence_create_recorder(
        &mut self,
        plan: CreateRecorderPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CreateRecorderPlan {
            name,
            body_sql,
            target,
        } = plan;
        if self.recorders.contains_key(&name) {
            return Err(AdapterError::Unstructured(anyhow::anyhow!(
                "recorder {name} already exists"
            )));
        }
        // The COALESCE makes the empty-target case (MAX is NULL) record
        // everything, without needing a literal of type mz_timestamp.
        let insert_sql = format!(
            "INSERT INTO {target} \
             SELECT * FROM ({body_sql}) AS __record \
             WHERE COALESCE(__record.mz_timestamp > \
             (SELECT MAX(mz_timestamp) FROM {target}), TRUE)"
        );
        let task_name = format!("recorder-{name}");
        let recorder_name = name.clone();
        let handle = mz_ore::task::spawn(|| task_name, async move {
            recorder_loop(recorder_name, insert_sql).await;
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

/// The recorder's polling loop: (re)connect to the internal SQL listener and
/// re-execute the record statement every [`TICK_INTERVAL`].
async fn recorder_loop(name: String, insert_sql: String) {
    let addr = std::env::var("MZ_RECORDER_SQL_ADDR")
        .unwrap_or_else(|_| "postgres://mz_system@127.0.0.1:6877/materialize".into());
    info!(recorder = %name, sql = %insert_sql, "recorder starting");
    loop {
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
        loop {
            tokio::time::sleep(TICK_INTERVAL).await;
            match client.execute(&insert_sql, &[]).await {
                Ok(rows) if rows > 0 => {
                    info!(recorder = %name, rows, "recorder recorded deltas");
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(recorder = %name, "recorder tick failed: {err}");
                    if client.is_closed() {
                        break;
                    }
                }
            }
        }
    }
}
