// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use tokio_postgres::NoTls;

use mz_ore::retry::Retry;
use mz_ore::task;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct VerifySlotAction {
    connection: String,
    slot: String,
    active: bool,
}

pub fn build_verify_slot(mut cmd: BuiltinCommand) -> Result<VerifySlotAction, anyhow::Error> {
    let connection = cmd.args.string("connection")?;
    let slot = cmd.args.string("slot")?;
    let active: bool = cmd.args.parse("active")?;
    cmd.args.done()?;
    Ok(VerifySlotAction {
        connection,
        slot,
        active,
    })
}

#[async_trait]
impl Action for VerifySlotAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let (client, conn) = tokio_postgres::connect(&self.connection, NoTls)
            .await
            .context("connecting to postgres")?;
        println!(
            "Executing queries against PostgreSQL server at {}...",
            self.connection
        );
        let conn_handle = task::spawn(|| "verify_slot_action_pgconn", conn);

        Retry::default()
            .initial_backoff(Duration::from_millis(50))
            .max_duration(cmp::max(state.default_timeout, Duration::from_secs(10)))
            .retry_async_canceling(|_| async {
                println!(">> checking for postgres replication slot {}", &self.slot);
                let rows = client
                    .query(
                        "SELECT active_pid FROM pg_replication_slots WHERE slot_name LIKE $1::TEXT",
                        &[&self.slot],
                    )
                    .await
                    .context("querying postgres for replication slot")?;

                if self.active {
                    if rows.len() != 1 {
                        bail!(
                            "expected entry for slot {} in pg_replication slots, found {}",
                            &self.slot,
                            rows.len()
                        );
                    }
                    let active_pid: Option<i32> = rows[0].get(0);
                    if active_pid.is_none() {
                        bail!("expected slot {} to be active, is inactive", &self.slot);
                    }
                } else if rows.len() != 0 {
                    bail!("expected slot {} to be inactive, is active", &self.slot);
                }
                Ok(())
            })
            .await?;

        drop(client);
        conn_handle
            .await
            .unwrap()
            .context("postgres connection error")?;

        Ok(ControlFlow::Continue)
    }
}
