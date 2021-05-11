// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use std::time::Duration;
use tokio_postgres::NoTls;

use ore::retry::Retry;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifySlotAction {
    connection: String,
    slot: String,
    active: bool,
}

pub fn build_verify_slot(mut cmd: BuiltinCommand) -> Result<VerifySlotAction, String> {
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
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, _: &mut State) -> Result<(), String> {
        let (client, conn) = tokio_postgres::connect(&self.connection, NoTls)
            .await
            .map_err(|e| format!("connecting to postgres: {}", e))?;
        println!(
            "Executing queries against PostgreSQL server at {}...",
            self.connection
        );
        let conn_handle = tokio::spawn(conn);

        Retry::default()
            .initial_backoff(Duration::from_millis(50))
            .max_duration(Duration::from_secs(3))
            .retry(|_| async {
                println!(">> checking for postgres replication slot {}", &self.slot);
                let rows = client
                    .query(
                        "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1::TEXT",
                        &[&self.slot],
                    )
                    .await
                    .map_err(|e| format!("querying postgres for replication slot: {}", e))?;

                if self.active {
                    if rows.len() != 1 {
                        return Err(format!(
                            "expected entry for slot {} in pg_replication slots, found {}",
                            &self.slot,
                            rows.len()
                        ));
                    }
                    let active_pid: Option<i32> = rows[0].get(0);
                    if active_pid.is_none() {
                        return Err(format!(
                            "expected slot {} to be active, is inactive",
                            &self.slot
                        ));
                    }
                } else {
                    if rows.len() != 0 {
                        return Err(format!(
                            "expected slot {} to be inactive, is active",
                            &self.slot
                        ));
                    }
                }
                Ok(())
            })
            .await?;

        drop(client);
        conn_handle
            .await
            .unwrap()
            .map_err(|e| format!("postgres connection error: {}", e))
    }
}
