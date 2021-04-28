// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use tokio_postgres::NoTls;

use postgres_util;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifySlotAction {
    connection: String,
    slot: String,
    exists: bool,
}

pub fn build_verify_slot(mut cmd: BuiltinCommand) -> Result<VerifySlotAction, String> {
    let connection = cmd.args.string("connection")?;
    let slot = cmd.args.string("slot")?;
    let exists: bool = cmd.args.parse("exists")?;
    cmd.args.done()?;
    Ok(VerifySlotAction {
        connection,
        slot,
        exists,
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

        println!(">> checking for postgres replication slot {}", &self.slot);
        let (active, _) = postgres_util::query_pg_replication_slots(&client, &self.slot)
            .await
            .map_err(|e| format!("querying postgres for replication slot: {}", e))?;

        if self.exists && !active {
            return Err(format!(
                "expected slot {} to be active, is inactive",
                &self.slot
            ));
        } else if !self.exists && active {
            return Err(format!(
                "expected slot {} to be inactive, is active",
                &self.slot
            ));
        }

        drop(client);
        conn_handle
            .await
            .unwrap()
            .map_err(|e| format!("postgres connection error: {}", e))
    }
}
