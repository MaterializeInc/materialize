// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, bail};
use mz_ore::retry::Retry;
use mz_pgrepr::UInt8;
use mz_storage_types::sources::MzOffset;
use mz_timely_util::antichain::AntichainExt;
use timely::progress::Antichain;
use timely::PartialOrder;
use tokio_postgres::types::PgLsn;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;
use crate::util::postgres::postgres_client;

pub async fn run_await_ingestion(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let connection = cmd.args.string("connection")?;
    let source = cmd.args.opt_string("source");
    let progress_subsource = cmd.args.opt_string("progress-subsource");
    cmd.args.done()?;

    let progress_subsource = match (source, progress_subsource) {
        (None, Some(progress)) => progress,
        (Some(source), None) => format!("{source}_progress"),
        (None, None) => bail!("Missing argument. Specify one of `source` or `progress-subsource`"),
        (Some(_), Some(_)) => {
            bail!("Conflicting arguments. Specify one of `source` or `progress-subsource`")
        }
    };

    let client;
    let client = if connection.starts_with("postgres://") {
        let (client_inner, _) = postgres_client(&connection, state.default_timeout).await?;
        client = client_inner;
        &client
    } else {
        state
            .postgres_clients
            .get(&connection)
            .ok_or_else(|| anyhow!("connection '{}' not found", &connection))?
    };

    let row = client.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
    let lsn: PgLsn = row.get(0);
    let current_upper = Antichain::from_elem(MzOffset::from(u64::from(lsn)));

    // And now wait until the ingested frontier is beyond the upper we observed
    let query = format!(r#"SELECT "lsn" FROM {progress_subsource}"#);
    Retry::default()
        .max_duration(state.default_timeout)
        .retry_async_canceling(|_| async {
            let row_time = state.pgclient.query_one(&query, &[]).await?;
            let lsn = row_time.get::<_, UInt8>("lsn").0;

            let ingested_upper = Antichain::from_elem(MzOffset::from(lsn));

            if !PartialOrder::less_equal(&current_upper, &ingested_upper) {
                bail!(
                    "expected ingestion to proceed until {} but only reached {}",
                    current_upper.pretty(),
                    ingested_upper.pretty(),
                );
            }
            Ok(())
        })
        .await?;

    Ok(ControlFlow::Continue)
}
