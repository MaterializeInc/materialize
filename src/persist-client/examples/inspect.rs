// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist command-line utilities

use mz_persist_client::ShardId;

use serde_json::json;
use std::str::FromStr;

/// Commands for inspecting current persist state
#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of inspect
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Prints latest consensus state as JSON
    State(StateArgs),

    /// Prints each consensus state change as JSON. Output includes the full consensus state
    /// before and after each state transitions:
    ///
    ///     {
    ///         "previous": previous_consensus_state,
    ///         "new": new_consensus_state,
    ///     }
    ///
    /// This is most helpfully consumed using a JSON diff tool like `jd`. A useful incantation
    /// to show only the changed fields between state transitions:
    ///
    ///     persistcli inspect state-diff --shard-id <shard> --consensus-uri <consensus_uri> |
    ///         while read diff; do
    ///             echo $diff | jq '.new' > temp_new
    ///             echo $diff | jq '.previous' > temp_previous
    ///             echo $diff | jq '.new.seqno'
    ///             jd -color -set temp_previous temp_new
    ///         done
    ///
    #[clap(verbatim_doc_comment)]
    StateDiff(StateArgs),
}

/// Arguments for viewing the current state of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateArgs {
    /// Shard to view
    #[clap(long)]
    shard_id: String,

    /// Consensus to use.
    ///
    /// When connecting to a deployed environment's consensus table, the Postgres/CRDB connection
    /// string must contain the database name and `options=--search_path=consensus`.
    ///
    /// When connecting to Cockroach Cloud, use the following format:
    ///
    ///   postgresql://<user>:$COCKROACH_PW@<hostname>:<port>/environment_<environment-id>
    ///     ?sslmode=verify-full
    ///     &sslrootcert=/path/to/cockroach-cloud/certs/cluster-ca.crt
    ///     &options=--search_path=consensus
    ///
    #[clap(long, verbatim_doc_comment)]
    consensus_uri: String,
}

pub async fn run(command: InspectArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::State(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state =
                mz_persist_client::inspect::fetch_current_state(shard_id, &args.consensus_uri)
                    .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state).expect("unserializable state")
            );
        }
        Command::StateDiff(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let states =
                mz_persist_client::inspect::fetch_state_diffs(shard_id, &args.consensus_uri)
                    .await?;
            for window in states.windows(2) {
                println!(
                    "{}",
                    json!({
                        "previous": window[0],
                        "new": window[1]
                    })
                );
            }
        }
    }

    Ok(())
}
