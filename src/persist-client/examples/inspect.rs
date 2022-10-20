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

    /// Prints latest consensus rollup state as JSON
    StateRollup(StateArgs),

    /// Prints consensus rollup state of all known rollups as JSON
    StateRollups(StateArgs),

    /// Prints the count and size of blobs in an environment
    BlobCount(BlobCountArgs),

    /// Prints the unreferenced blobs across all shards
    UnreferencedBlobs(StateArgs),

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

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,
}

/// Arguments for viewing the blobs of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobCountArgs {
    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,
}

pub async fn run(command: InspectArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::State(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state = mz_persist_client::inspect::fetch_latest_state(
                shard_id,
                &args.consensus_uri,
                &args.blob_uri,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state).expect("unserializable state")
            );
        }
        Command::StateRollup(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state_rollup = mz_persist_client::inspect::fetch_latest_state_rollup(
                shard_id,
                &args.consensus_uri,
                &args.blob_uri,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollup).expect("unserializable state")
            );
        }
        Command::StateRollups(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state_rollups = mz_persist_client::inspect::fetch_state_rollups(
                shard_id,
                &args.consensus_uri,
                &args.blob_uri,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollups).expect("unserializable state")
            );
        }
        Command::StateDiff(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let states = mz_persist_client::inspect::fetch_state_diffs(
                shard_id,
                &args.consensus_uri,
                &args.blob_uri,
            )
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
        Command::BlobCount(args) => {
            let blob_counts = mz_persist_client::inspect::blob_counts(&args.blob_uri).await?;
            println!("{}", json!(blob_counts));
        }
        Command::UnreferencedBlobs(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let unreferenced_blobs = mz_persist_client::inspect::unreferenced_blobs(
                &shard_id,
                &args.consensus_uri,
                &args.blob_uri,
            )
            .await?;
            println!("{}", json!(unreferenced_blobs));
        }
    }

    Ok(())
}
