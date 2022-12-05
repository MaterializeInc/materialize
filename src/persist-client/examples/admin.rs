// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Commands for read-write administration of persist state

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::{PersistConfig, ShardId};

use prometheus::proto::{MetricFamily, MetricType};
use std::str::FromStr;
use tracing::info;

use crate::inspect::StateArgs;
use crate::BUILD_INFO;

/// Commands for read-write administration of persist state
#[derive(Debug, clap::Args)]
pub struct AdminArgs {
    #[clap(subcommand)]
    command: Command,

    /// Whether to commit any modifications (defaults to dry run).
    #[clap(long)]
    pub(crate) commit: bool,
}

/// Individual subcommands of admin
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Manually completes all fueled compactions in a shard.
    ForceCompaction(ForceCompactionArgs),
}

/// Manually completes all fueled compactions in a shard.
#[derive(Debug, clap::Parser)]
pub(crate) struct ForceCompactionArgs {
    #[clap(flatten)]
    state: StateArgs,

    /// An upper bound on compaction's memory consumption.
    #[clap(long, default_value_t = 0)]
    compaction_memory_bound_bytes: usize,
}

pub async fn run(command: AdminArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::ForceCompaction(args) => {
            let shard_id = ShardId::from_str(&args.state.shard_id).expect("invalid shard id");
            let mut cfg = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone());
            if args.compaction_memory_bound_bytes > 0 {
                cfg.compaction_memory_bound_bytes = args.compaction_memory_bound_bytes;
            }
            let metrics_registry = MetricsRegistry::new();
            let () = mz_persist_client::admin::force_compaction(
                cfg,
                &metrics_registry,
                shard_id,
                &args.state.consensus_uri,
                &args.state.blob_uri,
                command.commit,
            )
            .await?;
            info_log_non_zero_metrics(&metrics_registry.gather());
        }
    }
    Ok(())
}

fn info_log_non_zero_metrics(metric_families: &[MetricFamily]) {
    for mf in metric_families {
        for m in mf.get_metric() {
            let val = match mf.get_field_type() {
                MetricType::COUNTER => m.get_counter().get_value(),
                MetricType::GAUGE => m.get_gauge().get_value(),
                x => unimplemented!("unhandled metric type: {:?}", x),
            };
            if val == 0.0 {
                continue;
            }
            let label_pairs = m.get_label();
            let mut labels = String::new();
            if !label_pairs.is_empty() {
                labels.push_str("{");
                for lb in label_pairs {
                    if labels != "{" {
                        labels.push_str(",");
                    }
                    labels.push_str(lb.get_name());
                    labels.push_str(":");
                    labels.push_str(lb.get_value());
                }
                labels.push_str("}");
            }
            info!("{}{} {}", mf.get_name(), labels, val);
        }
    }
}
