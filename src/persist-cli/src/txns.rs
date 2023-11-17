// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{Diagnostics, PersistLocation, ShardId};
use mz_persist_txn::txns::{Tidy, TxnsHandle};
use mz_persist_types::codec_impls::{StringSchema, UnitSchema};
use tracing::{info_span, Instrument};

/// Txns benchmark.
#[derive(Debug, clap::Parser)]
pub struct Args {
    /// Number of data shards.
    #[clap(long, default_value_t = 10)]
    num_datas: usize,

    /// Number of txns.
    #[clap(long, default_value_t = 100)]
    num_txns: usize,

    /// Number of data shards used in each txn.
    #[clap(long, default_value_t = 3)]
    num_datas_in_txn: usize,

    /// Handle to the persist consensus system.
    #[clap(long, default_value = "mem://")]
    consensus_uri: String,

    /// Handle to the persist blob storage.
    #[clap(long, default_value = "mem://")]
    blob_uri: String,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();

    let location = PersistLocation {
        blob_uri: args.blob_uri.clone(),
        consensus_uri: args.consensus_uri.clone(),
    };
    let persist = PersistClientCache::new(
        PersistConfig::new(&mz_persist_client::BUILD_INFO, SYSTEM_TIME.clone()),
        &metrics_registry,
        |_, _| PubSubClientConnection::noop(),
    )
    .open(location)
    .await?;

    let init_ts = 0;
    let mut txns = TxnsHandle::<_, _, _, _>::open(
        init_ts,
        persist.clone(),
        ShardId::new(),
        Arc::new(StringSchema),
        Arc::new(UnitSchema),
    )
    // Note: the Future is intentionally boxed because it is very large.
    .boxed()
    .await;

    let data_ids = (0..args.num_datas)
        .map(|_| ShardId::new())
        .collect::<Vec<_>>();
    let data_writes = data_ids
        .iter()
        .map(|data_id| async {
            persist
                .open_writer::<String, (), u64, i64>(
                    *data_id,
                    Arc::new(StringSchema),
                    Arc::new(UnitSchema),
                    Diagnostics::from_purpose("data write"),
                )
                .await
                .expect("codecs should not change")
        })
        .collect::<FuturesUnordered<_>>();
    let data_writes = data_writes
        .collect::<Vec<_>>()
        .instrument(info_span!("data_write_open"))
        .await;
    let register_ts = init_ts + 1;
    txns.register(register_ts, data_writes)
        .instrument(info_span!("data_write_register"))
        .await
        .map_err(|ts| anyhow!("cannot register at {} must be {} or later", register_ts, ts))?;

    let mut commit_ts = register_ts + 1;
    let mut data_idx = 0;
    let mut tidy = Tidy::default();
    for txn_idx in 0..args.num_txns {
        let start = Instant::now();
        let mut txn = txns.begin();
        txn.tidy(std::mem::take(&mut tidy));
        for _ in 0..args.num_datas_in_txn {
            let data_id = &data_ids[data_idx % data_ids.len()];
            data_idx += 1;
            txn.write(data_id, txn_idx.to_string(), (), 1).await;
        }
        let t = async {
            anyhow::Result::<Tidy>::Ok(
                txn.commit_at(&mut txns, commit_ts)
                    .await
                    .map_err(|ts| {
                        anyhow!("could not commit at {} must be at least {}", commit_ts, ts)
                    })?
                    .apply(&mut txns)
                    .await,
            )
        }
        .instrument(info_span!("txn", txn_idx))
        .await?;
        commit_ts += 1;
        tidy.merge(t);

        let txns_finished = txn_idx + 1;
        if txns_finished % 100 == 0 || txns_finished == args.num_txns {
            tracing::info!(
                "finished {}/{} txns most recent took {:?}",
                txns_finished,
                args.num_txns,
                start.elapsed(),
            );
        } else {
            tracing::debug!(
                "finished {}/{} txns took {:?}",
                txns_finished,
                args.num_txns,
                start.elapsed(),
            );
        }
    }

    Ok(())
}
