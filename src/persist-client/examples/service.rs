// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_persist_client::rpc::{
    GrpcPubSubClient, PersistGrpcPubSubServer, PersistPubSubClient, PersistPubSubClientConfig,
};
use mz_persist_client::ShardId;
use tracing::{info, Span};

use crate::BUILD_INFO;

#[derive(clap::ArgEnum, Copy, Clone, Debug)]
pub enum Role {
    Server,
    Writer,
    Reader,
}

#[derive(Debug, clap::Parser)]
pub struct Args {
    #[clap(long, value_name = "HOST:PORT", default_value = "127.0.0.1:6878")]
    listen_addr: SocketAddr,

    #[clap(long, arg_enum)]
    role: Role,

    connect_addrs: Vec<String>,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let span = Span::current();
    let shard_id = ShardId::from_str("s00000000-0000-0000-0000-000000000000").expect("shard id");
    let config = PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone());
    match args.role {
        Role::Server => {
            let _guard = span.enter();
            info!("listening on {}", args.listen_addr);
            PersistGrpcPubSubServer::new(&config, &MetricsRegistry::new())
                .serve(args.listen_addr.clone())
                .await
                .expect("server running");
            info!("server ded");
        }
        Role::Writer => {
            let connection = GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    url: format!("http://{}", args.listen_addr),
                    caller_id: "writer".to_string(),
                    persist_cfg: config.clone(),
                },
                Arc::new(Metrics::new(&config, &MetricsRegistry::new())),
            );

            let mut i = 0;
            loop {
                info!("writing");
                connection.sender.push_diff(
                    &shard_id,
                    &VersionedData {
                        seqno: SeqNo(i),
                        data: Bytes::default(),
                    },
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                i += 1;
            }
        }
        Role::Reader => {
            let mut connection = GrpcPubSubClient::connect(
                PersistPubSubClientConfig {
                    url: format!("http://{}", args.listen_addr),
                    caller_id: "reader".to_string(),
                    persist_cfg: config.clone(),
                },
                Arc::new(Metrics::new(&config, &MetricsRegistry::new())),
            );

            let _token = connection.sender.subscribe(&shard_id);
            while let Some(message) = connection.receiver.next().await {
                info!("client res: {:?}", message);
            }
            info!("stream to client ded");
        }
    }
    Ok(())
}
