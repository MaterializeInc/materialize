// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs, clippy::clone_on_ref_ptr)]

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.r#impl.service.rs"
));

use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use differential_dataflow::lattice::Lattice;
use mz_persist::location::Blob;
use mz_persist_types::Codec64;
use mz_proto::{ProtoType, RustType};
use timely::progress::Timestamp;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::async_runtime::CpuHeavyRuntime;
use crate::cache::PersistClientCache;
use crate::r#impl::compact::{CompactReq, Compactor};
use crate::write::WriterId;
use crate::{Metrics, PersistConfig};

/// WIP
#[derive(Debug)]
pub struct PersistService {
    cfg: PersistConfig,
    metrics: Arc<Metrics>,
    cpu_heavy_runtime: Arc<CpuHeavyRuntime>,
    blob: Arc<dyn Blob + Send + Sync>,
}

impl PersistService {
    pub async fn new(cache: &mut PersistClientCache, blob_uri: String) -> Self {
        let blob = cache.open_blob(blob_uri).await.expect("WIP");
        PersistService {
            cfg: cache.cfg.clone(),
            metrics: Arc::clone(&cache.metrics),
            cpu_heavy_runtime: Arc::clone(&cache.cpu_heavy_runtime),
            blob,
        }
    }

    async fn compact_typed<T: Timestamp + Lattice + Codec64>(
        &self,
        req: ProtoCompactReq,
    ) -> Result<ProtoCompactRes, anyhow::Error> {
        let req: CompactReq<T> = req.into_rust().map_err(anyhow::Error::new)?;
        // WIP maintain a shard_id -> writer_id map
        let writer_id = WriterId::new();
        // WIP plumb diff_codec too
        let res = Compactor::compact::<T, i64>(
            self.cfg.clone(),
            Arc::clone(&self.blob),
            Arc::clone(&self.metrics),
            Arc::clone(&self.cpu_heavy_runtime),
            req,
            writer_id,
        )
        .await?;
        Ok(res.into_proto())
    }
}

#[async_trait]
impl proto_persist_server::ProtoPersist for PersistService {
    async fn compact(
        &self,
        req: Request<ProtoCompactReq>,
    ) -> Result<Response<ProtoCompactRes>, Status> {
        info!("req: {:?}", req);
        let req = req.into_inner();
        let res = match req.ts_codec.as_str() {
            "u64" => self.compact_typed::<u64>(req).await,
            ts_codec => Err(anyhow!("unknown codec64 {}", ts_codec)),
        };
        let res = match res {
            Ok(x) => Ok(Response::new(x)),
            // WIP how should we format errors
            Err(err) => Err(Status::invalid_argument(format!("{}", err))),
        };
        info!("res: {:?}", res);
        res
    }
}
