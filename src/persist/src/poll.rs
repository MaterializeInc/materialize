// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_executor::block_on;
use mz_persist_types::Codec;

use crate::error::Error;
use crate::gen::persist::ProtoMeta;
use crate::storage::{BlobRead, BlobTail, BlobTailMetaStream};

/// WIP
#[derive(Clone, Debug)]
pub struct PollingBlob<B> {
    blob: B,
    interval: Duration,
}

impl<B: BlobRead + Sync> PollingBlob<B> {
    /// WIP
    pub fn new(blob: B, interval: Duration) -> Self {
        PollingBlob { blob, interval }
    }
}

#[async_trait]
impl<B: BlobRead + Sync> BlobRead for PollingBlob<B> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.blob.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.blob.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        self.blob.close().await
    }
}

/// WIP
#[derive(Debug)]
pub struct PollingBlobMetaStream<B> {
    blob: B,
    interval: Duration,
    prev: ProtoMeta,
    prev_poll: Instant,
}

impl<B: BlobRead> PollingBlobMetaStream<B> {
    async fn fetch_meta(&self) -> Result<ProtoMeta, Error> {
        let meta = self.blob.get("META").await?.ok_or("missing META")?;
        let meta = ProtoMeta::decode(&meta)?;
        Ok(meta)
    }
}

#[async_trait]
impl<B: BlobRead + Sync> BlobTailMetaStream for PollingBlobMetaStream<B> {
    async fn next(&mut self) -> Result<ProtoMeta, Error> {
        loop {
            let now = Instant::now();
            if self.prev_poll + self.interval > now {
                tokio::time::sleep(self.prev_poll + self.interval - now).await;
            }
            self.prev_poll = now;
            match self.fetch_meta().await {
                Ok(current) => {
                    if current.seqno > self.prev.seqno {
                        self.prev = current.clone();
                        return Ok(current);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl<B: BlobRead + Clone + Sync> BlobTail for PollingBlob<B> {
    type Stream = PollingBlobMetaStream<B>;

    fn tail_meta(&self) -> Result<Self::Stream, Error> {
        let now = Instant::now();
        let initial = block_on(self.blob.get("META"))?.ok_or("missing META")?;
        let initial = ProtoMeta::decode(&initial)?;
        Ok(PollingBlobMetaStream {
            blob: self.blob.clone(),
            interval: self.interval,
            prev: initial,
            prev_poll: now,
        })
    }
}
