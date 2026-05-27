// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-shard subscriber registry with bounded broadcast.

use std::collections::BTreeMap;
use std::sync::Mutex;

use mz_persist::location::VersionedData;
use tokio::sync::broadcast;

const DEFAULT_BUFFER: usize = 64;

#[derive(Debug)]
pub struct SubscriberRegistry {
    channels: Mutex<BTreeMap<String, broadcast::Sender<VersionedData>>>,
    buffer: usize,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self::with_buffer(DEFAULT_BUFFER)
    }

    pub fn with_buffer(buffer: usize) -> Self {
        Self {
            channels: Mutex::new(BTreeMap::new()),
            buffer,
        }
    }

    pub fn register(&self, shard: &str) -> broadcast::Receiver<VersionedData> {
        let mut channels = self.channels.lock().expect("registry lock poisoned");
        let sender = channels
            .entry(shard.to_string())
            .or_insert_with(|| broadcast::channel(self.buffer).0);
        sender.subscribe()
    }

    pub fn publish(&self, shard: &str, data: VersionedData) {
        let sender = {
            let channels = self.channels.lock().expect("registry lock poisoned");
            channels.get(shard).cloned()
        };
        if let Some(s) = sender {
            let _ = s.send(data);
        }
    }
}

impl Default for SubscriberRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{SeqNo, VersionedData};

    fn v(seqno: u64) -> VersionedData {
        VersionedData {
            seqno: SeqNo(seqno),
            data: Bytes::from(vec![seqno as u8]),
        }
    }

    #[tokio::test]
    async fn broadcast_reaches_all_subscribers() {
        let registry = SubscriberRegistry::new();
        let mut a = registry.register("s1");
        let mut b = registry.register("s1");
        registry.publish("s1", v(1));
        assert_eq!(a.recv().await.unwrap().seqno, SeqNo(1));
        assert_eq!(b.recv().await.unwrap().seqno, SeqNo(1));
    }

    #[tokio::test]
    async fn other_shards_do_not_receive() {
        let registry = SubscriberRegistry::new();
        let mut a = registry.register("s1");
        registry.publish("s2", v(1));
        assert!(a.try_recv().is_err());
    }

    #[tokio::test]
    async fn slow_subscriber_drops_old_messages() {
        let registry = SubscriberRegistry::with_buffer(2);
        let mut a = registry.register("s1");
        registry.publish("s1", v(1));
        registry.publish("s1", v(2));
        registry.publish("s1", v(3));
        // With a buffer of 2 and 3 messages published, the receiver has lagged
        // by 1.  recv() surfaces the lag as Err(Lagged(_)); the next recv()
        // yields the oldest surviving message in the buffer.
        assert!(matches!(
            a.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        let recovered = a.recv().await.unwrap();
        assert!(recovered.seqno >= SeqNo(2));
    }
}
