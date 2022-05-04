// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

//! An abstraction presenting as a durable time-varying collection (aka shard)

use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist::cfg::{BlobMultiConfig, ConsensusConfig};
use mz_persist::location::{BlobMulti, Consensus, ExternalError};
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tracing::{debug, trace};
use uuid::Uuid;

use crate::error::InvalidUsage;
use crate::r#impl::machine::Machine;
use crate::read::{ReadHandle, ReaderId};
use crate::write::{WriteHandle, WriterId};

pub mod error;
pub mod read;
pub mod write;

/// An implementation of the public crate interface.
///
/// TODO: Move this to another crate.
pub(crate) mod r#impl {
    pub mod machine;
    pub mod state;
}

// Notes
// - Pretend that everything marked with Serialize and Deserialize instead has
//   some sort of encode/decode API that uses byte slices, Buf+BufMut, or proto,
//   depending on what we decide.

// TODOs
// - Decide if the inner and outer error of the two-level errors should be
//   swapped.
// - Split errors into definitely failed vs maybe failed variants. More
//   generally, possibly we just overhaul our errors.
// - Figure out how to communicate that a lease expired.
// - Add a cache around location usage.
// - Crate mz_persist_client shouldn't depend on mz_persist. Pull common code
//   out into mz_persist_types or a new crate.
// - Hook up timeouts into various location impls.
// - Incremental state
// - After incremental state, state roll-ups and truncation
// - Permanent storage format for State
// - Non-polling listener
// - Impls and tests for setting upper to empty antichain (no more writes)
// - Impls and tests for setting since to empty antichain (no more reads)
// - Idempotence and retries + tests
// - Leasing
// - Physical and logical compaction
// - Garbage collection of blob and consensus data
// - Nemesis
// - Benchmarks
// - Logging
// - Tests, tests, tests
// - Test coverage of every `PartialOrder::less_*` call

/// A location in s3, other cloud storage, or otherwise "durable storage" used
/// by persist.
///
/// This structure can be durably written down or transmitted for use by other
/// processes. This location can contain any number of persist shards.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PersistLocation {
    /// Uri string that identifies the blob store.
    pub blob_uri: String,

    /// Uri string that identifies the consensus system.
    pub consensus_uri: String,
}

impl PersistLocation {
    /// Opens the associated implementations of [BlobMulti] and [Consensus].
    pub async fn open(
        &self,
        timeout: Duration,
    ) -> Result<
        (
            Arc<dyn BlobMulti + Send + Sync>,
            Arc<dyn Consensus + Send + Sync>,
        ),
        ExternalError,
    > {
        let deadline = Instant::now() + timeout;
        debug!(
            "Location::open timeout={:?} blob={} consensus={}",
            timeout, self.blob_uri, self.consensus_uri,
        );
        let blob = BlobMultiConfig::try_from(&self.blob_uri)
            .await?
            .open(deadline)
            .await?;
        let consensus = ConsensusConfig::try_from(&self.consensus_uri)
            .await?
            .open(deadline)
            .await?;
        Ok((blob, consensus))
    }
}

/// An opaque identifier for a persist durable TVC (aka shard).
///
/// The [std::string::ToString::to_string] format of this may be stored durably
/// or otherwise used as an interchange format. It can be parsed back using
/// [str::parse] or [std::str::FromStr::from_str].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId([u8; 16]);

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for ShardId {
    type Err = InvalidUsage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let u = match s.strip_prefix('s') {
            Some(x) => x,
            None => {
                return Err(InvalidUsage(anyhow!(
                    "invalid ShardId {}: incorrect prefix",
                    s
                )))
            }
        };
        let uuid = Uuid::parse_str(&u)
            .map_err(|err| InvalidUsage(anyhow!("invalid ShardId {}: {}", s, err)))?;
        Ok(ShardId(*uuid.as_bytes()))
    }
}

impl ShardId {
    /// Returns a random [ShardId] that is reasonably likely to have never been
    /// generated before.
    pub fn new() -> Self {
        ShardId(Uuid::new_v4().as_bytes().to_owned())
    }
}

/// A handle for interacting with the set of persist shard made durable at a
/// single [PersistLocation].
#[derive(Debug, Clone)]
pub struct PersistClient {
    blob: Arc<dyn BlobMulti + Send + Sync>,
    consensus: Arc<dyn Consensus + Send + Sync>,
}

impl PersistClient {
    /// Returns a new client for interfacing with persist shards made durable to
    /// the given `location`.
    ///
    /// The same `location` may be used concurrently from multiple processes.
    /// Concurrent usage is subject to the constraints documented on individual
    /// methods (mostly [WriteHandle::append]).
    pub async fn new(
        timeout: Duration,
        blob: Arc<dyn BlobMulti + Send + Sync>,
        consensus: Arc<dyn Consensus + Send + Sync>,
    ) -> Result<Self, ExternalError> {
        trace!(
            "Client::new timeout={:?} blob={:?} consensus={:?}",
            timeout,
            blob,
            consensus
        );
        // TODO: Verify somehow that blob matches consensus to prevent
        // accidental misuse.
        Ok(PersistClient { blob, consensus })
    }

    /// Provides capabilities for the durable TVC identified by `shard_id` at
    /// its current since and upper frontiers.
    ///
    /// This method is a best-effort attempt to regain control of the frontiers
    /// of a shard. Its most common uses are to recover capabilities that have
    /// expired (leases) or to attempt to read a TVC that one did not create (or
    /// otherwise receive capabilities for). If the frontiers have been fully
    /// released by all other parties, this call may result in capabilities with
    /// empty frontiers (which are useless).
    ///
    /// If `shard_id` has never been used before, initializes a new shard and
    /// returns handles with `since` and `upper` frontiers set to initial values
    /// of `Antichain::from_elem(T::minimum())`.
    pub async fn open<K, V, T, D>(
        &self,
        timeout: Duration,
        shard_id: ShardId,
    ) -> Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), ExternalError>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
    {
        trace!("Client::open timeout={:?} shard_id={:?}", timeout, shard_id);
        let deadline = Instant::now() + timeout;
        let mut machine = Machine::new(deadline, shard_id, Arc::clone(&self.consensus)).await?;
        let (writer_id, reader_id) = (WriterId::new(), ReaderId::new());
        let (write_cap, read_cap) = machine.register(deadline, &writer_id, &reader_id).await?;
        let writer = WriteHandle {
            writer_id,
            machine: machine.clone(),
            blob: Arc::clone(&self.blob),
            upper: write_cap.upper,
        };
        let reader = ReadHandle {
            reader_id,
            machine,
            blob: Arc::clone(&self.blob),
            since: read_cap.since,
        };

        Ok((writer, reader))
    }
}

#[cfg(test)]
const NO_TIMEOUT: Duration = Duration::from_secs(1_000_000);

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::str::FromStr;
    use std::task::Context;

    use futures_task::noop_waker;
    use mz_persist::mem::{MemBlobMulti, MemBlobMultiConfig, MemConsensus};
    use mz_persist::workload::DataGenerator;
    use timely::progress::Antichain;
    use timely::PartialOrder;
    use tokio::task::JoinHandle;

    use crate::read::ListenEvent;

    use super::*;

    pub async fn new_test_client() -> Result<PersistClient, ExternalError> {
        let blob = Arc::new(MemBlobMulti::open(MemBlobMultiConfig::default()));
        let consensus = Arc::new(MemConsensus::default());
        PersistClient::new(NO_TIMEOUT, blob, consensus).await
    }

    pub fn all_ok<'a, K, V, T, D, I>(
        iter: I,
        as_of: T,
    ) -> Vec<((Result<K, String>, Result<V, String>), T, D)>
    where
        K: Clone + 'a,
        V: Clone + 'a,
        T: Lattice + Clone + 'a,
        D: Clone + 'a,
        I: IntoIterator<Item = &'a ((K, V), T, D)>,
    {
        let as_of = Antichain::from_elem(as_of);
        iter.into_iter()
            .map(|((k, v), t, d)| {
                let mut t = t.clone();
                t.advance_by(as_of.borrow());
                ((Ok(k.clone()), Ok(v.clone())), t, d.clone())
            })
            .collect()
    }

    #[tokio::test]
    async fn sanity_check() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = new_test_client()
            .await?
            .open::<String, String, u64, i64>(NO_TIMEOUT, ShardId::new())
            .await?;
        assert_eq!(write.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));

        // Write a [0,3) batch.
        write
            .append(NO_TIMEOUT, &data[..2], Antichain::from_elem(3))
            .await??
            .expect("invalid current upper");
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        // Grab a snapshot and listener as_of 2.
        let mut snap = read.snapshot_one(1).await??;
        let mut listen = read.listen(NO_TIMEOUT, Antichain::from_elem(1)).await??;

        // Snapshot should only have part of what we wrote.
        assert_eq!(snap.read_all().await?, all_ok(&data[..1], 1));

        // Write a [3,4) batch.
        write
            .append(NO_TIMEOUT, &data[2..], Antichain::from_elem(4))
            .await??
            .expect("invalid current upper");
        assert_eq!(write.upper(), &Antichain::from_elem(4));

        // Listen should have part of the initial write plus the new one.
        let expected_events = vec![
            ListenEvent::Updates(all_ok(&data[1..2], 1)),
            ListenEvent::Progress(Antichain::from_elem(3)),
            ListenEvent::Updates(all_ok(&data[2..], 1)),
            ListenEvent::Progress(Antichain::from_elem(4)),
        ];
        assert_eq!(listen.read_until(&4).await?, expected_events);

        // Downgrading the since is tracked locally (but otherwise is a no-op).
        read.downgrade_since(NO_TIMEOUT, Antichain::from_elem(2))
            .await??;
        assert_eq!(read.since(), &Antichain::from_elem(2));

        Ok(())
    }

    #[tokio::test]
    async fn multiple_shards() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data1 = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let data2 = vec![(("1".to_owned(), ()), 1, 1), (("2".to_owned(), ()), 2, 1)];

        let client = new_test_client().await?;

        let (mut write1, read1) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, ShardId::new())
            .await?;

        // Different types, so that checks would fail in case we were not separating these
        // collections internally.
        let (mut write2, read2) = client
            .open::<String, (), u64, i64>(NO_TIMEOUT, ShardId::new())
            .await?;

        let res = write1
            .compare_and_append(
                NO_TIMEOUT,
                &data1[..],
                Antichain::from_elem(0),
                Antichain::from_elem(3),
            )
            .await??;
        assert_eq!(res, Ok(()));

        let res = write2
            .compare_and_append(
                NO_TIMEOUT,
                &data2[..],
                Antichain::from_elem(0),
                Antichain::from_elem(3),
            )
            .await??;
        assert_eq!(res, Ok(()));

        let mut snap = read1.snapshot_one(2).await??;
        assert_eq!(snap.read_all().await?, all_ok(&data1[..], 1));

        let mut snap = read2.snapshot_one(2).await??;
        assert_eq!(snap.read_all().await?, all_ok(&data2[..], 1));

        Ok(())
    }

    #[tokio::test]
    async fn fetch_upper() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let client = new_test_client().await?;

        let shard_id = ShardId::new();

        let (mut write1, _read1) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, shard_id)
            .await?;

        let (mut write2, _read2) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, shard_id)
            .await?;

        let res = write1
            .append(NO_TIMEOUT, &data[..], Antichain::from_elem(3))
            .await?;
        assert_eq!(res, Ok(Ok(())));

        // The shard-global upper does advance, even if this writer didn't advance its local upper.
        assert_eq!(
            write2.fetch_recent_upper(NO_TIMEOUT).await?,
            Antichain::from_elem(3)
        );

        // The writer-local upper should not advance if another writer advances the frontier.
        assert_eq!(write2.upper().clone(), Antichain::from_elem(0));

        Ok(())
    }

    #[tokio::test]
    async fn append_with_invalid_upper() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let client = new_test_client().await?;

        let shard_id = ShardId::new();

        let (mut write, _read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, shard_id)
            .await?;

        let res = write
            .append(NO_TIMEOUT, &data[..], Antichain::from_elem(3))
            .await?;
        assert_eq!(res, Ok(Ok(())));

        write.upper = Antichain::from_elem(0);
        let res = write
            .append(NO_TIMEOUT, &data[..], Antichain::from_elem(3))
            .await?;
        assert_eq!(res, Ok(Err(Antichain::from_elem(3))));

        // Writing with an outdated upper updates the write handle's upper to the correct upper.
        assert_eq!(write.upper(), &Antichain::from_elem(3));

        Ok(())
    }

    // Make sure that the API structs are Sync + Send, so that they can be used in async tasks.
    // NOTE: This is a compile-time only test. If it compiles, we're good.
    #[allow(unused)]
    async fn sync_send() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        fn is_send_sync<T: Send + Sync>(_x: T) -> bool {
            true
        }

        let client = new_test_client().await?;

        let (write, read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, ShardId::new())
            .await?;

        assert!(is_send_sync(client));
        assert!(is_send_sync(write));
        assert!(is_send_sync(read));

        Ok(())
    }

    #[tokio::test]
    async fn compare_and_append() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client().await?;
        let (mut write1, read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, id)
            .await?;

        let (mut write2, _read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, id)
            .await?;

        assert_eq!(write1.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(write2.upper(), &Antichain::from_elem(u64::minimum()));
        assert_eq!(read.since(), &Antichain::from_elem(u64::minimum()));

        // Write a [0,3) batch.
        let res = write1
            .compare_and_append(
                NO_TIMEOUT,
                &data[..2],
                Antichain::from_elem(0),
                Antichain::from_elem(3),
            )
            .await??;
        assert_eq!(res, Ok(()));
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        let mut snap = read.snapshot_one(2).await??;
        assert_eq!(snap.read_all().await?, all_ok(&data[..2], 1));

        // Try and write with the expected upper.
        let res = write2
            .compare_and_append(
                NO_TIMEOUT,
                &data[..2],
                Antichain::from_elem(0),
                Antichain::from_elem(3),
            )
            .await??;
        assert_eq!(res, Err(Antichain::from_elem(3)));

        // TODO(aljoscha): Should a writer forward its upper to the global upper on a failed
        // compare_and_append?
        assert_eq!(write2.upper(), &Antichain::from_elem(0));

        // Try again with a good expected upper.
        let res = write2
            .compare_and_append(
                NO_TIMEOUT,
                &data[2..],
                Antichain::from_elem(3),
                Antichain::from_elem(4),
            )
            .await??;
        assert_eq!(res, Ok(()));

        assert_eq!(write2.upper(), &Antichain::from_elem(4));

        let mut snap = read.snapshot_one(3).await??;
        assert_eq!(snap.read_all().await?, all_ok(&data, 1));

        Ok(())
    }

    #[tokio::test]
    async fn overlapping_append() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging_default("info");

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "vier".to_owned()), 4, 1),
            (("5".to_owned(), "cinque".to_owned()), 5, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client().await?;

        let (mut write1, read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, id)
            .await?;

        let (mut write2, _read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, id)
            .await?;

        // Grab a listener before we do any writing
        let mut listen = read.listen(NO_TIMEOUT, Antichain::from_elem(0)).await??;

        // Write a [0,3) batch.
        let res = write1
            .append(NO_TIMEOUT, &data[..2], Antichain::from_elem(3))
            .await??;
        assert_eq!(res, Ok(()));
        assert_eq!(write1.upper(), &Antichain::from_elem(3));

        // Write a [0,5) batch with the second writer.
        let res = write2
            .append(NO_TIMEOUT, &data[..4], Antichain::from_elem(5))
            .await??;
        assert_eq!(res, Ok(()));
        assert_eq!(write2.upper(), &Antichain::from_elem(5));

        // Write a [3,6) batch with the first writer.
        let res = write1
            .append(NO_TIMEOUT, &data[2..5], Antichain::from_elem(6))
            .await??;
        assert_eq!(res, Ok(()));
        assert_eq!(write1.upper(), &Antichain::from_elem(6));

        let mut snap = read.snapshot_one(5).await??;
        assert_eq!(snap.read_all().await?, all_ok(&data, 1));

        let expected_events = vec![
            ListenEvent::Updates(all_ok(&data[0..2], 1)),
            ListenEvent::Progress(Antichain::from_elem(3)),
            ListenEvent::Updates(all_ok(&data[2..4], 1)),
            ListenEvent::Progress(Antichain::from_elem(5)),
            ListenEvent::Updates(all_ok(&data[4..5], 1)),
            ListenEvent::Progress(Antichain::from_elem(6)),
        ];
        assert_eq!(listen.read_until(&6).await?, expected_events);

        Ok(())
    }

    #[test]
    fn fmt_ids() {
        assert_eq!(
            format!("{}", ShardId([0u8; 16])),
            "s00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            format!("{:?}", ShardId([0u8; 16])),
            "ShardId(00000000-0000-0000-0000-000000000000)"
        );
        assert_eq!(
            format!("{}", WriterId([0u8; 16])),
            "w00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            format!("{:?}", WriterId([0u8; 16])),
            "WriterId(00000000-0000-0000-0000-000000000000)"
        );
        assert_eq!(
            format!("{}", ReaderId([0u8; 16])),
            "r00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(
            format!("{:?}", ReaderId([0u8; 16])),
            "ReaderId(00000000-0000-0000-0000-000000000000)"
        );

        // ShardId can be parsed back from its Display/to_string format.
        assert_eq!(
            ShardId::from_str("s00000000-0000-0000-0000-000000000000"),
            Ok(ShardId([0u8; 16]))
        );
        assert_eq!(
            ShardId::from_str("x00000000-0000-0000-0000-000000000000"),
            Err(InvalidUsage(anyhow!(
                "invalid ShardId x00000000-0000-0000-0000-000000000000: incorrect prefix"
            )))
        );
        assert_eq!(
            ShardId::from_str("s0"),
            Err(InvalidUsage(anyhow!(
                "invalid ShardId s0: invalid length: expected one of [36, 32], found 1"
            )))
        );
        assert_eq!(
            ShardId::from_str("s00000000-0000-0000-0000-000000000000FOO"),
            Err(InvalidUsage(anyhow!(
                "invalid ShardId s00000000-0000-0000-0000-000000000000FOO: invalid length: expected one of [36, 32], found 39"
            )))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrency() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();

        let data = DataGenerator::small();

        const NUM_WRITERS: usize = 2;
        let id = ShardId::new();
        let client = new_test_client().await?;
        let mut handles = Vec::<JoinHandle<Result<(), anyhow::Error>>>::new();
        for idx in 0..NUM_WRITERS {
            let (data, client) = (data.clone(), client.clone());
            let handle = mz_ore::task::spawn(|| format!("writer-{}", idx), async move {
                let (mut write, _) = client
                    .open::<Vec<u8>, Vec<u8>, u64, i64>(NO_TIMEOUT, id)
                    .await?;
                for batch in data.batches() {
                    let new_upper = match batch.get(batch.len() - 1) {
                        Some((_, max_ts, _)) => Antichain::from_elem(max_ts + 1),
                        None => continue,
                    };
                    // Because we (intentionally) call open inside the task,
                    // some other writer may have raced ahead and already
                    // appended some data before this one was registered. As a
                    // result, this writer may not be starting with an upper of
                    // the initial empty antichain. This is nice because it
                    // mimics how a real HA source would work, but it means we
                    // have to skip any batches that have already been committed
                    // (otherwise our new_upper would be before our upper).
                    //
                    // Note however, that unlike a real source, our
                    // DataGenerator-derived batches are guaranteed to be
                    // chunked along the same boundaries. This means we don't
                    // have to consider partial batches when generating the
                    // updates below.
                    if PartialOrder::less_equal(&new_upper, write.upper()) {
                        continue;
                    }
                    let updates = batch
                        .iter()
                        .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d))
                        .collect::<Vec<_>>();
                    write
                        .append(NO_TIMEOUT, updates, new_upper)
                        .await??
                        .expect("invalid current upper");
                }
                Ok(())
            });
            handles.push(handle);
        }

        for handle in handles {
            let () = handle.await??;
        }

        let expected = data
            .records()
            .map(|((k, v), t, d)| ((Ok(k), Ok(v)), t, d))
            .collect::<Vec<_>>();
        let max_ts = expected.last().map(|(_, t, _)| *t).unwrap_or_default();
        let (_, read) = client
            .open::<Vec<u8>, Vec<u8>, u64, i64>(NO_TIMEOUT, id)
            .await?;
        let actual = read.snapshot_one(max_ts).await??.read_all().await?;
        assert_eq!(actual, expected);

        Ok(())
    }

    // Regression test for #12131. Snapshot with as_of >= upper would
    // immediately return the data currently available instead of waiting for
    // upper to advance past as_of.
    #[tokio::test]
    async fn regression_blocking_reads() -> Result<(), Box<dyn std::error::Error>> {
        mz_ore::test::init_logging();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let id = ShardId::new();
        let client = new_test_client().await?;
        let (mut write, read) = client
            .open::<String, String, u64, i64>(NO_TIMEOUT, id)
            .await?;

        // Grab a listener as_of (aka gt) 1, which is not yet closed out.
        let mut listen = read.listen(NO_TIMEOUT, Antichain::from_elem(1)).await??;
        let mut listen_next = Box::pin(listen.next(NO_TIMEOUT));
        // Intentionally don't await the listen_next, but instead manually poke
        // it for a while and assert that it doesn't resolve yet. See below for
        // discussion of some alternative ways of writing this unit test.
        for _ in 0..100 {
            assert!(
                Pin::new(&mut listen_next).poll(&mut cx).is_pending(),
                "listen::next unexpectedly ready"
            );
        }

        // Write a [0,3) batch.
        let res = write
            .compare_and_append(
                NO_TIMEOUT,
                &data[..2],
                Antichain::from_elem(0),
                Antichain::from_elem(3),
            )
            .await??;
        assert_eq!(res, Ok(()));

        // The initial listen_next call should now be able to return data at 2.
        // It doesn't get 1 because the as_of was 1 and listen is strictly gt.
        assert_eq!(
            listen_next.await?,
            vec![
                ListenEvent::Updates(vec![((Ok("2".to_owned()), Ok("two".to_owned())), 2, 1)]),
                ListenEvent::Progress(Antichain::from_elem(3)),
            ]
        );

        // Grab a snapshot as_of 3, which is not yet closed out. Intentionally
        // don't await the snap, but instead manually poke it for a while and
        // assert that it doesn't resolve yet.
        //
        // An alternative to this would be to run it in a task and poll the task
        // with some timeout, but this would introduce a fixed test execution
        // latency of the timeout in the happy case. Plus, it would be
        // non-deterministic.
        //
        // Another alternative (that's potentially quite interesting!) would be
        // to separate creating a snapshot immediately (which would fail if
        // as_of was >= upper) from a bit of logic that retries until that case
        // is ready.
        let mut snap = Box::pin(read.snapshot_one(3));
        for _ in 0..100 {
            assert!(
                Pin::new(&mut snap).poll(&mut cx).is_pending(),
                "snapshot unexpectedly ready"
            );
        }

        // Now add the data at 3 and also unblock the snapshot.
        let res = write
            .compare_and_append(
                NO_TIMEOUT,
                &data[2..],
                Antichain::from_elem(3),
                Antichain::from_elem(4),
            )
            .await??;
        assert_eq!(res, Ok(()));

        // Read the snapshot and check that it got all the appropriate data.
        //
        // TODO: If we made the SeqNo of the snap and the writes available, we
        // could assert on the ordering of them to provide additional confidence
        // that the test hasn't rotted as things change.
        let mut snap = snap.await??;
        assert_eq!(snap.read_all().await?, all_ok(&data[..], 1));

        Ok(())
    }
}
