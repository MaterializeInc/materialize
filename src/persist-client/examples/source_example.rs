// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An example that shows how a highly-available (HA) persistent source could be implemented on top
//! of the new persist API, along with a way to do compare-and-set updates of a collection of
//! timestamp bindings. In our case, what we need to make the source HA is that multiple instances
//! can run concurrently and produce identical data. We achieve this by making the minting of new
//! timestamp bindings "go through" consensus. In our case, some system that supports a
//! compare-and-set operation.
//!
//! The example consists of five parts, an [`api`] module, a [`types`] module, an [`impls`] module,
//! a [`render`] module, and a [`reader`] module. The `api` module has traits for defining sources
//! and a timestamper, `impls` has example implementations of these traits, and [`render`] can
//! spawn (or "render") the machinery to read from a source and write it into persistence. Module
//! [`reader`] has code for a persist consumer, it reads and prints the data that the source
//! pipeline writes to persistence.

// TODO(aljoscha): Figure out which of the Send + Sync shenanigans are needed and/or if the
// situation can be improved.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::async_runtime::CpuHeavyRuntime;
use tracing::{error, trace};

use mz_ore::now::SYSTEM_TIME;
use mz_persist::location::{Blob, Consensus, ExternalError, Indeterminate};
use mz_persist::unreliable::{UnreliableBlob, UnreliableConsensus, UnreliableHandle};
use mz_persist_client::{Metrics, PersistClient, PersistConfig, PersistLocation};

use self::impls::ConsensusTimestamper;
use self::impls::PersistConsensus;
use self::impls::SequentialSource;
use self::types::Timestamp;

/// Simplified model of the mz source pipeline and its usage of persist.
#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    /// Blob to use
    #[clap(long)]
    blob_uri: String,

    /// Consensus to use
    #[clap(long)]
    consensus_uri: String,

    /// How much unreliability to inject into Blob and Consensus usage
    ///
    /// This value must be in [0, 1]. 0 means no-op, 1 means complete
    /// unreliability.
    #[clap(long, default_value_t = 0.0)]
    unreliability: f64,
}

async fn retry_mut<F, I, T>(name: &str, input: &mut I, mut action: F) -> T
where
    F: for<'a> FnMut(&'a mut I) -> BoxFuture<'a, Result<T, Indeterminate>>,
{
    let mut i = 0;
    loop {
        let result = action(input);
        match result.await {
            Ok(ok) => {
                trace!("success after {i} retries",);
                return ok;
            }
            Err(e) => {
                trace!("attempt {i} of {name} failed, retrying: {:?}", e);
            }
        }
        i += 1;
    }
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let batch_size = 2;
    let timestamp_interval = Duration::from_secs(1);
    let num_readers = 1;

    // Make this artificially slow so we can eyeball the output.
    let source_wait_time = Duration::from_secs(2);
    let now_fn = SYSTEM_TIME.clone();

    let source = SequentialSource::new(2, source_wait_time);

    let persist = persist_client(args).await?;

    let bindings_id = "sa3150c18-f7a0-4062-b189-463531b9f36a"
        .parse()
        .map_err(anyhow::Error::msg)?;
    let data_id = "s1ba38f23-e28f-41fc-a92b-cddef58c48ce"
        .parse()
        .map_err(anyhow::Error::msg)?;

    let (bindings_write, bindings_read) = persist.open(bindings_id).await?;

    let consensus = PersistConsensus::new(bindings_write, bindings_read);
    let timestamper =
        ConsensusTimestamper::new(now_fn.clone(), timestamp_interval.clone(), consensus).await;

    let (data_write, data_read) = persist.open(data_id).await?;

    // First, render one instance of the source.
    let source1 = source.clone();
    let source_pipeline1 = mz_ore::task::spawn(|| "source-1", async move {
        if let Err(e) = render::spawn_source_pipeline(
            "source-1".to_string(),
            source1,
            timestamper,
            batch_size,
            data_write,
        )
        .await
        {
            error!("error in source: {:?}", e);
        }
    });

    let mut reader_pipelines = vec![];
    for i in 0..num_readers {
        let reader_name = format!("reader-{}", i);
        let reader_name_clone = reader_name.clone();
        let (_write, data_read) = persist.open::<String, (), Timestamp, _>(data_id).await?;
        let pipeline = mz_ore::task::spawn(|| &reader_name_clone, async move {
            let as_of = data_read.since().clone();
            if let Err(e) = reader::spawn_reader_pipeline(reader_name, data_read, as_of).await {
                error!("error in reader: {:?}", e);
            }
        });

        reader_pipelines.push(pipeline);
    }

    // Make it explicit that we will never read from this, but only after creating the "real"
    // reader. Otherwise, that real reader couldn't read because the global since would have
    // advanced.
    drop(data_read);

    // After a while, start a second pipeline that reads from the same source and shares the
    // timestamper.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let (bindings_write, bindings_read) = persist.open(bindings_id).await?;
    let consensus = PersistConsensus::new(bindings_write, bindings_read);
    let timestamper =
        ConsensusTimestamper::new(now_fn.clone(), timestamp_interval.clone(), consensus).await;

    let data_write = persist.open_writer(data_id).await?;

    let source2 = source.clone();
    let source_pipeline2 = mz_ore::task::spawn(|| "source-2", async move {
        if let Err(e) = render::spawn_source_pipeline(
            "source-2".to_string(),
            source2,
            timestamper,
            batch_size,
            data_write,
        )
        .await
        {
            error!("error in source: {:?}", e);
        }
    });

    source_pipeline1.await?;
    source_pipeline2.await?;

    for pipeline in reader_pipelines.into_iter() {
        pipeline.await?;
    }

    Ok(())
}

async fn persist_client(args: Args) -> Result<PersistClient, ExternalError> {
    let location = PersistLocation {
        blob_uri: args.blob_uri,
        consensus_uri: args.consensus_uri,
    };
    let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
    let config = PersistConfig::new(SYSTEM_TIME.clone());
    let (blob, consensus) = location.open_locations(&config, &metrics).await?;
    let unreliable = UnreliableHandle::default();
    let should_happen = 1.0 - args.unreliability;
    let should_timeout = args.unreliability;
    unreliable.partially_available(should_happen, should_timeout);
    let blob =
        Arc::new(UnreliableBlob::new(blob, unreliable.clone())) as Arc<dyn Blob + Send + Sync>;
    let consensus = Arc::new(UnreliableConsensus::new(consensus, unreliable))
        as Arc<dyn Consensus + Send + Sync>;
    let cpu_heavy_runtime = Arc::new(CpuHeavyRuntime::new());
    PersistClient::new(config, blob, consensus, metrics, cpu_heavy_runtime).await
}

mod api {
    use std::fmt::Debug;

    use async_trait::async_trait;
    use timely::progress::{Antichain, ChangeBatch};

    #[derive(Debug)]
    pub struct Message<T, P> {
        pub capability: ChangeBatch<T>,
        pub payload: P,
    }

    #[derive(Debug)]
    pub struct Batch<D, T> {
        pub updates: Vec<(D, T)>,

        /// The lowest timestamp(s) of updates in this batch.
        pub lower: Antichain<T>,

        /// The highest timestamp(s) of updates in this batch. This is different from a
        /// _differential upper_, which is exclusive and denotes the frontier at which new updates
        /// would happen. For reclocking puposes (finding/minting a matching timestamp binding) we
        /// are interested in the largest source timestamps (think Kafka partition/offset pairs)
        /// for which we need to find a binding.
        pub inclusive_upper: Antichain<T>,
    }

    /// `TT` is `ToTime`.
    ///
    /// We carry information about the source frontier inline with batches, so that we can know the
    /// frontier in the task that does emission.
    #[derive(Debug)]
    pub struct TimestampedBatch<D, TT> {
        pub updates: Vec<(D, TT)>,

        pub inclusive_upper: Antichain<TT>,
    }

    #[async_trait]
    pub trait Source: Clone {
        type SourceTimestamp: timely::PartialOrder + PartialOrd + Debug;
        type D: Debug;

        /// Reads up to `max_num_records` updates from the source. Returns `None` if no data is
        /// available.
        ///
        /// We could think about returning an empty batch with the same lower/inclusive_upper as
        /// last time. Or we could think about this method just blocking forever, but it doesn't
        /// feel like the correct thing to do. But, idk... ¯\_(ツ)_/¯
        async fn read_batch(
            &mut self,
            max_num_records: usize,
        ) -> Option<Batch<Self::D, Self::SourceTimestamp>>;

        /// Returns the current frontier of this source. All future updates will be at times that
        /// are beyond this frontier.
        async fn frontier(&self) -> Antichain<Self::SourceTimestamp>;
    }

    #[async_trait]
    pub trait Timestamper {
        type FromTimestamp: Debug + timely::PartialOrder;
        type ToTimestamp: Debug + Copy;

        async fn get_bindings(
            &mut self,
            lower: Antichain<Self::FromTimestamp>,
            upper: Antichain<Self::FromTimestamp>,
        ) -> TimestampBindings<Self::FromTimestamp, Self::ToTimestamp>;

        /// All bindings where this `Timestamper` knows that they are durable. It might be that
        /// there are more bindings in other (distributed) instances that we don't know, but I
        /// think it's correct to advance the frontier solely based on our local knowledge.
        async fn all_bindings(&self) -> Vec<(Self::ToTimestamp, Self::FromTimestamp)>;
    }

    /// A set of timestamp bindings that was retrieved from a [`Timestamper`].
    ///
    /// `FT` is `FromTime`, `TT` is `ToTime`.
    #[derive(Debug)]
    pub struct TimestampBindings<FT: timely::PartialOrder + Debug, TT: Debug> {
        // Sorted by from timestamp, so that get_timestamp() can simply iterate from the
        // beginning and return when it finds a binding that covers the from-ts.
        //
        // Due to how the timestamper works, if for two bindings the from timestamp of the first is
        // less-equal than the from timestamp of the second, then the to timestamp is also
        // less-equal.
        bindings: Vec<(FT, TT)>,

        lower: Antichain<FT>,

        /// The highest timestamp(s) for which this contains bindings. This is different from a
        /// _differential upper_, which is exclusive and denotes the frontier at which new updates
        /// would happen. For reclocking puposes (finding/minting a matching timestamp binding) we
        /// are interested in the largest from timestamps (think Kafka partition/offset pairs)
        /// for which we have a binding.
        inclusive_upper: Antichain<FT>,
    }

    impl<FT: timely::PartialOrder + Debug, TT: Copy + Debug> TimestampBindings<FT, TT> {
        pub fn new(
            lower: Antichain<FT>,
            inclusive_upper: Antichain<FT>,
            mut bindings: Vec<(FT, TT)>,
        ) -> Self {
            bindings.sort_by(|a, b| {
                if a.0.less_than(&b.0) {
                    std::cmp::Ordering::Less
                } else if a.0.less_equal(&b.0) {
                    std::cmp::Ordering::Equal
                } else {
                    std::cmp::Ordering::Greater
                }
            });
            Self {
                lower,
                inclusive_upper,
                bindings,
            }
        }

        pub fn get_timestamp(&self, ts: FT) -> TT {
            assert!(
                self.lower.less_equal(&ts),
                "from timestamp {:?} is not beyond lowers: {:?}",
                ts,
                self.lower
            );
            assert!(
                !self.inclusive_upper.less_than(&ts),
                "inclusive upper must not be less than ts"
            );

            // Very naive solution for this!

            let mut result_ts = None;

            for (from_ts, to_ts) in self.bindings.iter() {
                if ts.less_equal(from_ts) {
                    result_ts = Some(to_ts);
                } else {
                    break;
                }
            }

            match result_ts {
                Some(binding) => *binding,
                None => panic!("missing binding for {:?}, bindings: {:?}", ts, self),
            }
        }
    }

    // A trait for doing iterated consensus.
    //
    // The updates are not differential, while they probably should be. As it is now, we can never
    // retract updates.
    #[async_trait]
    pub trait IteratedConsensus<K, V, T> {
        /// Returns all updates with timestamps up to (and including) the given `as_of`.
        ///
        /// Returns an `Err` with the current upper if the given `as_of` is beyond the current
        /// upper.
        async fn snapshot(&mut self, as_of: Antichain<T>)
            -> Result<Vec<((K, V), T)>, Antichain<T>>;

        async fn current_upper(&mut self) -> &Antichain<T>;

        /// Tries to append the given updates to the maintained collection. If the current
        /// upper is not `expected_upper` this will return an `Err` containing the current
        /// upper.
        async fn compare_and_append(
            &mut self,
            updates: Vec<((K, V), T)>,
            expected_upper: Antichain<T>,
            new_upper: Antichain<T>,
        ) -> Result<(), Antichain<T>>;
    }
}

mod types {
    use bytes::BufMut;
    use differential_dataflow::lattice::Lattice;
    use serde::{Deserialize, Serialize};
    use timely::progress::PathSummary;

    use mz_persist_types::{Codec, Codec64};

    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub struct PartitionOffset {
        pub partition: u64,
        pub offset: u64,
    }

    impl PartitionOffset {
        pub fn new(partition: u64, offset: u64) -> Self {
            PartitionOffset { partition, offset }
        }
    }

    impl Codec for PartitionOffset {
        fn codec_name() -> String {
            "partition_offset[u64,u64]".to_owned()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            buf.put_u64_le(self.partition);
            buf.put_u64_le(self.offset);
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            let partition =
                u64::from_le_bytes(<[u8; 8]>::try_from(&buf[0..8]).map_err(|err| err.to_string())?);
            let offset = u64::from_le_bytes(
                <[u8; 8]>::try_from(&buf[8..16]).map_err(|err| err.to_string())?,
            );

            Ok(Self { partition, offset })
        }
    }

    impl timely::PartialOrder for PartitionOffset {
        fn less_equal(&self, other: &Self) -> bool {
            if self.partition != other.partition {
                return false;
            }
            self.offset <= other.offset
        }
    }

    impl PartialOrd for PartitionOffset {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            if self.partition == other.partition {
                return self.offset.partial_cmp(&other.offset);
            }
            self.partition.partial_cmp(&other.partition)
        }
    }

    // NOTE: We only need this because `ChangeBatch` needs `Ord`. We need to return a good order
    // when there is a partial order, otherwise it doesn't matter.
    impl Ord for PartitionOffset {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            if self.partition == other.partition {
                return self.offset.cmp(&other.offset);
            }
            self.partition.cmp(&other.partition)
        }
    }

    #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub struct Timestamp(pub u64);

    impl Timestamp {
        pub fn new(timestamp: u64) -> Self {
            Timestamp(timestamp)
        }
    }

    impl Default for Timestamp {
        fn default() -> Self {
            Timestamp(0)
        }
    }

    impl Codec for Timestamp {
        fn codec_name() -> String {
            "timestamp[u64]".to_owned()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            buf.put_u64_le(self.0);
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            let timestamp =
                u64::from_le_bytes(<[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?);
            Ok(Timestamp(timestamp))
        }
    }

    impl Codec64 for Timestamp {
        fn codec_name() -> String {
            "timestamp[u64]".to_owned()
        }

        fn encode(&self) -> [u8; 8] {
            self.0.encode()
        }

        fn decode(buf: [u8; 8]) -> Self {
            let timestamp = u64::decode(buf);
            Timestamp(timestamp)
        }
    }

    impl timely::PartialOrder for Timestamp {
        fn less_equal(&self, other: &Self) -> bool {
            self.0 <= other.0
        }
    }

    impl PathSummary<Timestamp> for Timestamp {
        fn results_in(&self, src: &Timestamp) -> Option<Timestamp> {
            self.0.checked_add(src.0).map(Timestamp)
        }

        fn followed_by(&self, other: &Self) -> Option<Self> {
            self.0.checked_add(other.0).map(Timestamp)
        }
    }

    impl timely::progress::Timestamp for Timestamp {
        type Summary = Timestamp;

        fn minimum() -> Self {
            Timestamp(0)
        }
    }

    impl Lattice for Timestamp {
        fn join(&self, other: &Self) -> Self {
            std::cmp::max(*self, *other)
        }

        fn meet(&self, other: &Self) -> Self {
            std::cmp::min(*self, *other)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn partition_offset_codec_roundtrip() -> Result<(), String> {
            let original = PartitionOffset::new(42, 17);
            let mut encoded = Vec::new();
            original.encode(&mut encoded);
            let decoded = PartitionOffset::decode(&encoded)?;

            assert_eq!(decoded, original);

            Ok(())
        }
    }
}

mod impls {
    use std::collections::hash_map::Entry;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::time::Duration;

    use async_trait::async_trait;
    use differential_dataflow::lattice::Lattice;
    use timely::progress::Antichain;
    use timely::progress::Timestamp as TimelyTimestamp;
    use timely::PartialOrder;
    use tracing::trace;

    use mz_ore::cast::CastFrom;
    use mz_ore::now::NowFn;
    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::write::WriteHandle;
    use mz_persist_client::Upper;
    use mz_persist_types::{Codec, Codec64};

    use super::api::{Batch, IteratedConsensus, Source, TimestampBindings, Timestamper};
    use super::retry_mut;
    use super::types::{PartitionOffset, Timestamp};

    #[derive(Clone)]
    pub struct SequentialSource {
        offsets: HashMap<u64, u64>,
        num_partitions: u64,
        current_partition: u64,
        wait_time: Duration,
    }

    impl SequentialSource {
        /// Creates a new [`SequentialSource`] that waits for the given `wait_time` before each
        /// batch is emitted.
        pub fn new(num_partitions: u64, wait_time: Duration) -> Self
        where
            Self: Sized,
        {
            assert!(num_partitions > 0, "number of partitions must be > 0");

            Self {
                offsets: HashMap::new(),
                num_partitions,
                current_partition: 0,
                wait_time,
            }
        }
    }

    #[async_trait]
    impl Source for SequentialSource {
        type SourceTimestamp = PartitionOffset;
        type D = String;

        async fn read_batch(
            &mut self,
            max_num_records: usize,
        ) -> Option<Batch<Self::D, Self::SourceTimestamp>> {
            let mut updates = Vec::new();

            let current_offset = self.offsets.entry(self.current_partition).or_default();
            let lower = *current_offset;
            let upper = lower + u64::cast_from(max_num_records);

            for i in lower..upper {
                updates.push((
                    format!("{}-{}", self.current_partition, i),
                    PartitionOffset::new(self.current_partition, i),
                ));
            }

            *current_offset = upper;

            let batch = Batch {
                updates,
                lower: Antichain::from_elem(PartitionOffset::new(self.current_partition, lower)),
                inclusive_upper: Antichain::from_elem(PartitionOffset::new(
                    self.current_partition,
                    upper - 1,
                )),
            };

            self.current_partition = self.current_partition + 1;
            if self.current_partition > self.num_partitions - 1 {
                self.current_partition = 0;
            }

            if !self.wait_time.is_zero() {
                // It's a slow source...
                tokio::time::sleep(self.wait_time).await;
            }

            Some(batch)
        }

        async fn frontier(&self) -> Antichain<PartitionOffset> {
            let mut result = Antichain::new();
            for p in 0..self.num_partitions {
                let offset = self.offsets.get(&p).cloned().unwrap_or_default();
                result.insert(PartitionOffset::new(p, offset));
            }
            result
        }
    }

    /// A [`Timestamper`] that maintains timestamps using the given [`IteratedConsensus`].
    ///
    /// The `IteratorConsensus` is used to mint timestamp bindings, and we have a bunch of cached
    /// state. If there is only one process minting timestamps, the cached state always reflects
    /// the global state. If there are concurrent minters, we sometimes have to update our state
    /// from global state. We would notice this when a CAS operation fails.
    pub struct ConsensusTimestamper<ST, C>
    where
        C: IteratedConsensus<ST, Timestamp, u64>,
    {
        now_fn: NowFn,
        timestamp_interval: Duration,
        cache: HashMap<ST, (ST, Timestamp)>,
        current_ts: u64,
        consensus: C,
        // Current version (as far as we know) of the timestamp bindings in consensus. This is a
        // sequential number.
        current_version: u64,
    }

    impl<ST, C> ConsensusTimestamper<ST, C>
    where
        C: IteratedConsensus<ST, Timestamp, u64>,
        ST: Eq + Hash + Clone,
    {
        #[allow(unused)]
        pub async fn new(now_fn: NowFn, timestamp_interval: Duration, mut consensus: C) -> Self
        where
            Self: Sized,
        {
            let mut cache = HashMap::new();

            let current_ts = consensus.current_upper().await;
            // We know this time is one dimensional.
            let current_ts = current_ts
                .iter()
                .next()
                .cloned()
                .unwrap_or_else(TimelyTimestamp::minimum);

            let bindings = consensus
                .snapshot(Antichain::from_elem(current_ts.saturating_sub(1)))
                .await
                .expect("snapshot");

            for ((source_ts, ts), _ts) in bindings {
                cache.insert(source_ts.clone(), (source_ts, ts));
            }

            Self {
                now_fn,
                timestamp_interval,
                cache,
                current_ts,
                consensus,
                current_version: 0,
            }
        }
    }

    #[async_trait]
    impl<ST, C> Timestamper for ConsensusTimestamper<ST, C>
    where
        ST: timely::PartialOrder + Sync + Send + Debug + Copy + Hash + Ord,
        C: IteratedConsensus<ST, Timestamp, u64> + Sync + Send,
    {
        type FromTimestamp = ST;
        type ToTimestamp = Timestamp;

        async fn get_bindings(
            &mut self,
            lower: Antichain<Self::FromTimestamp>,
            inclusive_upper: Antichain<Self::FromTimestamp>,
        ) -> TimestampBindings<Self::FromTimestamp, Self::ToTimestamp> {
            let mut result_bindings = Vec::new();

            for upper_element in inclusive_upper.iter() {
                let cached = self.cache.entry(*upper_element);

                match cached {
                    Entry::Occupied(cached_binding) => {
                        trace!("Cached binding: {:?}", cached_binding.get());
                        result_bindings.push(*cached_binding.get());
                        continue;
                    }
                    Entry::Vacant(e) => {
                        // Well, this is awkward: there's no binding yet so we have to try and mint
                        // one. It could be that this fails, and we notice that someone else
                        // already minted a binding. In that case, update our view of the consensus
                        // data and try again.

                        let binding = 'outer: loop {
                            // TODO(aljoscha): Make sure that ToTime never regresses by storing the
                            // latest mapped timestamp in consensus as well. Or at least derive it
                            // from the currently existing bindings.
                            let now = (self.now_fn)();
                            let now_clamped = now
                                - (now
                                    % u64::try_from(self.timestamp_interval.as_millis()).unwrap());
                            if now_clamped != self.current_ts {
                                self.current_ts = now_clamped;
                            }

                            let new_binding = (*upper_element, Timestamp::new(self.current_ts));
                            let new_bindings = vec![(new_binding, self.current_version)];
                            let expected_frontier = Antichain::from_elem(self.current_version);
                            let new_frontier = Antichain::from_elem(self.current_version + 1);

                            let append_result = self
                                .consensus
                                .compare_and_append(
                                    new_bindings.clone(),
                                    expected_frontier.clone(),
                                    new_frontier.clone(),
                                )
                                .await;

                            let consensus_version = match append_result {
                                Ok(_) => {
                                    // We successfully advanced to a new version.
                                    self.current_version = self.current_version + 1;
                                    break 'outer new_binding;
                                }
                                Err(consensus_version) => {
                                    // We failed to mint a binding. This could mean that someone
                                    // else already minted a binding that covers our requirements.
                                    // Or it could just mean that state advanced without a binding
                                    // that we can re-use. In any case, we learned what the current
                                    // version of state is and can try minting again if we don't
                                    // get a binding that works for us.

                                    // TODO: Yeah yeah, very hacky...
                                    let consensus_version =
                                        consensus_version.iter().next().cloned().unwrap();

                                    trace!("Could not mint binding at version {}, version is already {}", self.current_version, consensus_version);

                                    consensus_version
                                }
                            };

                            // Update what we think the current version is. So that we match
                            // consensus and our next try might succeed.
                            self.current_version = consensus_version;

                            // NOTE: It's okay if this is somewhat expensive to do: we only end up
                            // in this case if another source instance concurrently minted new
                            // bindings, which probably means that it will also write data and do
                            // that faster than we can. We're only the backup... ¯\_(ツ)_/¯
                            let bindings = self
                                .consensus
                                .snapshot(Antichain::from_elem(self.current_version - 1))
                                .await;

                            let mut bindings =
                                bindings.expect("we just learned that this is the current version");

                            // Sort by `FromTime`, to make sure that the loop below gets the
                            // "earliest" binding that covers the needed `FromTime`.
                            bindings.sort_by_key(|((from_ts, _to_ts), _seq_no)| *from_ts);

                            // Break out of trying if we now have a binding.
                            for ((from_ts, to_ts), _version) in bindings {
                                if upper_element.less_equal(&from_ts) {
                                    break 'outer (from_ts, to_ts);
                                }
                            }
                        };

                        result_bindings.push(binding);
                        e.insert(binding);
                    }
                }
            }

            TimestampBindings::new(lower, inclusive_upper, result_bindings)
        }

        async fn all_bindings(&self) -> Vec<(Self::ToTimestamp, Self::FromTimestamp)> {
            // The cache maps from `FromTime` to `ToTime`, but we want the reverse mapping here.
            self.cache
                .iter()
                .map(|(_from, (from, to))| (*to, *from))
                .collect()
        }
    }

    /// An [`IteratedConsensus`] that uses persist to mint timestamp bindings.
    pub struct PersistConsensus<K, V, T>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Lattice + timely::progress::Timestamp + Codec64 + Debug,
    {
        /// Our state consists of the current upper (aka. version) and the updates.
        read: ReadHandle<K, V, T, i64>,
        write: WriteHandle<K, V, T, i64>,
    }

    impl<K, V, T> PersistConsensus<K, V, T>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Lattice + timely::progress::Timestamp + Codec64 + Clone + Debug,
    {
        #[allow(unused)]
        pub fn new(write: WriteHandle<K, V, T, i64>, read: ReadHandle<K, V, T, i64>) -> Self
        where
            Self: Sized,
        {
            Self { read, write }
        }
    }

    #[async_trait]
    impl<K, V, T> IteratedConsensus<K, V, T> for PersistConsensus<K, V, T>
    where
        K: Codec + Send + Sync + Copy + Debug,
        V: Codec + Send + Sync + Copy + Debug,
        T: Lattice
            + timely::progress::Timestamp
            + timely::PartialOrder
            + Codec64
            + Send
            + Sync
            + Copy,
    {
        async fn current_upper(&mut self) -> &Antichain<T> {
            self.write.fetch_recent_upper().await
        }

        async fn snapshot(
            &mut self,
            as_of: Antichain<T>,
        ) -> Result<Vec<((K, V), T)>, Antichain<T>> {
            if PartialOrder::less_equal(&as_of, &Antichain::from_elem(T::minimum())) {
                // Early exit, because snapshot errors if we try and do this.
                return Ok(vec![]);
            }

            let updates = self
                .read
                .snapshot_and_fetch(as_of.clone())
                .await
                .expect("wrong as_of");

            let result = updates
                .into_iter()
                .map(|((k, v), ts, diff)| {
                    // TODO(aljoscha): We'll have to figure out how to store bindings for real, and
                    // how to retract/compact them.
                    assert_eq!(diff, 1);
                    ((k.unwrap(), v.unwrap()), ts)
                })
                .collect();

            Ok(result)
        }

        async fn compare_and_append(
            &mut self,
            updates: Vec<((K, V), T)>,
            expected_upper: Antichain<T>,
            new_upper: Antichain<T>,
        ) -> Result<(), Antichain<T>> {
            let mut input = (&mut self.write, &updates);

            let result = retry_mut(
                "consensus::compare_and_append",
                &mut input,
                move |(write, updates)| {
                    let updates = updates.iter().map(|((k, v), ts)| ((k, v), ts, 1));
                    let fut = write.compare_and_append(
                        updates,
                        expected_upper.clone(),
                        new_upper.clone(),
                    );
                    Box::pin(fut)
                },
            )
            .await
            .expect("invalid usage");

            match result {
                Ok(x) => Ok(x),
                Err(Upper(upper)) => Err(upper),
            }
        }
    }
}

mod render {
    use std::error::Error;
    use std::fmt::{Debug, Display};
    use std::sync::Arc;
    use std::time::Duration;

    use differential_dataflow::lattice::Lattice;
    use timely::progress::frontier::MutableAntichain;
    use timely::progress::{Antichain, ChangeBatch};
    use timely::PartialOrder;
    use tokio::sync::{mpsc, Mutex};
    use tracing::{error, trace};

    use mz_persist_client::write::WriteHandle;
    use mz_persist_client::Upper;
    use mz_persist_types::{Codec, Codec64};

    use super::api::{Batch, Message, Source, TimestampedBatch, Timestamper};

    /// Spawns tokio tasks for the different steps of a source pipeline. We read from the source,
    /// decode, assign timestamps to updates, and emit them.
    ///
    /// `ST` is the type of timestamp that batches from the source have.
    pub async fn spawn_source_pipeline<S, TS, ST>(
        name: String,
        source: S,
        timestamper: TS,
        batch_size: usize,
        write: WriteHandle<String, (), TS::ToTimestamp, i64>,
    ) -> Result<(), Box<dyn Error>>
    where
        S: Source<SourceTimestamp = ST> + Send + Sync + 'static,
        TS: Timestamper<FromTimestamp = ST> + Send + Sync + 'static,
        ST: timely::PartialOrder + PartialOrd + Copy + Debug + Send + Sync + 'static,
        S::SourceTimestamp: Send + Ord,
        S::D: Display + Send + Sync + Codec,
        TS::FromTimestamp: Send,
        TS::ToTimestamp: timely::progress::Timestamp
            + Lattice
            + timely::PartialOrder
            + Codec64
            + Ord
            + Send
            + Sync
            + 'static,
    {
        // Right now, everything is a separate task, including the prefetching. We'd have to see
        // how to split these in a real implementation.

        // Yes, it seems we're implementing a poor-mans version of progress tracking here.
        let source_frontier = Arc::new(Mutex::new(MutableAntichain::new()));

        let (raw_batches_tx, raw_batches_rx) = mpsc::channel(2);

        let source_frontier1 = Arc::clone(&source_frontier);
        let source_task = mz_ore::task::spawn(|| "source", async move {
            read_source(source, batch_size, source_frontier1, raw_batches_tx).await;
        });

        let (raw_batches_tx2, raw_batches_rx2) = mpsc::channel(2);

        let timestamper = Arc::new(Mutex::new(timestamper));
        let timestamper1 = Arc::clone(&timestamper);
        let prefetcher_task = mz_ore::task::spawn(|| "source-timestamp-prefetcher", async move {
            prefetch_timestamp_bindings(timestamper1, raw_batches_rx, raw_batches_tx2).await;
        });

        let (decoded_batches_tx, decoded_batches_rx) = mpsc::channel(2);

        let decoder_task = mz_ore::task::spawn(|| "source-decoder", async move {
            decode(raw_batches_rx2, decoded_batches_tx).await;
        });

        let (reclocked_batches_tx, reclocked_batches_rx) = mpsc::channel(2);

        let timestamper1 = Arc::clone(&timestamper);
        let timestamper_task = mz_ore::task::spawn(|| "source-reclock", async move {
            reclock(timestamper1, decoded_batches_rx, reclocked_batches_tx).await;
        });

        let consumer_task = mz_ore::task::spawn(|| "source-emit", async move {
            emit(
                name,
                timestamper,
                source_frontier,
                reclocked_batches_rx,
                write,
            )
            .await;
        });

        let _ = tokio::join!(
            source_task,
            prefetcher_task,
            decoder_task,
            timestamper_task,
            consumer_task
        );

        Ok(())
    }

    /// Reads batches from the given `source` and forwards them to `tx`.
    async fn read_source<S, ST>(
        mut source: S,
        batch_size: usize,
        source_frontier: Arc<Mutex<MutableAntichain<S::SourceTimestamp>>>,
        tx: mpsc::Sender<Message<S::SourceTimestamp, Batch<S::D, S::SourceTimestamp>>>,
    ) where
        S: Source<SourceTimestamp = ST>,
        ST: timely::PartialOrder + PartialOrd + Debug + Copy + Ord + Send + Sync,
    {
        let current_frontier = source.frontier().await;
        let current_frontier = current_frontier.into_iter().map(|ts| (ts, 1));
        source_frontier.lock().await.update_iter(current_frontier);

        loop {
            let current_frontier = source.frontier().await;
            let batch = source.read_batch(batch_size).await;
            let new_frontier = source.frontier().await;

            let current_frontier = current_frontier.into_iter().map(|ts| (ts, 1));
            let new_frontier = new_frontier.into_iter().map(|ts| (ts, 1));

            source_frontier.lock().await.update_iter(new_frontier);

            if let Some(batch) = batch {
                // If we have a batch, we send the update in capabilities along with the batch, so
                // that we can apply the changes when the batch makes it to the end of the
                // pipeline.
                let mut capability = ChangeBatch::new();
                capability.extend(current_frontier);

                let message = Message {
                    capability,
                    payload: batch,
                };

                if let Err(_) = tx.send(message).await {
                    error!("receiver dropped");
                    return;
                }
            } else {
                // If we don't have a batch, update the frontier right away.
                let subtraction = current_frontier.map(|(ts, diff)| (ts, -diff));
                source_frontier.lock().await.update_iter(subtraction);

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// Notifies the [`Timestamper`] of newly available source uppers, in an effort to parallelize
    /// the minting and retrieval of new timestamp bindings and decoding of updates.
    async fn prefetch_timestamp_bindings<D: Debug, TS, ST, TT>(
        timestamper: Arc<Mutex<TS>>,
        mut rx: mpsc::Receiver<Message<ST, Batch<D, ST>>>,
        tx: mpsc::Sender<Message<ST, Batch<D, ST>>>,
    ) where
        TS: Timestamper<FromTimestamp = ST, ToTimestamp = TT> + Send + Sync + 'static,
        ST: timely::PartialOrder + PartialOrd + Debug + Send + Sync + Copy + 'static,
        TT: Debug + Copy,
        D: Send,
    {
        while let Some(message) = rx.recv().await {
            let batch = &message.payload;
            let timestamper = Arc::clone(&timestamper);
            let upper = batch.inclusive_upper.clone();
            let lower = batch.lower.clone();
            mz_ore::task::spawn(|| "inner-prefetch", async move {
                let _ = timestamper.lock().await.get_bindings(lower, upper).await;
            });

            if let Err(_) = tx.send(message).await {
                error!("receiver dropped");
                return;
            }
        }
    }

    /// A mock "decode" operation that uses [`Display`] to turn incoming updates into strings and
    /// appends "-decoded".
    async fn decode<D: Display, ST: Debug, T: Debug>(
        mut rx: mpsc::Receiver<Message<ST, Batch<D, T>>>,
        tx: mpsc::Sender<Message<ST, Batch<String, T>>>,
    ) {
        while let Some(message) = rx.recv().await {
            let batch = message.payload;

            let mut decoded_updates = Vec::new();

            for (update, ts) in batch.updates {
                decoded_updates.push((format!("{}-decoded", update), ts));
            }

            let decoded_batch = Batch {
                updates: decoded_updates,
                lower: batch.lower,
                inclusive_upper: batch.inclusive_upper,
            };

            let message = Message {
                capability: message.capability,
                payload: decoded_batch,
            };
            if let Err(_) = tx.send(message).await {
                error!("receiver dropped");
                return;
            }
        }
    }

    /// Changes the timestamp of updates from some "from timestamp" to a "to timestamp". In our
    /// case, "from timestamps" are source timestamps and "to timestamps" are the timestamps that
    /// should be used to drive around timely.
    async fn reclock<D, TS, FT, TT>(
        timestamper: Arc<Mutex<TS>>,
        mut rx: mpsc::Receiver<Message<FT, Batch<D, FT>>>,
        tx: mpsc::Sender<Message<FT, TimestampedBatch<D, TT>>>,
    ) where
        D: Debug,
        TS: Timestamper<FromTimestamp = FT, ToTimestamp = TT>,
        FT: timely::PartialOrder + PartialOrd + Debug + Copy,
        TT: timely::PartialOrder + Debug + Copy,
    {
        while let Some(message) = rx.recv().await {
            let batch = message.payload;

            let bindings = timestamper
                .lock()
                .await
                .get_bindings(batch.lower.clone(), batch.inclusive_upper.clone())
                .await;
            let mut timestamped_updates = Vec::new();

            // TODO(aljoscha): This seems wasteful.
            let mut inclusive_upper = Antichain::new();
            for (update, ts) in batch.updates {
                let ts = bindings.get_timestamp(ts);
                inclusive_upper.insert(ts);
                timestamped_updates.push((update, ts));
            }

            let timestamped_batch = TimestampedBatch {
                updates: timestamped_updates,
                inclusive_upper,
            };

            let message = Message {
                capability: message.capability,
                payload: timestamped_batch,
            };
            if let Err(_) = tx.send(message).await {
                error!("receiver dropped");
                return;
            }
        }
    }

    /// The egress point of the source pipeline. This could either write into persistence or feed
    /// straight into a timely dataflow.
    async fn emit<D, TS, FT, TT>(
        name: String,
        timestamper: Arc<Mutex<TS>>,
        source_frontier: Arc<Mutex<MutableAntichain<FT>>>,
        mut rx: mpsc::Receiver<Message<FT, TimestampedBatch<D, TT>>>,
        mut write: WriteHandle<D, (), TS::ToTimestamp, i64>,
    ) where
        D: Codec + Debug + Send + Sync,
        TS: Timestamper<FromTimestamp = FT, ToTimestamp = TT>,
        FT: timely::PartialOrder + PartialOrd + Ord + Send + Sync + Debug + Copy,
        TT: timely::progress::Timestamp
            + Lattice
            + Codec64
            + timely::PartialOrder
            + Ord
            + Debug
            + Copy,
    {
        // Figure out what everyone else has written so far, and don't write updates that are not
        // beyond that.
        let mut initial_upper = write.upper().clone();

        // We need to stash batches whose upper is beyond the frontier, so that we can "atomically"
        // write all batches and downgrade to a new upper when the frontier advances.
        let mut stashed_batches = Vec::new();

        while let Some(mut message) = rx.recv().await {
            let batch = message.payload;
            println!("instance {}: emit got = {:?}", name, batch);

            let capability_subtraction = message.capability.drain().map(|(ts, diff)| (ts, -diff));
            source_frontier
                .lock()
                .await
                .update_iter(capability_subtraction);

            let source_frontier = source_frontier.lock().await.frontier().to_owned();

            println!("instance {}: source_frontier: {:?}", name, source_frontier,);

            let all_bindings = timestamper.lock().await.all_bindings().await;

            let open_timestamps = all_bindings
                .iter()
                .filter(|(_to, from)| source_frontier.less_than(&from))
                .map(|(to, _from)| *to);

            let mut target_upper = Antichain::new();
            target_upper.extend(open_timestamps);

            let largest_target_ts = all_bindings
                .iter()
                .map(|(to, _from)| *to)
                .max()
                .expect("there must be some bindings");

            // It's **very** important that we also insert the largest seen timestamp because the
            // open timestamps might be empty. And then we would be downgrading to the empty
            // frontier, closes the shard permanently for writes.
            target_upper.insert(largest_target_ts);

            trace!(
                "instance {}: all bindings: {:?}, source_frontier: {:?}, target_frontier: {:?}",
                name,
                all_bindings,
                source_frontier,
                target_upper
            );

            println!("instance {}: frontier: {:?}", name, target_upper);

            stashed_batches.push(batch);

            let stashed_batches = &mut stashed_batches;
            let target_upper = &mut target_upper;
            let initial_upper = &mut initial_upper;

            if PartialOrder::less_than(write.upper(), target_upper) {
                println!("instance {}: initial_upper: {:?}", name, initial_upper);
                let target_upper1 = target_upper.clone();
                let updates = stashed_batches
                    .iter()
                    .filter(move |batch| {
                        !timely::PartialOrder::less_equal(&target_upper1, &batch.inclusive_upper)
                    })
                    .flat_map(|batch| batch.updates.iter());

                // Filter out updates that have already been written by someone.
                let initial_upper1 = initial_upper.clone();
                let updates = updates
                    .filter(move |(_update, ts)| initial_upper1.less_equal(ts))
                    .map(|(update, ts)| ((update, ()), ts, 1));

                let (size_hint, _) = updates.size_hint();
                let mut builder = write.builder(size_hint, write.upper().clone());
                for ((key, val), ts, diff) in updates {
                    builder
                        .add(key, &val, ts, &diff)
                        .await
                        .expect("invalid usage");
                }
                let batch = builder
                    .finish(target_upper.clone())
                    .await
                    .expect("invalid usage");

                let result = write
                    .append_batch(batch, write.upper().clone(), target_upper.clone())
                    .await;

                match result {
                    Ok(Ok(_)) => {
                        // All good!
                    }
                    Ok(Err(Upper(current_upper))) => {
                        // We messed up, and somehow constructed a batch that would
                        // have left a hole in the shard.
                        panic!(
                            "emit.append: unrecoverable usage error: {:?}",
                            current_upper
                        );
                    }
                    Err(e) => {
                        panic!("emit.append: unrecoverable usage error: {:?}", e);
                    }
                }
            }

            stashed_batches.retain(|batch| {
                timely::PartialOrder::less_equal(target_upper, &batch.inclusive_upper)
            });
        }
    }
}

mod reader {
    use std::error::Error;
    use std::fmt::Debug;

    use differential_dataflow::lattice::Lattice;
    use timely::progress::Antichain;
    use timely::PartialOrder;

    use mz_persist_client::read::{ListenEvent, ReadHandle};
    use mz_persist_types::{Codec, Codec64};

    /// Spawns a persist consumer that reads from the given `ReadHandle`.
    pub async fn spawn_reader_pipeline<K, V, T>(
        name: String,
        mut read: ReadHandle<K, V, T, i64>,
        as_of: Antichain<T>,
    ) -> Result<(), Box<dyn Error>>
    where
        K: Codec + Debug + Send + Sync,
        V: Codec + Debug + Send + Sync,
        T: timely::progress::Timestamp + Lattice + Codec64 + Send + Sync,
    {
        let reader_task = mz_ore::task::spawn(|| "reader", async move {
            // Cannot snapshot at `[0]` if that's not ready.
            if !PartialOrder::less_equal(&as_of, &Antichain::from_elem(T::minimum())) {
                for next in read
                    .snapshot_and_fetch(as_of.clone())
                    .await
                    .expect("invalid as_of")
                {
                    println!("instance {}: got from snapshot: {:?}", name, next);
                }
            }
            let mut listen = read
                .clone()
                .await
                .listen(as_of.clone())
                .await
                .expect("invalid as_of");

            'outer: loop {
                let next = listen.next().await;

                for event in next {
                    match event {
                        ListenEvent::Progress(p) => {
                            println!("instance {}: got progress from listen: {:?}", name, p);

                            if p.is_empty() {
                                break 'outer;
                            }

                            read.downgrade_since(&p).await;
                        }
                        ListenEvent::Updates(updates) => {
                            println!("instance {}: got updates from listen: {:?}", name, updates);
                        }
                    }
                }
            }
        });

        let _ = tokio::join!(reader_task,);

        Ok(())
    }
}
