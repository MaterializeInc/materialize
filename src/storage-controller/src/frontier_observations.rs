// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cooperative observations of physical storage progress. These rows are not
//! read holds. Catalog views must filter them by current object and shard.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;
use std::time::Duration;

use mz_ore::now::NowFn;
use mz_persist_client::read::{ListenEvent, ReadHandle, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadataCertifier;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

const CERTIFICATION_BATCH: usize = 64;

type Key = (String, String);
type Samples = BTreeMap<Key, Observation>;
type Writer = WriteHandle<SourceData, (), Timestamp, StorageDiff>;
type Reader = ReadHandle<SourceData, (), Timestamp, StorageDiff>;
type Subscription = Subscribe<SourceData, (), Timestamp, StorageDiff>;
type Update = ((SourceData, ()), Timestamp, StorageDiff);

/// Unknown is bottom, finite frontiers are ordered by timestamp, and empty is
/// top. Read and write knowledge are independent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
enum Frontier {
    #[default]
    Unknown,
    At(Timestamp),
    Empty,
}

impl Frontier {
    fn observed(frontier: Antichain<Timestamp>) -> Self {
        frontier.as_option().map_or(Self::Empty, |t| Self::At(*t))
    }

    fn decode(known: Datum<'_>, frontier: Datum<'_>) -> Self {
        if !known.unwrap_bool() {
            Self::Unknown
        } else if frontier.is_null() {
            Self::Empty
        } else {
            Self::At(frontier.unwrap_mz_timestamp())
        }
    }

    fn datum(self) -> Datum<'static> {
        match self {
            Self::At(t) => Datum::MzTimestamp(t),
            Self::Unknown | Self::Empty => Datum::Null,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
struct Observation {
    read: Frontier,
    write: Frontier,
}

impl Observation {
    fn join(self, other: Self) -> Self {
        Self {
            read: self.read.max(other.read),
            write: self.write.max(other.write),
        }
    }

    fn row(self, key: &Key) -> Row {
        Row::pack([
            Datum::String(&key.0),
            Datum::String(&key.1),
            Datum::from(self.read != Frontier::Unknown),
            self.read.datum(),
            Datum::from(self.write != Frontier::Unknown),
            self.write.datum(),
        ])
    }

    fn decode(row: &Row) -> (Key, Self) {
        let d = row.unpack();
        assert_eq!(d.len(), 6);
        (
            (d[0].unwrap_str().to_owned(), d[1].unwrap_str().to_owned()),
            Self {
                read: Frontier::decode(d[2], d[3]),
                write: Frontier::decode(d[4], d[5]),
            },
        )
    }
}

enum Command {
    Upsert(GlobalId, CollectionMetadata),
    Remove(GlobalId),
}

/// Samples on the caller's existing maintain cadence. Dropping this owner stops
/// the task, after any submitted conditional write obtains a definitive result.
#[derive(Debug)]
pub(crate) struct StorageFrontierObservations {
    commands: mpsc::UnboundedSender<Command>,
    sample: watch::Sender<()>,
    _shutdown: oneshot::Sender<()>,
}

impl StorageFrontierObservations {
    /// `persist` must address both the output binding and registered collections.
    pub(crate) fn new(
        persist: PersistClient,
        binding: (GlobalId, ShardId, RelationDesc),
        now: NowFn,
        certifier: Arc<dyn CollectionMetadataCertifier>,
    ) -> Self {
        let (commands, rx) = mpsc::unbounded_channel();
        let (sample, ticks) = watch::channel(());
        let (shutdown, shutdown_rx) = oneshot::channel();
        // The shutdown channel owns task lifetime. Aborting the task would also
        // cancel an in-flight conditional write with an indeterminate outcome.
        drop(mz_ore::task::spawn(
            || "storage-frontier-observations",
            run(persist, binding, now, certifier, rx, ticks, shutdown_rx),
        ));
        Self {
            commands,
            sample,
            _shutdown: shutdown,
        }
    }

    pub(crate) fn upsert(&self, id: GlobalId, metadata: CollectionMetadata) {
        let _ = self.commands.send(Command::Upsert(id, metadata));
    }

    /// Stops future sampling without retracting any shared observation.
    pub(crate) fn remove(&self, id: GlobalId) {
        let _ = self.commands.send(Command::Remove(id));
    }

    /// Requests a sample. Repeated notifications coalesce while work is pending.
    pub(crate) fn sample(&self) {
        self.sample.send_replace(());
    }
}

#[derive(Default)]
struct Inventory {
    entries: BTreeMap<GlobalId, (CollectionMetadata, Option<Observation>)>,
    pending: Samples,
}

impl Inventory {
    fn command(&mut self, command: Command) {
        let (id, replacement) = match command {
            Command::Upsert(id, metadata) => (id, Some(metadata)),
            Command::Remove(id) => (id, None),
        };
        if let Some((old, _)) = self.entries.get(&id) {
            if replacement.as_ref() == Some(old) {
                return;
            }
            self.pending
                .remove(&(id.to_string(), old.data_shard.to_string()));
        }
        self.entries.remove(&id);
        if let Some(metadata) = replacement {
            self.entries.insert(id, (metadata, None));
        }
    }

    fn retire(&mut self, keys: BTreeSet<Key>) {
        for key in keys {
            self.pending.remove(&key);
            let id = key.0.parse().expect("native observation global id");
            if self
                .entries
                .get(&id)
                .is_some_and(|(metadata, _)| metadata.data_shard.to_string() == key.1)
            {
                self.entries.remove(&id);
            }
        }
    }

    async fn sample(&mut self, persist: &PersistClient) {
        for (id, (metadata, previous)) in &mut self.entries {
            let sampled = sample(persist, metadata).await;
            let joined = previous.unwrap_or_default().join(sampled);
            if *previous != Some(joined) {
                self.pending
                    .insert((id.to_string(), metadata.data_shard.to_string()), joined);
                *previous = Some(joined);
            }
            tokio::task::yield_now().await;
        }
    }
}

async fn sample(persist: &PersistClient, metadata: &CollectionMetadata) -> Observation {
    let read = persist
        .recent_since::<SourceData, (), Timestamp, StorageDiff>(metadata.data_shard, diagnostics())
        .await;
    // TxnsCodecRow uses the same key/value codecs as data shards. The WAL
    // upper, not the possibly unapplied data upper, bounds transactional writes.
    let write = persist
        .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
            metadata.txns_shard.unwrap_or(metadata.data_shard),
            diagnostics(),
        )
        .await;
    if let Err(error) = &read {
        tracing::warn!(?error, shard = %metadata.data_shard, "sampling storage since");
    }
    if let Err(error) = &write {
        tracing::warn!(?error, shard = %metadata.data_shard, "sampling storage upper");
    }
    Observation {
        read: read.map_or(Frontier::Unknown, Frontier::observed),
        write: write.map_or(Frontier::Unknown, Frontier::observed),
    }
}

fn diagnostics() -> Diagnostics {
    Diagnostics::from_purpose("storage frontier observations")
}

struct Output {
    writer: Writer,
    reader: Option<Reader>,
    subscription: Option<Subscription>,
    // Retractions and insertions may arrive in different fetched parts. Only
    // the completed prefix is eligible for certification and reconciliation.
    observed: BTreeMap<Key, BTreeMap<Observation, StorageDiff>>,
    observed_upper: Timestamp,
    certifier: Option<Arc<dyn CollectionMetadataCertifier>>,
    dirty: BTreeSet<Key>,
    revisit_after: Option<Key>,
    // Drained into inventory after preparation, never retained as tombstones.
    retired: BTreeSet<Key>,
}

#[derive(Debug)]
enum PrepareError {
    Closed,
    Certification(anyhow::Error),
}

struct Attempt {
    updates: Vec<Update>,
    lower: Timestamp,
    upper: Timestamp,
}

impl Output {
    async fn open(persist: &PersistClient, binding: (GlobalId, ShardId, RelationDesc)) -> Self {
        let (id, shard, desc) = binding;
        let (writer, reader) = persist
            .open(
                shard,
                Arc::new(desc),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    ..diagnostics()
                },
                false,
            )
            .await
            .expect("frontier observation schema and codecs");
        Self {
            writer,
            reader: Some(reader),
            subscription: None,
            observed: BTreeMap::new(),
            observed_upper: Timestamp::MIN,
            certifier: None,
            dirty: BTreeSet::new(),
            revisit_after: None,
            retired: BTreeSet::new(),
        }
    }

    /// Revisit a bounded ordered slice, including rows unchanged since startup.
    fn revisit(&mut self) {
        let start = self.revisit_after.as_ref().map_or(Unbounded, Excluded);
        let keys: Vec<_> = self
            .observed
            .range((start, Unbounded))
            .take(CERTIFICATION_BATCH)
            .map(|(key, _)| key.clone())
            .collect();
        self.revisit_after = if keys.len() == CERTIFICATION_BATCH {
            keys.last().cloned()
        } else {
            None
        };
        self.dirty.extend(keys);
    }

    /// Retains snapshot plus listen across conflicts. Never cancel this future
    /// for ordinary commands: fetching leased parts is not cancellation safe.
    /// Certification failures retain work and must be retried, not close output.
    async fn prepare(
        &mut self,
        samples: &Samples,
        now: &NowFn,
    ) -> Result<Option<Attempt>, PrepareError> {
        let target = self
            .writer
            .fetch_recent_upper()
            .await
            .as_option()
            .copied()
            .ok_or(PrepareError::Closed)?;
        if let Some(reader) = &self.reader {
            let since = reader
                .since()
                .as_option()
                .copied()
                .ok_or(PrepareError::Closed)?;
            if target <= since {
                // A snapshot needs a complete timestamp, including on a new
                // shard or when compaction has caught up with the upper.
                return Ok(Some(Attempt {
                    updates: Vec::new(),
                    lower: target,
                    upper: Timestamp::from(now())
                        .max(since.try_step_forward().ok_or(PrepareError::Closed)?),
                }));
            }
            self.subscription = Some(
                self.reader
                    .take()
                    .expect("reader present")
                    .subscribe(Antichain::from_elem(
                        target.step_back().expect("positive upper"),
                    ))
                    .await
                    .expect("leased since protects snapshot"),
            );
        }
        let subscription = self
            .subscription
            .as_mut()
            .expect("subscription initialized");
        while self.observed_upper < target {
            for event in subscription.next(None).await {
                match event {
                    ListenEvent::Updates(parts) => {
                        for part in parts {
                            let fetched = subscription.fetch_batch_part(part).await;
                            for (index, ((data, ()), _, diff)) in fetched.enumerate() {
                                let (key, observation) =
                                    Observation::decode(&data.0.expect("observation row"));
                                if self.certifier.is_some() {
                                    self.dirty.insert(key.clone());
                                }
                                let rows = self.observed.entry(key.clone()).or_default();
                                let count = rows.entry(observation).or_default();
                                *count += diff;
                                if *count == 0 {
                                    rows.remove(&observation);
                                }
                                if rows.is_empty() {
                                    self.observed.remove(&key);
                                }
                                if index % 1024 == 0 {
                                    tokio::task::yield_now().await;
                                }
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                    ListenEvent::Progress(upper) => {
                        self.observed_upper =
                            upper.as_option().copied().ok_or(PrepareError::Closed)?;
                    }
                }
            }
            tokio::task::yield_now().await;
        }
        // Listen may have advanced beyond target. The candidate must use the
        // frontier corresponding to the state we actually incorporated.
        let lower = self.observed_upper;
        let mut updates = Vec::new();
        let mut retired = BTreeSet::new();
        if let Some(certifier) = &self.certifier {
            let batch: Vec<_> = self
                .dirty
                .iter()
                .take(CERTIFICATION_BATCH)
                .cloned()
                .collect();
            let ids: BTreeSet<_> = batch
                .iter()
                .filter(|key| self.observed.contains_key(*key))
                .map(|key| key.0.parse().expect("native observation global id"))
                .collect();
            // Native observations only follow committed exact bindings, which
            // are never readmitted after deletion. Certifying AFTER consuming
            // this prefix therefore authorizes retracting its exact counts.
            let current = if ids.is_empty() {
                BTreeMap::new()
            } else {
                certifier
                    .collection_shards_at_current(&ids)
                    .await
                    .map_err(PrepareError::Certification)?
            };
            for key in batch {
                self.dirty.remove(&key);
                if let Some(rows) = self.observed.get(&key) {
                    let id = key.0.parse().expect("native observation global id");
                    let shard = key.1.parse().expect("native observation shard id");
                    if current.get(&id) != Some(&shard) {
                        for (observation, diff) in rows {
                            updates.push((
                                (SourceData(Ok(observation.row(&key))), ()),
                                lower,
                                -diff,
                            ));
                        }
                        // A conflict must refresh and certify again. Success
                        // also needs subscription confirmation before forgetting.
                        self.dirty.insert(key.clone());
                        retired.insert(key);
                    }
                }
            }
        }
        self.retired.extend(retired.iter().cloned());
        for (index, (key, sampled)) in samples.iter().enumerate() {
            if retired.contains(key) {
                continue;
            }
            let old = self.observed.get(key).map(|rows| {
                assert_eq!(rows.len(), 1, "one shared observation per key");
                let (observation, diff) = rows.first_key_value().expect("nonempty");
                assert_eq!(*diff, 1);
                *observation
            });
            let joined = old.unwrap_or_default().join(*sampled);
            if old != Some(joined) {
                if let Some(old) = old {
                    updates.push(((SourceData(Ok(old.row(key))), ()), lower, -1));
                }
                updates.push(((SourceData(Ok(joined.row(key))), ()), lower, 1));
            }
            if index % 1024 == 0 {
                tokio::task::yield_now().await;
            }
        }
        if updates.is_empty() {
            return Ok(None);
        }
        Ok(Some(Attempt {
            updates,
            lower,
            upper: Timestamp::from(now())
                .max(lower.try_step_forward().ok_or(PrepareError::Closed)?),
        }))
    }

    /// Once submitted, await the definitive CAS result, including at shutdown.
    async fn append(&mut self, attempt: Attempt) -> bool {
        self.writer
            .compare_and_append(
                attempt.updates,
                Antichain::from_elem(attempt.lower),
                Antichain::from_elem(attempt.upper),
            )
            .await
            .expect("valid observation append")
            .is_ok()
    }
}

async fn run(
    persist: PersistClient,
    binding: (GlobalId, ShardId, RelationDesc),
    now: NowFn,
    certifier: Arc<dyn CollectionMetadataCertifier>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    mut ticks: watch::Receiver<()>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut output = tokio::select! {
        _ = &mut shutdown => return,
        output = Output::open(&persist, binding) => output,
    };
    output.certifier = Some(certifier);
    let mut inventory = Inventory::default();
    let mut retry_at = Instant::now();
    let mut backoff = Duration::from_millis(10);
    // Even unchanged samples must consume peer progress so the retained reader
    // does not pin the observation shard's since indefinitely.
    let mut refresh_due = true;
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            command = commands.recv() => match command {
                Some(command) => inventory.command(command),
                None => return,
            },
            tick = ticks.changed() => {
                if tick.is_err() { return; }
                refresh_due = true;
                output.revisit();
                // Incorporate queued inventory changes before sampling. Bound
                // the drain so an active producer cannot monopolize CPU.
                for _ in 0..1024 {
                    match commands.try_recv() {
                        Ok(command) => inventory.command(command),
                        Err(_) => break,
                    }
                }
                tokio::select! {
                    _ = &mut shutdown => return,
                    () = inventory.sample(&persist) => {},
                }
            },
            _ = tokio::time::sleep_until(retry_at),
                if refresh_due || !inventory.pending.is_empty() || !output.dirty.is_empty() =>
            {
                let prepared = tokio::select! {
                    _ = &mut shutdown => return,
                    result = output.prepare(&inventory.pending, &now) => result,
                };
                inventory.retire(std::mem::take(&mut output.retired));
                match prepared {
                    Err(PrepareError::Closed) => return,
                    Err(PrepareError::Certification(error)) => {
                        tracing::warn!(?error, "certifying storage frontier observations");
                        backoff = (backoff * 2).min(Duration::from_secs(1));
                    }
                    Ok(None) => {
                        refresh_due = false;
                        let pending = std::mem::take(&mut inventory.pending);
                        for (index, entry) in pending.into_iter().enumerate() {
                            drop(entry);
                            if index % 1024 == 0 {
                                tokio::task::yield_now().await;
                            }
                        }
                        backoff = Duration::from_millis(10);
                    }
                    Ok(Some(attempt)) => {
                        // Even a successful bootstrap append needs a subsequent
                        // prepare. Only a fully incorporated no-op clears pending.
                        if output.append(attempt).await {
                            backoff = Duration::from_millis(10);
                        } else {
                            backoff = (backoff * 2).min(Duration::from_secs(1));
                        }
                    }
                }
                // Refresh after backoff, never reuse a pre-conflict candidate.
                retry_at = Instant::now() + backoff;
            },
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_client::critical::{CriticalReaderId, Opaque};
    use mz_persist_types::PersistLocation;
    use mz_repr::SqlScalarType;
    use mz_storage_types::controller::TxnsCodecRow;

    #[derive(Debug, Default)]
    struct Catalog {
        mappings: std::sync::Mutex<BTreeMap<GlobalId, ShardId>>,
        fail: std::sync::atomic::AtomicBool,
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl CollectionMetadataCertifier for Catalog {
        async fn collection_shards_at_current(
            &self,
            ids: &BTreeSet<GlobalId>,
        ) -> Result<BTreeMap<GlobalId, ShardId>, anyhow::Error> {
            use std::sync::atomic::Ordering::SeqCst;
            assert!(ids.len() <= CERTIFICATION_BATCH);
            self.calls.fetch_add(1, SeqCst);
            anyhow::ensure!(!self.fail.load(SeqCst), "catalog unavailable");
            Ok(self
                .mappings
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, _)| ids.contains(*id))
                .map(|(id, shard)| (*id, *shard))
                .collect())
        }
    }

    fn desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("object_id", SqlScalarType::String.nullable(false))
            .with_column("data_shard", SqlScalarType::String.nullable(false))
            .with_column("read_known", SqlScalarType::Bool.nullable(false))
            .with_column("read_frontier", SqlScalarType::MzTimestamp.nullable(true))
            .with_column("write_known", SqlScalarType::Bool.nullable(false))
            .with_column("write_frontier", SqlScalarType::MzTimestamp.nullable(true))
            .finish()
    }

    fn binding(shard: ShardId) -> (GlobalId, ShardId, RelationDesc) {
        (GlobalId::System(1), shard, desc())
    }

    fn metadata(data_shard: ShardId, txns_shard: Option<ShardId>) -> CollectionMetadata {
        CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: "mem://".parse().unwrap(),
                consensus_uri: "mem://".parse().unwrap(),
            },
            data_shard,
            relation_desc: RelationDesc::empty(),
            txns_shard,
        }
    }

    fn at(t: u64) -> Frontier {
        Frontier::At(Timestamp::from(t))
    }

    async fn advance(writer: &mut Writer, upper: u64) {
        let lower = writer.fetch_recent_upper().await.clone();
        writer
            .compare_and_append(
                Vec::<Update>::new(),
                lower,
                Antichain::from_elem(Timestamp::from(upper)),
            )
            .await
            .unwrap()
            .unwrap();
    }

    async fn data(persist: &PersistClient, shard: ShardId) -> (Writer, Reader) {
        persist
            .open(
                shard,
                Arc::new(RelationDesc::empty()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .unwrap()
    }

    async fn publish(output: &mut Output, samples: &Samples) {
        // A fixed clock exercises the upper+1 path on every subsequent append.
        let now = NowFn::from(|| 0);
        for _ in 0..16 {
            match output.prepare(samples, &now).await.unwrap() {
                None if output.dirty.is_empty() => return,
                None => {}
                Some(attempt) => {
                    output.append(attempt).await;
                }
            }
        }
        panic!("uncontended publication failed to converge");
    }

    /// Check the durable relation through a separate reader, not the writer's
    /// incremental cache. Multiplicities and uniqueness are part of the contract.
    async fn rows(persist: &PersistClient, shard: ShardId) -> Samples {
        let mut reader: Reader = persist
            .open_leased_reader(
                shard,
                Arc::new(desc()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
                false,
            )
            .await
            .unwrap();
        let upper = persist
            .recent_upper::<SourceData, (), Timestamp, StorageDiff>(shard, Diagnostics::for_tests())
            .await
            .unwrap();
        let snapshot = reader
            .snapshot_and_fetch(Antichain::from_elem(
                upper.as_option().unwrap().step_back().unwrap(),
            ))
            .await
            .unwrap();
        let mut counts = BTreeMap::new();
        for ((row, ()), _, diff) in snapshot {
            *counts.entry(row.0.unwrap()).or_insert(0) += diff;
        }
        let mut result = Samples::new();
        for (row, diff) in counts {
            if diff != 0 {
                assert_eq!(diff, 1);
                let (key, observation) = Observation::decode(&row);
                assert!(result.insert(key, observation).is_none());
            }
        }
        reader.expire().await;
        result
    }

    #[mz_ore::test(tokio::test)]
    async fn same_key_race_and_late_sample() {
        let persist = PersistClient::new_for_tests().await;
        let output_shard = ShardId::new();
        let data_shard = ShardId::new();
        let (mut writer, mut lease) = data(&persist, data_shard).await;
        let metadata = metadata(data_shard, None);
        advance(&mut writer, 5).await;
        let old = sample(&persist, &metadata).await;
        assert_eq!(
            old,
            Observation {
                read: at(0),
                write: at(5)
            }
        );
        advance(&mut writer, 10).await;
        lease
            .downgrade_since(&Antichain::from_elem(Timestamp::from(2)))
            .await;
        let new = sample(&persist, &metadata).await;
        assert_eq!(
            new,
            Observation {
                read: at(2),
                write: at(10)
            }
        );
        let key = (GlobalId::User(1).to_string(), data_shard.to_string());
        let older = BTreeMap::from([(key.clone(), old)]);
        let newer = BTreeMap::from([(key.clone(), new)]);
        let mut left = Output::open(&persist, binding(output_shard)).await;
        publish(&mut left, &Samples::new()).await;
        let mut right = Output::open(&persist, binding(output_shard)).await;
        let now = NowFn::from(|| 0);
        // Both candidates depend on exactly the same version and name the same
        // key. This forces a real Persist CAS conflict, regardless of scheduling.
        let a = left.prepare(&older, &now).await.unwrap().unwrap();
        let b = right.prepare(&newer, &now).await.unwrap().unwrap();
        assert_eq!(a.lower, b.lower);
        let (a, b) = tokio::join!(left.append(a), right.append(b));
        assert_ne!(a, b);
        publish(&mut left, &older).await;
        publish(&mut right, &newer).await;
        // The delayed sample predates the peer's progress. It must not retract it.
        publish(&mut left, &older).await;
        assert_eq!(rows(&persist, output_shard).await, newer);
    }

    #[mz_ore::test(tokio::test)]
    async fn independent_unknown_and_empty_frontiers() {
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let key = ("u1".to_owned(), ShardId::new().to_string());
        let mut left = Output::open(&persist, binding(shard)).await;
        let mut right = Output::open(&persist, binding(shard)).await;
        let samples = |read, write| BTreeMap::from([(key.clone(), Observation { read, write })]);
        publish(&mut left, &samples(Frontier::Unknown, Frontier::Unknown)).await;
        assert_eq!(
            rows(&persist, shard).await,
            samples(Frontier::Unknown, Frontier::Unknown)
        );
        publish(&mut left, &samples(Frontier::Unknown, at(7))).await;
        publish(&mut right, &samples(Frontier::Empty, Frontier::Unknown)).await;
        assert_eq!(rows(&persist, shard).await, samples(Frontier::Empty, at(7)));
        publish(&mut left, &samples(at(100), Frontier::Empty)).await;
        publish(&mut right, &samples(at(200), at(200))).await;
        assert_eq!(
            rows(&persist, shard).await,
            samples(Frontier::Empty, Frontier::Empty)
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn physical_since_and_transactional_upper() {
        let persist = PersistClient::new_for_tests().await;
        let data_shard = ShardId::new();
        let wal_shard = ShardId::new();
        let (mut writer, mut lease) = data(&persist, data_shard).await;
        advance(&mut writer, 20).await;
        let mut critical = persist
            .open_critical_since::<SourceData, (), Timestamp, StorageDiff>(
                data_shard,
                CriticalReaderId::new(),
                Opaque::encode(&0u64),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let epoch = critical.opaque().clone();
        critical
            .compare_and_downgrade_since(
                &epoch,
                (&epoch, &Antichain::from_elem(Timestamp::from(10))),
            )
            .await
            .unwrap();
        let mut wal: Writer = persist
            .open_writer(
                wal_shard,
                Arc::new(TxnsCodecRow::desc()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        advance(&mut wal, 40).await;
        let metadata = metadata(data_shard, Some(wal_shard));
        let key = ("u1".to_owned(), data_shard.to_string());
        let output_shard = ShardId::new();
        let mut output = Output::open(&persist, binding(output_shard)).await;
        // Controller protection is 10, but the physical since is held at 0 by
        // another lease. The table's physical upper is 20, but its WAL is at 40.
        for expected_since in [0, 5, 10] {
            if expected_since != 0 {
                lease
                    .downgrade_since(&Antichain::from_elem(Timestamp::from(expected_since)))
                    .await;
            }
            let observed = sample(&persist, &metadata).await;
            assert_eq!(
                observed,
                Observation {
                    read: at(expected_since),
                    write: at(40)
                }
            );
            let expected = BTreeMap::from([(key.clone(), observed)]);
            publish(&mut output, &expected).await;
            assert_eq!(rows(&persist, output_shard).await, expected);
        }
        // Observation itself acquired no lease that could hold back this since.
        lease.expire().await;
        critical
            .compare_and_downgrade_since(&epoch, (&epoch, &Antichain::new()))
            .await
            .unwrap();
        let observed = sample(&persist, &metadata).await;
        assert_eq!(
            observed,
            Observation {
                read: Frontier::Empty,
                write: at(40)
            }
        );
        let expected = BTreeMap::from([(key, observed)]);
        publish(&mut output, &expected).await;
        assert_eq!(rows(&persist, output_shard).await, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn inventory_removal_and_replacement_preserve_shared_rows() {
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let mut output = Output::open(&persist, binding(shard)).await;
        let mut inventory = Inventory::default();
        let id = GlobalId::User(1);
        let old = metadata(ShardId::new(), None);
        let new = metadata(ShardId::new(), None);
        inventory.command(Command::Upsert(id, old.clone()));
        inventory.sample(&persist).await;
        let mut expected = inventory.pending.clone();
        publish(&mut output, &inventory.pending).await;
        inventory.pending.clear();
        inventory.command(Command::Upsert(id, new.clone()));
        inventory.sample(&persist).await;
        assert_eq!(inventory.pending.len(), 1);
        expected.extend(inventory.pending.clone());
        publish(&mut output, &inventory.pending).await;
        inventory.pending.clear();
        inventory.command(Command::Remove(id));
        inventory.sample(&persist).await;
        assert!(inventory.pending.is_empty());
        publish(&mut output, &inventory.pending).await;
        assert_eq!(rows(&persist, shard).await, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn sampling_errors_preserve_independent_knowledge() {
        let persist = PersistClient::new_for_tests().await;
        let incompatible = ShardId::new();
        let _wrong_codec = persist
            .open_writer::<(), (), Timestamp, StorageDiff>(
                incompatible,
                Arc::new(UnitSchema),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();
        let shard = ShardId::new();
        let (mut writer, _lease) = data(&persist, shard).await;
        advance(&mut writer, 9).await;
        assert_eq!(
            sample(&persist, &metadata(incompatible, Some(shard))).await,
            Observation {
                read: Frontier::Unknown,
                write: at(9)
            },
        );
        assert_eq!(
            sample(&persist, &metadata(shard, Some(incompatible))).await,
            Observation {
                read: at(0),
                write: Frontier::Unknown
            },
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn catalog_retirement_conflict_error_and_restart() {
        use std::sync::atomic::Ordering::SeqCst;
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let live = ShardId::new();
        let old = ShardId::new();
        let successor = ShardId::new();
        // Retirement belongs to an exact binding, not the shared data shard.
        let absent = live;
        let key = |id: u64, shard: ShardId| (GlobalId::User(id).to_string(), shard.to_string());
        let observation = Observation {
            read: at(2),
            write: at(9),
        };
        let all = BTreeMap::from([
            (key(1, live), observation),
            (key(2, old), observation),
            (key(2, successor), observation),
            (key(3, absent), observation),
        ]);
        let expected = BTreeMap::from([
            (key(1, live), observation),
            (key(2, successor), observation),
        ]);
        let catalog = Arc::new(Catalog::default());
        *catalog.mappings.lock().unwrap() =
            BTreeMap::from([(GlobalId::User(1), live), (GlobalId::User(2), successor)]);
        let mut peer = Output::open(&persist, binding(shard)).await;
        publish(&mut peer, &all).await;
        let mut cleaner = Output::open(&persist, binding(shard)).await;
        cleaner.certifier = Some(Arc::<Catalog>::clone(&catalog));
        let now = NowFn::from(|| 0);
        catalog.fail.store(true, SeqCst);
        assert!(matches!(
            cleaner.prepare(&Samples::new(), &now).await,
            Err(PrepareError::Certification(_))
        ));
        assert_eq!(rows(&persist, shard).await, all);
        catalog.fail.store(false, SeqCst);
        let attempt = cleaner
            .prepare(&Samples::new(), &now)
            .await
            .unwrap()
            .unwrap();
        // Advance the same Persist upper behind the cleaner's candidate.
        let upper = *peer.writer.fetch_recent_upper().await.as_option().unwrap();
        advance(&mut peer.writer, u64::from(upper) + 1).await;
        assert!(!cleaner.append(attempt).await);
        let calls = catalog.calls.load(SeqCst);
        publish(&mut cleaner, &Samples::new()).await;
        assert!(catalog.calls.load(SeqCst) > calls);
        assert_eq!(rows(&persist, shard).await, expected);
        let mut inventory = Inventory::default();
        inventory.command(Command::Upsert(GlobalId::User(2), metadata(old, None)));
        inventory.pending.insert(key(2, old), observation);
        inventory.retire(std::mem::take(&mut cleaner.retired));
        assert!(inventory.entries.is_empty());
        assert!(inventory.pending.is_empty());

        // An already-submitted stale publisher can reinsert. No tombstone may
        // hide this new evidence, including across a cleaner restart.
        publish(&mut peer, &all).await;
        assert_eq!(rows(&persist, shard).await, all);
        publish(&mut cleaner, &Samples::new()).await;
        assert_eq!(rows(&persist, shard).await, expected);
        drop(cleaner);
        publish(&mut peer, &all).await;
        let mut restarted = Output::open(&persist, binding(shard)).await;
        restarted.certifier = Some(catalog);
        publish(&mut restarted, &Samples::new()).await;
        assert_eq!(rows(&persist, shard).await, expected);
    }

    #[mz_ore::test(tokio::test)]
    async fn retirement_retracts_exact_counts_with_sample_updates() {
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let live = ShardId::new();
        let dead = ShardId::new();
        let live_key = ("u1".to_owned(), live.to_string());
        let dead_key = ("u2".to_owned(), dead.to_string());
        let old = Observation {
            read: at(0),
            write: at(1),
        };
        let new = Observation {
            read: at(2),
            write: at(3),
        };
        let mut peer = Output::open(&persist, binding(shard)).await;
        publish(&mut peer, &BTreeMap::from([(live_key.clone(), old)])).await;
        let lower = *peer.writer.fetch_recent_upper().await.as_option().unwrap();
        assert!(
            peer.append(Attempt {
                updates: vec![
                    ((SourceData(Ok(old.row(&dead_key))), ()), lower, 3),
                    ((SourceData(Ok(new.row(&dead_key))), ()), lower, 2),
                ],
                lower,
                upper: lower.try_step_forward().unwrap(),
            })
            .await
        );
        let catalog = Arc::new(Catalog::default());
        catalog
            .mappings
            .lock()
            .unwrap()
            .insert(GlobalId::User(1), live);
        let mut cleaner = Output::open(&persist, binding(shard)).await;
        cleaner.certifier = Some(catalog);
        let pending = BTreeMap::from([(live_key.clone(), new), (dead_key, new)]);
        let attempt = cleaner
            .prepare(&pending, &NowFn::from(|| 0))
            .await
            .unwrap()
            .unwrap();
        assert!(cleaner.append(attempt).await);
        assert_eq!(
            rows(&persist, shard).await,
            BTreeMap::from([(live_key, new)])
        );
        let mut inventory = Inventory::default();
        inventory.pending = pending;
        inventory.command(Command::Upsert(GlobalId::User(2), metadata(dead, None)));
        inventory.retire(std::mem::take(&mut cleaner.retired));
        publish(&mut cleaner, &inventory.pending).await;
        assert!(cleaner.dirty.is_empty());
        assert_eq!(rows(&persist, shard).await.len(), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn unchanged_rows_revisited_in_bounded_batches() {
        let persist = PersistClient::new_for_tests().await;
        let shard = ShardId::new();
        let catalog = Arc::new(Catalog::default());
        let mut samples = Samples::new();
        for i in 0..u64::try_from(CERTIFICATION_BATCH * 2 + 1).expect("small batch count") {
            let data = ShardId::new();
            catalog
                .mappings
                .lock()
                .unwrap()
                .insert(GlobalId::User(i), data);
            samples.insert(
                (GlobalId::User(i).to_string(), data.to_string()),
                Observation {
                    read: at(0),
                    write: at(5),
                },
            );
        }
        let mut peer = Output::open(&persist, binding(shard)).await;
        publish(&mut peer, &samples).await;
        let mut cleaner = Output::open(&persist, binding(shard)).await;
        cleaner.certifier = Some(Arc::<Catalog>::clone(&catalog));
        publish(&mut cleaner, &Samples::new()).await;
        assert_eq!(rows(&persist, shard).await, samples);
        catalog.mappings.lock().unwrap().clear();
        let mut previous = samples.len();
        for _ in 0..3 {
            cleaner.revisit();
            assert!(cleaner.dirty.len() <= CERTIFICATION_BATCH);
            publish(&mut cleaner, &Samples::new()).await;
            let remaining = rows(&persist, shard).await.len();
            assert!(remaining < previous);
            assert!(previous - remaining <= CERTIFICATION_BATCH);
            previous = remaining;
        }
        assert!(rows(&persist, shard).await.is_empty());
        assert!(cleaner.observed.is_empty());
        assert!(cleaner.dirty.is_empty());
    }

    #[mz_ore::test(tokio::test)]
    async fn actor_samples_on_notification_and_stops_on_drop() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let persist = PersistClient::new_for_tests().await;
            let shard = ShardId::new();
            let data_shard = ShardId::new();
            let (mut writer, _lease) = data(&persist, data_shard).await;
            advance(&mut writer, 9).await;
            let actor = StorageFrontierObservations::new(
                persist.clone(),
                binding(shard),
                NowFn::from(|| 0),
                Arc::new(Catalog {
                    mappings: std::sync::Mutex::new(BTreeMap::from([(
                        GlobalId::User(1),
                        data_shard,
                    )])),
                    ..Default::default()
                }),
            );
            actor.upsert(GlobalId::User(1), metadata(data_shard, None));
            for _ in 0..100 {
                actor.sample();
            }
            let expected = BTreeMap::from([(
                ("u1".to_owned(), data_shard.to_string()),
                Observation {
                    read: at(0),
                    write: at(9),
                },
            )]);
            let mut upper = Antichain::from_elem(Timestamp::MIN);
            loop {
                persist
                    .wait_for_upper_past::<SourceData, (), Timestamp, StorageDiff>(
                        shard,
                        &upper,
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap();
                upper = persist
                    .recent_upper::<SourceData, (), Timestamp, StorageDiff>(
                        shard,
                        Diagnostics::for_tests(),
                    )
                    .await
                    .unwrap();
                if rows(&persist, shard).await == expected {
                    break;
                }
            }
            actor.remove(GlobalId::User(1));
            // Closing the receiver proves the detached task released its
            // inventory and Persist handles, not merely its command sender.
            let stopped = actor.sample.clone();
            drop(actor);
            stopped.closed().await;
            assert_eq!(rows(&persist, shard).await, expected);
        })
        .await
        .expect("actor made progress and stopped");
    }
}
