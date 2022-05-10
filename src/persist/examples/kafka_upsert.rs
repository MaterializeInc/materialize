// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp, env, process};

use differential_dataflow::Hashable;
use mz_persist::operators::stream::Persist;
use serde::{Deserialize, Serialize};
use tracing::info;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{NowFn, SYSTEM_TIME};
use mz_persist::client::{MultiWriteHandle, RuntimeClient, StreamReadHandle};
use mz_persist::error::{Error as PersistError, ErrorLog};
use mz_persist::file::FileBlob;
use mz_persist::location::{Blob, LockInfo};
use mz_persist::operators::stream::{AwaitFrontier, Seal};
use mz_persist::operators::upsert::{PersistentUpsert, PersistentUpsertConfig};
use mz_persist::runtime::{self, RuntimeConfig};
use mz_persist::Data;
use mz_persist_types::Codec;

use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, Inspect, Map, Probe, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

fn main() {
    mz_ore::test::init_logging_default("trace");

    if let Err(err) = run(env::args().collect()) {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    if args.len() < 2 {
        Err(format!("usage: {} <persist_dir>", &args[0]))?;
    }
    let base_dir = PathBuf::from(&args[1]);

    let persist = {
        let lock_info = LockInfo::new("kafka_upsert".into(), "nonce".into())?;
        let log = ErrorLog;
        let blob = FileBlob::open_exclusive(base_dir.join("blob").into(), lock_info)?;
        runtime::start(
            RuntimeConfig::default(),
            log,
            blob,
            mz_build_info::DUMMY_BUILD_INFO,
            &MetricsRegistry::new(),
            None,
        )?
    };

    let worker_guards = timely::execute_from_args(args.into_iter(), move |worker| {
        let persist = persist.clone();

        worker.dataflow(|scope| {
            let (ok_stream, err_stream) = construct_persistent_upsert_source::<_, KafkaSource>(
                scope,
                persist,
                "persistent_kafka_1",
            )
            .unwrap_or_else(|err| {
                let ok_stream = operator::empty(scope);
                let err_stream = vec![(err.to_string(), 0, 1)].to_stream(scope);
                (ok_stream, err_stream)
            });
            ok_stream.inspect(|d| info!("ok: {:?}", d));
            err_stream.inspect(|d| info!("err: {:?}", d));
        })
    })?;

    let _result = worker_guards
        .join()
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    Ok(())
}

fn construct_persistent_upsert_source<G, S>(
    scope: &mut G,
    persist: RuntimeClient,
    name_base: &str,
) -> Result<
    (
        Stream<G, ((S::K, S::V), u64, i64)>,
        Stream<G, (String, u64, i64)>,
    ),
    Box<dyn Error>,
>
where
    G: Scope<Timestamp = u64>,
    S: Source + 'static,
{
    let (ts_write, ts_read) = persist
        .create_or_load::<S::SourceTimestamp, AssignedTimestamp>(&format!("{}_ts", name_base));
    let (out_write, out_read) = persist.create_or_load::<S::K, S::V>(&format!("{}_out", name_base));

    // TODO: I think we need to synchronize the sources on what they think
    // current "system time" is. Otherwise, the timestamps that we seal up to
    // might not align. I haven't yet thought hard enought about whether this
    // is a problem, though.
    //
    // An "epoch" source that is an input to the "real" sources could do the
    // trick.
    // let epoch_interval = Duration::from_secs(1);

    let source_interval = Duration::from_secs(1);
    let timestamp_interval = Duration::from_secs(5);
    let now_fn = SYSTEM_TIME.clone();

    let start_ts = cmp::min(sealed_ts(&ts_read)?, sealed_ts(&out_read)?);
    println!("Restored start timestamp: {}", start_ts);

    let (source_records, bindings) = read_source::<G, S>(
        scope,
        start_ts,
        now_fn,
        source_interval,
        timestamp_interval,
        ts_read,
    )?;

    let (bindings_persist_oks, bindings_persist_err) =
        bindings.persist("timestamp_bindings", ts_write.clone());

    bindings.inspect(|b| println!("Binding: {:?}", b));

    // Notice how we ignore diff here. Before the upsert uperator, we don't actually have
    // differential updates. We could update our mock-source to not emit the diff field.
    let source_records = source_records
        .map(|((key, value), source_ts, _assigned_ts, _diff)| (key, Some(value), source_ts));

    let (upsert_oks, upsert_persist_errs) = source_records.persistent_upsert(
        "kafka_example",
        Antichain::from_elem(u64::MIN),
        PersistentUpsertConfig::new(start_ts, out_read, out_write.clone()),
    );

    let mut seal_handle = MultiWriteHandle::new(&out_write);
    seal_handle
        .add_stream(&ts_write)
        .expect("known from same runtime");

    let upsert_oks = upsert_oks.seal(
        "timestamp_bindings|upsert_state",
        vec![bindings_persist_oks.probe()],
        seal_handle,
    );

    // Wait for persistent collections to be sealed before forwarding results.  We could do this
    // either by listening for `Seal` events on a StreamReadHandle or by waiting for the frontier
    // to advance, as we do here.

    // This operator controls whether we do "optimistic" or "pessimistic" concurrency control.
    // Without blocking here, we would send data along straight away but the frontier would still
    // be held back until data is persisted and sealed by the upstream persist/seal operators.
    let records_persist_ok = upsert_oks.await_frontier("upsert_state");

    let errs = bindings_persist_err.concat(&upsert_persist_errs);

    Ok((records_persist_ok, errs))
}

/// Reads from a source, assigns timestamps to records and emits them. We periodically "close"
/// timestamps. When timestamps are closed we emit a binding on the secondary output stream and
/// downgrade our capability to the new timstamp binding.
///
/// We use `start_ts`, which is in the target timeline, to determine up to which time we should
/// restore previously written bindings, which we use to reset the offsets of the source. Yes, very
/// Kafka-specific for now. 🤷
fn read_source<G, S>(
    scope: &mut G,
    start_ts: u64,
    now_fn: NowFn,
    emission_interval: Duration,
    timestamp_interval: Duration,
    bindings_read: StreamReadHandle<S::SourceTimestamp, AssignedTimestamp>,
) -> Result<
    (
        Stream<G, ((S::K, S::V), S::SourceTimestamp, u64, i64)>,
        Stream<G, ((S::SourceTimestamp, AssignedTimestamp), u64, i64)>,
    ),
    Box<dyn Error>,
>
where
    G: Scope<Timestamp = u64>,
    S: Source + 'static,
{
    let num_workers = scope.peers();
    let worker_index = scope.index();

    // Fetch start offsets.
    // - TODO: Don't use collect
    let starting_offsets: Vec<_> = bindings_read
        .snapshot()?
        .into_iter()
        .collect::<Result<Vec<_>, PersistError>>()?
        .into_iter()
        // TODO: look extra hard at whether we need < or <= here. According
        // to how timely frontiers work we need <.
        .filter(|(_binding, ts, _diff)| *ts < start_ts)
        .map(|((source_timestamp, _assigned_timestamp), _ts, _diff)| source_timestamp)
        .collect();

    let mut source = S::new(
        starting_offsets,
        emission_interval,
        now_fn.clone(),
        worker_index,
        num_workers,
    );

    let mut source_op = OperatorBuilder::new("read_source".to_string(), scope.clone());
    let activator = scope.activator_for(&source_op.operator_info().address[..]);

    let (mut records_out, records) = source_op.new_output();
    let (mut bindings_out, bindings) = source_op.new_output();

    let mut current_ts = now_fn();

    source_op.build(move |mut capabilities| {
        let mut records_capability = capabilities.remove(0);
        let mut bindings_capability = capabilities.remove(0);

        move |_frontiers| {
            // only emit a binding update if we actually have a current offset
            let now = now_fn();
            let now_clamped = now - (now % timestamp_interval.as_millis() as u64);
            if now_clamped != current_ts {
                // emit bindings for all offsets we're currently aware of
                let mut bindings_out = bindings_out.activate();
                let mut bindings_session = bindings_out.session(&bindings_capability);

                for source_timestamp in source.frontier() {
                    bindings_session.give((
                        (source_timestamp, AssignedTimestamp(current_ts)),
                        current_ts,
                        1,
                    ));
                }

                // then update to new binding
                current_ts = now_clamped;
                bindings_capability.downgrade(&current_ts);
                records_capability.downgrade(&current_ts);
            }

            if let Some((record, source_timestamp)) = source.get_next_message() {
                let mut records_out = records_out.activate();
                let mut records_session = records_out.session(&records_capability);
                records_session.give((record, source_timestamp, current_ts, 1));
            }

            activator.activate_after(Duration::from_millis(100));
        }
    });

    Ok((records, bindings))
}

// HACK: This should be a method on StreamReadHandle that actually queries the
// runtime.
//
// TODO: We're slightly better, now that we have `get_seal()`. Maybe that's already enough?
fn sealed_ts<K: Data, V: Data>(read: &StreamReadHandle<K, V>) -> Result<u64, Box<dyn Error>> {
    let seal_ts = read.snapshot()?.get_seal();

    if let Some(sealed) = seal_ts.first() {
        Ok(*sealed)
    } else {
        Ok(0)
    }
}

/// Cheapo version of our current `SourceReader`. The new source trait from Petros uses an async
/// `Stream` instead of polling for messages.
trait Source {
    type SourceTimestamp: timely::Data + timely::ExchangeData + mz_persist::Data + Ord;
    type K: timely::Data + timely::ExchangeData + mz_persist::Data + Hash + Codec;
    type V: timely::Data + timely::ExchangeData + mz_persist::Data + Hash + Codec;

    fn new(
        starting_offsets: Vec<Self::SourceTimestamp>,
        emission_interval: Duration,
        now_fn: NowFn,
        worker_index: usize,
        num_workers: usize,
    ) -> Self
    where
        Self: Sized;

    fn get_next_message(&mut self) -> Option<((Self::K, Self::V), Self::SourceTimestamp)>;

    fn frontier(&self) -> Vec<Self::SourceTimestamp>;
}

struct KafkaSource {
    current_offsets: HashMap<KafkaPartition, KafkaOffset>,
    last_message_time: u64,
    emission_interval: Duration,
    now_fn: NowFn,
    last_partition: usize,
}

impl Source for KafkaSource {
    type SourceTimestamp = SourceTimestamp<KafkaPartition, KafkaOffset>;
    type K = String;
    type V = String;

    fn new(
        starting_offsets: Vec<Self::SourceTimestamp>,
        emission_interval: Duration,
        now_fn: NowFn,
        worker_index: usize,
        num_workers: usize,
    ) -> Self
    where
        Self: Sized,
    {
        // We can put in multiple partitions here, but having just one makes
        // it easier to eyeball the data on restore.
        let starting_offsets = if starting_offsets.is_empty() {
            vec![
                SourceTimestamp(KafkaPartition(0), KafkaOffset(0)),
                // SourceTimestamp(KafkaPartition(1), KafkaOffset(0)),
                // SourceTimestamp(KafkaPartition(2), KafkaOffset(0)),
            ]
        } else {
            starting_offsets
        };

        let mut current_offsets = HashMap::new();

        for SourceTimestamp(partition, offset) in starting_offsets {
            if (partition.hashed() as usize) % num_workers == worker_index {
                current_offsets
                    .entry(partition)
                    .and_modify(|current_offset| {
                        *current_offset = std::cmp::max(*current_offset, offset)
                    })
                    .or_insert(offset);
            }
        }

        println!(
            "Source(worker = {}/{}): starting offsets: {:?}",
            worker_index, num_workers, current_offsets
        );

        KafkaSource {
            last_message_time: u64::MIN,
            current_offsets,
            emission_interval,
            now_fn,
            last_partition: 0,
        }
    }

    fn get_next_message(
        &mut self,
    ) -> Option<(
        (String, String),
        SourceTimestamp<KafkaPartition, KafkaOffset>,
    )> {
        let now = (self.now_fn)();
        let now_clamped = now - (now % self.emission_interval.as_millis() as u64);

        if self.current_offsets.is_empty() {
            // this sink doesn't have any partitions assigned
            return None;
        }

        if now_clamped > self.last_message_time {
            self.last_message_time = now_clamped;

            // This is not efficient code, don't do this in production!
            let partitions: Vec<_> = self.current_offsets.keys().cloned().collect();
            let partition = partitions
                .get(self.last_partition)
                .expect("missing partition");
            self.last_partition = (self.last_partition + 1) % partitions.len();

            let current_offset = self
                .current_offsets
                .get_mut(&partition)
                .unwrap_or_else(|| panic!("missing partition offset for {:?}", partition));
            current_offset.0 += 1;
            let kv = (
                format!("k{}", partition.0),
                format!("v{}", current_offset.0),
            );
            Some((kv, SourceTimestamp(partition.clone(), *current_offset)))
        } else {
            None
        }
    }

    fn frontier(&self) -> Vec<Self::SourceTimestamp> {
        self.current_offsets
            .iter()
            .map(|(partition, offset)| SourceTimestamp(partition.clone(), offset.clone()))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default, Serialize, Deserialize)]
struct KafkaOffset(u64);

#[derive(
    Debug, Clone, Copy, Hash, PartialOrd, Ord, PartialEq, Eq, Default, Serialize, Deserialize,
)]
struct KafkaPartition(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct AssignedTimestamp(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default, Serialize, Deserialize)]
struct SourceTimestamp<P, O>(P, O);

mod kafka_offset_impls {
    use bytes::BufMut;

    use mz_persist_types::Codec;

    use crate::AssignedTimestamp;
    use crate::KafkaOffset;
    use crate::KafkaPartition;
    use crate::SourceTimestamp;

    impl Codec for KafkaPartition {
        fn codec_name() -> String {
            "KafkaPartition".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            buf.put_slice(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaPartition(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl Codec for KafkaOffset {
        fn codec_name() -> String {
            "KafkaOffset".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            buf.put_slice(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaOffset(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl Codec for AssignedTimestamp {
        fn codec_name() -> String {
            "AssignedTimestamp".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            buf.put_slice(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(AssignedTimestamp(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl<P: Codec, O: Codec> Codec for SourceTimestamp<P, O> {
        fn codec_name() -> String {
            "SourceTimestamp".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: BufMut,
        {
            let mut inner_buf = Vec::new();
            self.0.encode(&mut inner_buf);
            buf.put_slice(&inner_buf.len().to_le_bytes());
            buf.put_slice(&inner_buf);

            inner_buf.clear();
            self.1.encode(&mut inner_buf);
            buf.put_slice(&inner_buf.len().to_le_bytes());
            buf.put_slice(&inner_buf);
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            let partition_len_bytes = &buf[0..8];
            let partition_len_bytes =
                <[u8; 8]>::try_from(partition_len_bytes).map_err(|err| err.to_string())?;
            let partition_end: usize = 8 + usize::from_le_bytes(partition_len_bytes);
            let partition_slice = &buf[8..partition_end];
            let partition = P::decode(partition_slice)?;

            let offset_len_bytes = &buf[(partition_end)..(partition_end + 8)];
            let offset_len_bytes =
                <[u8; 8]>::try_from(offset_len_bytes).map_err(|err| err.to_string())?;
            let offset_end: usize = partition_end + 8 + usize::from_le_bytes(offset_len_bytes);
            let offset_slice = &buf[(partition_end + 8)..offset_end];
            let offset = O::decode(offset_slice)?;

            Ok(SourceTimestamp(partition, offset))
        }
    }
}
