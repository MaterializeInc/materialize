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

use ore::metrics::MetricsRegistry;
use ore::now::{system_time, NowFn};
use persist::file::{FileBlob, FileLog};
use persist::indexed::runtime::{self, RuntimeClient, StreamReadHandle, StreamWriteHandle};
use persist::storage::LockInfo;
use persist::{Codec, Data};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::ok_err::OkErr;
use timely::dataflow::operators::{Concat, Inspect, Operator, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::PartialOrder;

fn main() {
    if let Err(err) = run(env::args().collect()) {
        eprintln!("error: {}", err);
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), Box<dyn Error>> {
    if args.len() != 2 {
        Err(format!("usage: {} <persist_dir>", &args[0]))?;
    }
    let base_dir = PathBuf::from(&args[1]);
    let persist = {
        let lock_info = LockInfo::new("kafka_upsert".into(), "nonce".into())?;
        let log = FileLog::new(base_dir.join("log"), lock_info.clone())?;
        let blob = FileBlob::new(base_dir.join("blob"), lock_info)?;
        runtime::start(log, blob, &MetricsRegistry::new())?
    };

    timely::execute_directly(|worker| {
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
            ok_stream.inspect(|d| println!("ok: {:?}", d));
            err_stream.inspect(|d| println!("err: {:?}", d));
        })
    });

    Ok(())
}

fn construct_persistent_upsert_source<G, S>(
    scope: &mut G,
    persist: RuntimeClient,
    name_base: &str,
) -> Result<
    (
        Stream<G, ((S::K, S::V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ),
    Box<dyn Error>,
>
where
    G: Scope<Timestamp = u64>,
    S: Source + 'static,
{
    let (ts_write, ts_read) = persist
        .create_or_load::<S::SourceTimestamp, AssignedTimestamp>(&format!("{}_ts", name_base))?;
    let (out_write, out_read) =
        persist.create_or_load::<S::K, S::V>(&format!("{}_out", name_base))?;

    // TODO: I think we need to synchronize the sources on what they think
    // current "system time" is. Otherwise, the timestamps that we seal up to
    // might not align. I haven't yet thought hard enought about whether this
    // is a problem, though.
    //
    // An "epoch" source that is an input to the "real" sources could do the
    // trick.
    // let epoch_interval_ms = 1000;

    let source_interval_ms = 1000;
    let timestamp_interval_ms = 5000;
    let now_fn = system_time;

    // TODO: This is not correct, because we might not have data or
    // bindings but still continue sealing up collections based on
    // processing time.
    let start_ts = cmp::min(sealed_ts(&ts_read)?, sealed_ts(&out_read)?);
    println!("Restored start timestamp: {}", start_ts);

    let (source_records, bindings) = read_source::<G, S>(
        scope,
        start_ts,
        now_fn,
        source_interval_ms,
        timestamp_interval_ms,
        ts_read,
    )?;

    let bindings_persist_err = persist_bindings(&bindings, ts_write);

    bindings.inspect(|b| println!("Binding: {:?}", b));

    let (records_persist_ok, records_persist_err) =
        persist_records::<G, S>(&source_records, start_ts, out_read, out_write)?;

    // TODO: add an operator that waits for the seal on bindings and
    // records (or upsert state, or whatnot) to advance before it releases records.
    //
    // We could do this either by listening for `Seal` events on a
    // StreamReadHandle or by waiting for the frontier to advance

    let errs = bindings_persist_err.concat(&records_persist_err);

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
    emission_interval_ms: u64,
    timestamp_interval_ms: u64,
    bindings_read: StreamReadHandle<S::SourceTimestamp, AssignedTimestamp>,
) -> Result<
    (
        Stream<G, ((S::K, S::V), u64, isize)>,
        Stream<G, ((S::SourceTimestamp, AssignedTimestamp), u64, isize)>,
    ),
    Box<dyn Error>,
>
where
    G: Scope<Timestamp = u64>,
    S: Source + 'static,
{
    // Fetch start offsets.
    // - TODO: Don't use read_to_end_flattened
    let starting_offsets: Vec<_> = bindings_read
        .snapshot()?
        .read_to_end_flattened()?
        .into_iter()
        // TODO: look extra hard at whether we need < or <= here. According
        // to how timely frontiers work we need <.
        .filter(|(_binding, ts, _diff)| *ts < start_ts)
        .map(|((source_timestamp, _assigned_timestamp), _ts, _diff)| source_timestamp)
        .collect();

    let mut source = S::new(starting_offsets, emission_interval_ms, now_fn);

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
            let now_clamped = now - (now % timestamp_interval_ms);
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

            if let Some((record, _source_timestamp)) = source.get_next_message() {
                let mut records_out = records_out.activate();
                let mut records_session = records_out.session(&records_capability);
                records_session.give((record, current_ts, 1));
            }

            activator.activate_after(Duration::from_millis(100));
        }
    });

    Ok((records, bindings))
}

/// Persists bindings to the given handle and emits errors as a `Stream`.
///
/// Returns a stream of errors.
// TODO: also return a Stream<G, ()> that could be used to listen on
// the frontier. As an alternative to listening for `Seal` events on a stream handle.
fn persist_bindings<G, ST>(
    bindings: &Stream<G, ((ST, AssignedTimestamp), u64, isize)>,
    bindings_write: StreamWriteHandle<ST, AssignedTimestamp>,
) -> Stream<G, (String, u64, isize)>
where
    G: Scope<Timestamp = u64>,
    ST: Codec + timely::Data,
{
    let mut buffer = Vec::new();
    let mut input_frontier =
        Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

    bindings.unary_frontier(
        Pipeline,
        "persist_bindings",
        move |mut capability, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    let result = bindings_write.write(buffer.iter().as_ref()).recv();
                    if let Err(e) = result {
                        let mut session = output.session(&time);
                        // TODO: make error retractable? Probably not...
                        session.give((e.to_string(), time.time().clone(), 1));
                    }
                });

                let new_input_frontier = input.frontier().frontier();
                let progress =
                    !PartialOrder::less_equal(&new_input_frontier, &input_frontier.borrow());
                if progress {
                    input_frontier.clear();
                    input_frontier.extend(new_input_frontier.into_iter().cloned());
                    if let Some(frontier) = input_frontier.elements().first() {
                        println!("Sealing bindings up to {}", *frontier);
                        let result = bindings_write.seal(*frontier).recv();
                        if let Err(e) = result {
                            let mut session = output.session(&capability);
                            // TODO: make error retractable? Probably not...
                            session.give((e.to_string(), capability.time().clone(), 1));
                        }
                        capability.downgrade(&frontier);
                    }
                }
            }
        },
    )
}

/// Persists records and sends them along. When restoring, we also emit the previously persisted
/// records.
///
/// We use `start_ts`, which is in the target timeline, to determine up to which time we should
/// restore previously written records.
fn persist_records<G, S>(
    records: &Stream<G, ((S::K, S::V), u64, isize)>,
    start_ts: u64,
    records_read: StreamReadHandle<S::K, S::V>,
    records_write: StreamWriteHandle<S::K, S::V>,
) -> Result<
    (
        Stream<G, ((S::K, S::V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ),
    Box<dyn Error>,
>
where
    G: Scope<Timestamp = u64>,
    S: Source,
{
    // TODO: don't use read_to_end_flattened()
    // TODO: actually do upserts here, and everywhere
    let mut prev_records = Vec::new();
    for ((k, v), ts, diff) in records_read.snapshot()?.read_to_end_flattened()? {
        // TODO: look extra hard at whether we need > or >= here. According
        // to how timely frontiers work we need >=.
        if ts >= start_ts {
            continue;
        }
        prev_records.push(((k, v), ts, diff));
    }

    let mut buffer = Vec::new();
    let mut input_frontier =
        Antichain::from_elem(<G::Timestamp as timely::progress::Timestamp>::minimum());

    let (oks, err) = records
        .unary_frontier(Pipeline, "persist_records", move |mut capability, _info| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    let result = records_write.write(buffer.iter().as_ref()).recv();
                    if let Err(e) = result {
                        let mut session = output.session(&time);
                        // TODO: make error retractable?
                        session.give(Err((e.to_string(), time.time().clone(), 1)));
                    }
                    // TODO: Be more efficient about this
                    let mut session = output.session(&time);
                    for record in buffer.drain(..) {
                        assert!(record.1 >= start_ts);
                        session.give(Ok(record));
                    }
                });

                let new_input_frontier = input.frontier().frontier();
                let progress =
                    !PartialOrder::less_equal(&new_input_frontier, &input_frontier.borrow());
                if progress {
                    input_frontier.clear();
                    input_frontier.extend(new_input_frontier.into_iter().cloned());
                    if let Some(frontier) = input_frontier.elements().first() {
                        println!("Sealing records up to {}", *frontier);
                        let result = records_write.seal(*frontier).recv();
                        if let Err(e) = result {
                            let mut session = output.session(&capability);
                            // TODO: make error retractable? Probably not...
                            session.give(Err((e.to_string(), capability.time().clone(), 1)));
                        }
                        capability.downgrade(&frontier);
                    }
                }
            }
        })
        .ok_err(|res| res);

    let prev_ok = prev_records.to_stream(&mut records.scope());

    let oks = oks.concat(&prev_ok);

    Ok((oks, err))
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
    type SourceTimestamp: timely::Data + persist::Data;
    type K: timely::Data + persist::Data + Hash;
    type V: timely::Data + persist::Data;

    fn new(
        starting_offsets: Vec<Self::SourceTimestamp>,
        emission_interval_ms: u64,
        now_fn: NowFn,
    ) -> Self
    where
        Self: Sized;

    fn get_next_message(&mut self) -> Option<((Self::K, Self::V), Self::SourceTimestamp)>;

    fn frontier(&self) -> Vec<Self::SourceTimestamp>;
}

struct KafkaSource {
    current_offsets: HashMap<KafkaPartition, KafkaOffset>,
    last_message_time: u64,
    emission_interval_ms: u64,
    now_fn: NowFn,
}

impl Source for KafkaSource {
    type SourceTimestamp = SourceTimestamp<KafkaPartition, KafkaOffset>;
    type K = String;
    type V = String;

    fn new(
        starting_offsets: Vec<Self::SourceTimestamp>,
        emission_interval_ms: u64,
        now_fn: NowFn,
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

        println!("Kafka Source, starting offsets: {:?}", starting_offsets);

        let mut current_offsets = HashMap::new();

        for SourceTimestamp(partition, offset) in starting_offsets {
            current_offsets
                .entry(partition)
                .and_modify(|current_offset| {
                    *current_offset = std::cmp::max(*current_offset, offset)
                })
                .or_insert(offset);
        }

        println!("Current offsets: {:?}", current_offsets);

        KafkaSource {
            last_message_time: u64::MIN,
            current_offsets,
            emission_interval_ms,
            now_fn,
        }
    }

    fn get_next_message(
        &mut self,
    ) -> Option<(
        (String, String),
        SourceTimestamp<KafkaPartition, KafkaOffset>,
    )> {
        let now = (self.now_fn)();
        let now_clamped = now - (now % self.emission_interval_ms);

        if now_clamped > self.last_message_time {
            self.last_message_time = now_clamped;

            let num_partitions = self.current_offsets.len();
            let partition =
                KafkaPartition((self.last_message_time as usize % num_partitions) as u64);
            let current_offset = self
                .current_offsets
                .get_mut(&partition)
                .unwrap_or_else(|| panic!("missing partition offset for {:?}", partition));
            current_offset.0 += 1;
            let kv = (
                format!("k{}", partition.0),
                format!("v{}", current_offset.0),
            );
            Some((kv, SourceTimestamp(partition, *current_offset)))
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

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct KafkaOffset(u64);

#[derive(Debug, Clone, Copy, Hash, PartialOrd, Ord, PartialEq, Eq, Default)]
struct KafkaPartition(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct AssignedTimestamp(u64);

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Default)]
struct SourceTimestamp<P, O>(P, O);

mod kafka_offset_impls {
    use std::convert::TryFrom;

    use persist::Codec;

    use crate::AssignedTimestamp;
    use crate::KafkaOffset;
    use crate::KafkaPartition;
    use crate::SourceTimestamp;

    impl Codec for KafkaPartition {
        fn codec_name() -> &'static str {
            "KafkaPartition"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaPartition(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl Codec for KafkaOffset {
        fn codec_name() -> &'static str {
            "KafkaOffset"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(KafkaOffset(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl Codec for AssignedTimestamp {
        fn codec_name() -> &'static str {
            "AssignedTimestamp"
        }

        fn size_hint(&self) -> usize {
            8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            buf.extend(&self.0.to_le_bytes())
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            Ok(AssignedTimestamp(u64::from_le_bytes(
                <[u8; 8]>::try_from(buf).map_err(|err| err.to_string())?,
            )))
        }
    }

    impl<P: Codec, O: Codec> Codec for SourceTimestamp<P, O> {
        fn codec_name() -> &'static str {
            "SourceTimestamp"
        }

        fn size_hint(&self) -> usize {
            self.0.size_hint() + self.1.size_hint() + 8 + 8
        }

        fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
            let mut inner_buf = Vec::new();
            self.0.encode(&mut inner_buf);
            buf.extend(&inner_buf.len().to_le_bytes());
            buf.extend(&inner_buf);

            inner_buf.clear();
            self.1.encode(&mut inner_buf);
            buf.extend(&inner_buf.len().to_le_bytes());
            buf.extend(&inner_buf);
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
