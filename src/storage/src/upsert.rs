// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::convert::AsRef;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::future::FutureExt;
use futures::StreamExt;
use indexmap::map::Entry;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, DatumVec, Diff, Row};
use mz_rocksdb::ValueIterator;
use mz_storage_operators::metrics::BackpressureMetrics;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::dyncfgs;
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_storage_types::sources::envelope::UpsertEnvelope;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::{Capability, CapabilitySet, InputCapability, Operator};
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::render::sources::OutputIndex;
use crate::storage_state::StorageInstanceContext;
use crate::upsert::types::PutStats;
use autospill::AutoSpillBackend;
use memory::InMemoryHashMap;
use types::{
    snapshot_merge_function, upsert_bincode_opts, BincodeOpts, StateValue, UpsertState,
    UpsertStateBackend, Value,
};

mod autospill;
mod memory;
mod rocksdb;
// TODO(aljoscha): Move next to upsert module, rename to upsert_types.
pub(crate) mod types;

pub type UpsertValue = Result<Row, UpsertError>;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UpsertKey([u8; 32]);

impl AsRef<[u8]> for UpsertKey {
    #[inline(always)]
    // Note we do 1 `multi_get` and 1 `multi_put` while processing a _batch of updates_. Within the
    // batch, we effectively consolidate each key, before persisting that consolidated value.
    // Easy!!
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for UpsertKey {
    fn from(bytes: &[u8]) -> Self {
        UpsertKey(bytes.try_into().expect("invalid key length"))
    }
}

/// The hash function used to map upsert keys. It is important that this hash is a cryptographic
/// hash so that there is no risk of collisions. Collisions on SHA256 have a probability of 2^128
/// which is many orders of magnitude smaller than many other events that we don't even think about
/// (e.g bit flips). In short, we can safely assume that sha256(a) == sha256(b) iff a == b.
type KeyHash = Sha256;

impl UpsertKey {
    pub fn from_key(key: Result<&Row, &UpsertError>) -> Self {
        Self::from_iter(key.map(|r| r.iter()))
    }

    pub fn from_value(value: Result<&Row, &UpsertError>, key_indices: &[usize]) -> Self {
        thread_local! {
            /// A thread-local datum cache used to calculate hashes
            static VALUE_DATUMS: RefCell<DatumVec> = RefCell::new(DatumVec::new());
        }
        VALUE_DATUMS.with(|value_datums| {
            let mut value_datums = value_datums.borrow_mut();
            let value = value.map(|v| value_datums.borrow_with(v));
            let key = match value {
                Ok(ref datums) => Ok(key_indices.iter().map(|&idx| datums[idx])),
                Err(err) => Err(err),
            };
            Self::from_iter(key)
        })
    }

    pub fn from_iter<'a, 'b>(
        key: Result<impl Iterator<Item = Datum<'a>> + 'b, &UpsertError>,
    ) -> Self {
        thread_local! {
            /// A thread-local datum cache used to calculate hashes
            static KEY_DATUMS: RefCell<DatumVec> = RefCell::new(DatumVec::new());
        }
        KEY_DATUMS.with(|key_datums| {
            let mut key_datums = key_datums.borrow_mut();
            // Borrowing the DatumVec gives us a temporary buffer to store datums in that will be
            // automatically cleared on Drop. See the DatumVec docs for more details.
            let mut key_datums = key_datums.borrow();
            let key: Result<&[Datum], Datum> = match key {
                Ok(key) => {
                    for datum in key {
                        key_datums.push(datum);
                    }
                    Ok(&*key_datums)
                }
                Err(UpsertError::Value(err)) => {
                    key_datums.extend(err.for_key.iter());
                    Ok(&*key_datums)
                }
                Err(UpsertError::KeyDecode(err)) => Err(Datum::Bytes(&err.raw)),
                Err(UpsertError::NullKey(_)) => Err(Datum::Null),
            };
            let mut hasher = DigestHasher(KeyHash::new());
            key.hash(&mut hasher);
            Self(hasher.0.finalize().into())
        })
    }
}

struct DigestHasher<H: Digest>(H);

impl<H: Digest> Hasher for DigestHasher<H> {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        panic!("digest wrapper used to produce a hash");
    }
}

use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;

/// This leaf operator drops `token` after the input reaches the `resume_upper`.
/// This is useful to take coordinated actions across all workers, after the `upsert`
/// operator has rehydrated.
pub fn rehydration_finished<G, T>(
    scope: G,
    source_config: &crate::source::RawSourceCreationConfig,
    // A token that we can drop to signal we are finished rehydrating.
    token: impl std::any::Any + 'static,
    resume_upper: Antichain<T>,
    input: &Stream<G, Infallible>,
) where
    G: Scope<Timestamp = T>,
    T: Timestamp,
{
    let worker_id = source_config.worker_id;
    let id = source_config.id;
    let mut builder = AsyncOperatorBuilder::new(format!("rehydration_finished({id}"), scope);
    let mut input = builder.new_disconnected_input(input, Pipeline);

    builder.build(move |_capabilities| async move {
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());
        // Ensure this operator finishes if the resume upper is `[0]`
        while !PartialOrder::less_equal(&resume_upper, &input_upper) {
            let Some(event) = input.next().await else {
                break;
            };
            if let AsyncEvent::Progress(upper) = event {
                input_upper = upper;
            }
        }
        tracing::info!(
            "timely-{worker_id} upsert source {id} has downgraded past the resume upper ({resume_upper:?}) across all workers",
        );
        drop(token);
    });
}

/// Resumes an upsert computation at `resume_upper` given as inputs a collection of upsert commands
/// and the collection of the previous output of this operator.
/// Returns a tuple of
/// - A collection of the computed upsert operator and,
/// - A health update stream to propagate errors
pub(crate) fn upsert<G: Scope, FromTime>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    upsert_envelope: UpsertEnvelope,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Vec<PressOnDropButton>>,
    source_config: crate::source::RawSourceCreationConfig,
    instance_context: &StorageInstanceContext,
    storage_configuration: &StorageConfiguration,
    dataflow_paramters: &crate::internal_control::DataflowParameters,
    backpressure_metrics: Option<BackpressureMetrics>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
    FromTime: Timestamp,
{
    let upsert_metrics = source_config.metrics.get_upsert_metrics(
        source_config.id,
        source_config.worker_id,
        backpressure_metrics,
    );

    let rocksdb_cleanup_tries =
        dyncfgs::STORAGE_ROCKSDB_CLEANUP_TRIES.get(storage_configuration.config_set());

    // Whether or not to partially drain the input buffer
    // to prevent buffering of the _upstream_ snapshot.
    let prevent_snapshot_buffering =
        dyncfgs::STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING.get(storage_configuration.config_set());
    // If the above is true, the number of timely batches to process at once.
    let snapshot_buffering_max = dyncfgs::STORAGE_UPSERT_MAX_SNAPSHOT_BATCH_BUFFERING
        .get(storage_configuration.config_set());

    // Whether we should provide the upsert state merge operator to the RocksDB instance
    // (for faster performance during snapshot hydration).
    let rocksdb_use_native_merge_operator =
        dyncfgs::STORAGE_ROCKSDB_USE_MERGE_OPERATOR.get(storage_configuration.config_set());

    let upsert_config = UpsertConfig {
        shrink_upsert_unused_buffers_by_ratio: storage_configuration
            .parameters
            .shrink_upsert_unused_buffers_by_ratio,
    };

    let thin_input = upsert_thinning(input);

    if let Some(scratch_directory) = instance_context.scratch_directory.as_ref() {
        let tuning = dataflow_paramters.upsert_rocksdb_tuning_config.clone();

        let allow_auto_spill = storage_configuration
            .parameters
            .upsert_auto_spill_config
            .allow_spilling_to_disk;
        let spill_threshold = storage_configuration
            .parameters
            .upsert_auto_spill_config
            .spill_to_disk_threshold_bytes;

        tracing::info!(
            ?tuning,
            ?storage_configuration.parameters.upsert_auto_spill_config,
            ?rocksdb_use_native_merge_operator,
            "timely-{} rendering {} with rocksdb-backed upsert state",
            source_config.worker_id,
            source_config.id
        );
        let rocksdb_shared_metrics = Arc::clone(&upsert_metrics.rocksdb_shared);
        let rocksdb_instance_metrics = Arc::clone(&upsert_metrics.rocksdb_instance_metrics);
        let rocksdb_dir = scratch_directory
            .join("storage")
            .join("upsert")
            .join(source_config.id.to_string())
            .join(source_config.worker_id.to_string());

        let env = instance_context.rocksdb_env.clone();

        let rocksdb_in_use_metric = Arc::clone(&upsert_metrics.rocksdb_autospill_in_use);

        // A closure that will initialize and return a configured RocksDB instance
        let rocksdb_init_fn = move || async move {
            let merge_operator = if rocksdb_use_native_merge_operator {
                Some((
                    "upsert_state_snapshot_merge_v1".to_string(),
                    |a: &[u8], b: ValueIterator<BincodeOpts, StateValue<Option<FromTime>>>| {
                        snapshot_merge_function::<Option<FromTime>>(a.into(), b)
                    },
                ))
            } else {
                None
            };
            rocksdb::RocksDB::new(
                mz_rocksdb::RocksDBInstance::new(
                    &rocksdb_dir,
                    mz_rocksdb::InstanceOptions::new(
                        env,
                        rocksdb_cleanup_tries,
                        merge_operator,
                        // For now, just use the same config as the one used for
                        // merging snapshots.
                        upsert_bincode_opts(),
                    ),
                    tuning,
                    rocksdb_shared_metrics,
                    rocksdb_instance_metrics,
                )
                .await
                .unwrap(),
            )
        };

        // TODO(aljoscha): I don't like how we have basically the same call
        // three times here, but it's hard working around those impl Futures
        // that return an impl Trait. Oh well...
        if allow_auto_spill {
            upsert_operator(
                &thin_input,
                upsert_envelope.key_indices,
                resume_upper,
                previous,
                previous_token,
                upsert_metrics,
                source_config,
                move || async move {
                    AutoSpillBackend::new(rocksdb_init_fn, spill_threshold, rocksdb_in_use_metric)
                },
                upsert_config,
                storage_configuration,
                prevent_snapshot_buffering,
                snapshot_buffering_max,
            )
        } else {
            upsert_operator(
                &thin_input,
                upsert_envelope.key_indices,
                resume_upper,
                previous,
                previous_token,
                upsert_metrics,
                source_config,
                rocksdb_init_fn,
                upsert_config,
                storage_configuration,
                prevent_snapshot_buffering,
                snapshot_buffering_max,
            )
        }
    } else {
        tracing::info!(
            "timely-{} rendering {} with memory-backed upsert state",
            source_config.worker_id,
            source_config.id
        );
        upsert_operator(
            &thin_input,
            upsert_envelope.key_indices,
            resume_upper,
            previous,
            previous_token,
            upsert_metrics,
            source_config,
            || async { InMemoryHashMap::default() },
            upsert_config,
            storage_configuration,
            prevent_snapshot_buffering,
            snapshot_buffering_max,
        )
    }
}

// A shim so we can dispatch based on the dyncfg that tells us which upsert
// operator to use.
fn upsert_operator<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: Collection<G, Result<Row, DataflowError>, Diff>,
    persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::RawSourceCreationConfig,
    state: F,
    upsert_config: UpsertConfig,
    storage_configuration: &StorageConfiguration,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<Option<FromTime>>,
    FromTime: Debug + timely::ExchangeData + Ord,
{
    let use_continual_feedback_upsert =
        dyncfgs::STORAGE_USE_CONTINUAL_FEEDBACK_UPSERT.get(storage_configuration.config_set());

    tracing::info!(id = %source_config.id, %use_continual_feedback_upsert, "upsert operator implementation");

    if use_continual_feedback_upsert {
        todo!("move the continual impl out into it's own file")
    } else {
        upsert_classic(
            input,
            key_indices,
            resume_upper,
            persist_input,
            persist_token,
            upsert_metrics,
            source_config,
            state,
            upsert_config,
            prevent_snapshot_buffering,
            snapshot_buffering_max,
        )
    }
}

/// Renders an operator that discards updates that are known to not affect the outcome of upsert in
/// a streaming fashion. For each distinct (key, time) in the input it emits the value with the
/// highest from_time. Its purpose is to thin out data as much as possible before exchanging them
/// across workers.
fn upsert_thinning<G, K, V, FromTime>(
    input: &Collection<G, (K, V, FromTime), Diff>,
) -> Collection<G, (K, V, FromTime), Diff>
where
    G: Scope,
    G::Timestamp: TotalOrder,
    K: timely::Data + Eq + Ord,
    V: timely::Data,
    FromTime: Timestamp,
{
    input
        .inner
        .unary(Pipeline, "UpsertThinning", |_, _| {
            // A capability suitable to emit all updates in `updates`, if any.
            let mut capability: Option<InputCapability<G::Timestamp>> = None;
            // A batch of received updates
            let mut updates = Vec::new();
            let mut tmp = Vec::new();
            move |input, output| {
                while let Some((cap, data)) = input.next() {
                    assert!(
                        data.iter().all(|(_, _, diff)| *diff > 0),
                        "invalid upsert input"
                    );
                    data.swap(&mut tmp);
                    updates.append(&mut tmp);
                    match capability.as_mut() {
                        Some(capability) => {
                            if cap.time() <= capability.time() {
                                *capability = cap;
                            }
                        }
                        None => capability = Some(cap),
                    }
                }
                if let Some(capability) = capability.take() {
                    // Sort by (key, time, Reverse(from_time)) so that deduping by (key, time) gives
                    // the latest change for that key.
                    updates.sort_unstable_by(|a, b| {
                        let ((key1, _, from_time1), time1, _) = a;
                        let ((key2, _, from_time2), time2, _) = b;
                        Ord::cmp(
                            &(key1, time1, Reverse(from_time1)),
                            &(key2, time2, Reverse(from_time2)),
                        )
                    });
                    let mut session = output.session(&capability);
                    session.give_iterator(updates.drain(..).dedup_by(|a, b| {
                        let ((key1, _, _), time1, _) = a;
                        let ((key2, _, _), time2, _) = b;
                        (key1, time1) == (key2, time2)
                    }))
                }
            }
        })
        .as_collection()
}

/// Helper method for `upsert_classic` used to stage `data` updates
/// from the input/source timely edge.
fn stage_input<T, FromTime>(
    stash: &mut Vec<(
        Capability<T>,
        UpsertKey,
        Reverse<FromTime>,
        Option<UpsertValue>,
    )>,
    cap: Capability<T>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
    storage_shrink_upsert_unused_buffers_by_ratio: usize,
) where
    T: PartialOrder + timely::progress::Timestamp,
    FromTime: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff > 0, "invalid upsert input");
        // TODO: Don't retain a cap per update but instead bunch them up.
        let cap = cap.delayed(&time);
        (cap, key, Reverse(order), value)
    }));

    if storage_shrink_upsert_unused_buffers_by_ratio > 0 {
        let reduced_capacity = stash.capacity() / storage_shrink_upsert_unused_buffers_by_ratio;
        if reduced_capacity > stash.len() {
            stash.shrink_to(reduced_capacity);
        }
    }
}

/// The style of drain we are performing on the stash. `AtTime`-drains cannot
/// assume that all values have been seen, and must leave tombstones behind for deleted values.
#[derive(Debug)]
enum DrainStyle<'a, T> {
    ToUpper {
        input_upper: &'a Antichain<T>,
        persist_upper: &'a Antichain<T>,
    },
    // TODO: For partial draining when taking the source snapshot.
    #[allow(unused)]
    AtTime(T),
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
async fn drain_staged_input<S, G, T, FromTime, E>(
    stash: &mut Vec<(
        Capability<T>,
        UpsertKey,
        Reverse<FromTime>,
        Option<UpsertValue>,
    )>,
    commands_state: &mut indexmap::IndexMap<UpsertKey, types::UpsertValueAndSize<Option<FromTime>>>,
    output_updates: &mut Vec<(Result<Row, UpsertError>, Capability<T>, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    drain_style: DrainStyle<'_, T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, Option<FromTime>>,
) where
    S: UpsertStateBackend<Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Debug + timely::progress::Timestamp,
    FromTime: timely::ExchangeData + Ord,
    E: UpsertErrorEmitter<G>,
{
    // Sort by (key, time, Reverse(from_time)) so that deduping by (key, time) gives
    // the latest change for that key.
    stash.sort_unstable_by(|a, b| {
        let (ts1, key1, from_ts1, val1) = a;
        let (ts2, key2, from_ts2, val2) = b;
        Ord::cmp(
            &(ts1.time(), key1, from_ts1, val1),
            &(ts2.time(), key2, from_ts2, val2),
        )
    });

    // Find the prefix that we can emit
    let idx = stash.partition_point(|(ts, _, _, _)| match &drain_style {
        DrainStyle::ToUpper {
            input_upper,
            persist_upper,
        } => {
            // We make sure that a) we only process updates when we know their
            // timestamp is complete, that is there will be no more updates for
            // that timestamp, and b) that "previous" times in the persist
            // output are complete. The latter makes sure that we emit updates
            // for the next timestamp that are consistent with the global state
            // in the output persist shard, which also serves as a persistent
            // copy of our in-memory/on-disk upsert state.
            !input_upper.less_equal(ts.time()) && !persist_upper.less_than(ts.time())
        }
        DrainStyle::AtTime(time) => ts.time() <= time,
    });

    tracing::debug!(?drain_style, updates = idx, "draining stash");

    // Read the previous values _per key_ out of `state`, recording it
    // along with the value with the _latest timestamp for that key_.
    commands_state.clear();
    for (_, key, _, _) in stash.iter().take(idx) {
        commands_state.entry(*key).or_default();
    }

    // These iterators iterate in the same order because `commands_state`
    // is an `IndexMap`.
    multi_get_scratch.clear();
    multi_get_scratch.extend(commands_state.iter().map(|(k, _)| *k));
    match state
        .multi_get(multi_get_scratch.drain(..), commands_state.values_mut())
        .await
    {
        Ok(_) => {}
        Err(e) => {
            error_emitter
                .emit("Failed to fetch records from state".to_string(), e)
                .await;
        }
    }

    // From the prefix that can be emitted we can deduplicate based on (ts, key) in
    // order to only process the command with the maximum order within the (ts,
    // key) group. This is achieved by wrapping order in `Reverse(FromTime)` above.;
    let mut commands = stash.drain(..idx).dedup_by(|a, b| {
        let ((a_ts, a_key, _, _), (b_ts, b_key, _, _)) = (a, b);
        a_ts == b_ts && a_key == b_key
    });

    let bincode_opts = types::upsert_bincode_opts();
    // Upsert the values into `commands_state`, by recording the latest
    // value (or deletion). These will be synced at the end to the `state`.
    //
    // Note that we are effectively doing "mini-upsert" here, using
    // `command_state`. This "mini-upsert" is seeded with data from `state`, using
    // a single `multi_get` above, and the final state is written out into
    // `state` using a single `multi_put`. This simplifies `UpsertStateBackend`
    // implementations, and reduces the number of reads and write we need to do.
    //
    // This "mini-upsert" technique is actually useful in `UpsertState`'s
    // `consolidate_snapshot_read_write_inner` implementation, minimizing gets and puts on
    // the `UpsertStateBackend` implementations. In some sense, its "upsert all the way down".
    while let Some((ts, key, from_time, value)) = commands.next() {
        let mut command_state = if let Entry::Occupied(command_state) = commands_state.entry(key) {
            command_state
        } else {
            panic!("key missing from commands_state");
        };

        let existing_value = &mut command_state.get_mut().value;

        if let Some(cs) = existing_value.as_mut() {
            cs.ensure_decoded(bincode_opts);
        }

        // Skip this command if its order key is below the one in the upsert state.
        // Note that the existing order key may be `None` if the existing value
        // is from snapshotting, which always sorts below new values/deletes.
        let existing_order = existing_value.as_ref().and_then(|cs| cs.order().as_ref());
        if existing_order >= Some(&from_time.0) {
            // Skip this update. If no later updates adjust this key, then we just
            // end up writing the same value back to state. If there
            // is nothing in the state, `existing_order` is `None`, and this
            // does not occur.
            continue;
        }

        match value {
            Some(value) => {
                if let Some(old_value) = existing_value
                    .replace(StateValue::value(value.clone(), Some(from_time.0.clone())))
                {
                    if let Value::Value(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts.clone(), -1));
                    }
                }
                output_updates.push((value, ts, 1));
            }
            None => {
                if let Some(old_value) = existing_value.take() {
                    if let Value::Value(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts, -1));
                    }
                }

                // Record a tombstone for deletes.
                *existing_value = Some(StateValue::tombstone(Some(from_time.0.clone())));
            }
        }
    }
}

/// Helper method for `upsert_inner` used to ingest state updates from the
/// persist input.
async fn ingest_state_updates<S, G, T, FromTime, E>(
    updates: &mut Vec<(UpsertKey, UpsertValue, T, Diff)>,
    persist_upper: &Antichain<T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, Option<FromTime>>,
) where
    S: UpsertStateBackend<Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Debug + timely::progress::Timestamp,
    FromTime: timely::ExchangeData + Ord,
    E: UpsertErrorEmitter<G>,
{
    // Sort by (key, diff) and make sure additions sort before retractions,
    // which allows us to de-duplicate below, and only keep the latest addition,
    // if any.
    updates.sort_unstable_by(|a, b| {
        let (key1, _val1, ts1, diff1) = a;
        let (key2, _val2, ts2, diff2) = b;
        Ord::cmp(&(ts1, key1, Reverse(diff1)), &(ts2, key2, Reverse(diff2)))
    });

    // Find the prefix that we can ingest.
    let idx = updates.partition_point(|(_, _, ts, _)| !persist_upper.less_equal(ts));

    tracing::debug!(?persist_upper, updates = idx, "ingesting state updates");

    let mut precomputed_putstats = PutStats::default();

    // It's not ideal that we iterate once before ingesting, but we're doing it
    // to precalculate our stats. We might want to rework how we keep stats in
    // the future.
    for (key, value, ts, diff) in &updates[0..idx] {
        match diff {
            1 => {
                let value = StateValue::value(value.clone(), None::<FromTime>);
                let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                precomputed_putstats.size_diff += size;
                precomputed_putstats.values_diff += 1;
            }
            -1 => {
                let value = StateValue::value(value.clone(), None::<FromTime>);
                let size: i64 = value.memory_size().try_into().expect("less than i64 size");
                precomputed_putstats.size_diff -= size;
                precomputed_putstats.values_diff -= 1;
            }
            invalid_diff => {
                panic!(
                    "unexpected diff for update to upsert state: {:?}",
                    (key, value, ts, invalid_diff)
                );
            }
        }
    }

    let eligible_commands = updates.drain(..idx).dedup_by(|a, b| {
        let ((a_key, _, a_ts, _), (b_key, _, b_ts, _)) = (a, b);
        a_ts == b_ts && a_key == b_key
    });

    let commands = eligible_commands
        .map(|(key, value, ts, diff)| match diff {
            1 => {
                let value = StateValue::value(value, None::<FromTime>);
                (
                    key,
                    types::PutValue {
                        value: Some(value),
                        previous_value_metadata: None,
                    },
                )
            }
            -1 => (
                key.clone(),
                types::PutValue {
                    value: None,
                    previous_value_metadata: None,
                },
            ),
            invalid_diff => {
                panic!(
                    "unexpected diff for update to upsert state: {:?}",
                    (key, value, ts, invalid_diff)
                );
            }
        })
        .collect_vec();

    match state
        .multi_put_with_stats(commands, precomputed_putstats)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            error_emitter
                .emit("Failed to update records in state".to_string(), e)
                .await;
        }
    }
}

// Created a struct to hold the configs for upserts.
// So that new configs don't require a new method parameter.
pub(crate) struct UpsertConfig {
    pub shrink_upsert_unused_buffers_by_ratio: usize,
}

// WIP: As of right now, this mirrors upsert_continual_feedback, so it can be
// reviewed as a diff. Once the PR review is done, I will revert much of this
// file to the previous state and then we really only have the new operator impl
// in upsert_continual_feedback.
fn upsert_classic<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: Collection<G, Result<Row, DataflowError>, Diff>,
    mut persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::RawSourceCreationConfig,
    state_fn: F,
    upsert_config: UpsertConfig,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<Option<FromTime>>,
    FromTime: Debug + timely::ExchangeData + Ord,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let persist_input = persist_input.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(err),
                _ => return None,
            },
            Err(_) => return None,
        };
        Some((UpsertKey::from_value(value.as_ref(), &key_indices), value))
    });
    let (output_handle, output) = builder.new_output();

    // An output that just reports progress of the snapshot consolidation process upstream to the
    // persist source to ensure that backpressure is applied
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();

    let (mut health_output, health_stream) = builder.new_output();
    let mut input = builder.new_input_for(
        &input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    let mut persist_input = builder.new_disconnected_input(
        &persist_input.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
    );

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);

    let shutdown_button = builder.build(move |caps| async move {
        let [mut output_cap, snapshot_cap, health_cap]: [_; 3] = caps.try_into().unwrap();
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        // The order key of the `UpsertState` is `Option<FromTime>`, which implements `Default`
        // (as required for `consolidate_snapshot_chunk`), with slightly more efficient serialization
        // than a default `Partitioned`.

        let mut state = UpsertState::<_, Option<FromTime>>::new(
            state_fn().await,
            Arc::clone(&upsert_shared_metrics),
            &upsert_metrics,
            source_config.source_statistics.clone(),
            upsert_config.shrink_upsert_unused_buffers_by_ratio,
        );

        // True while we're still reading the initial "snapshot" (a whole bunch
        // of updates, all at the same initial timestamp) from our persist
        // input.
        let mut snapshotting_persist = true;

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<_, types::UpsertValueAndSize<Option<FromTime>>> =
            indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // For our source input, both of these.
        let mut stash = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());


        // For our persist/feedback input, both of these.
        let mut persist_stash = vec![];
        let mut persist_upper = Antichain::from_elem(Timestamp::minimum());

        // A buffer for our output.
        let mut output_updates = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);
        let mut legacy_errors_to_correct = vec![];


        loop {
            tokio::select! {
                Some(persist_event) = persist_input.next() => {

                    // Buffer as many events as possible. This should be
                    // bounded, as new data can't be produced in this worker
                    // until we yield to timely.
                    let persist_events = [persist_event]
                        .into_iter()
                        .chain(std::iter::from_fn(|| persist_input.next().now_or_never().flatten()));

                    // Read away as much input as we can.
                    for persist_event in persist_events {
                        tracing::trace!(?persist_event, "persist input");

                        match persist_event {
                            AsyncEvent::Data(_cap, data) => {
                                persist_stash.extend(data.into_iter().map(|((key, value), ts, diff)| {
                                    (key, value, ts, diff)
                                }))
                            }
                            AsyncEvent::Progress(upper) => persist_upper = upper,
                        }
                    }

                    if snapshotting_persist {

                        // Determine if this is the last time we call
                        // consolidate_snapshot_chunk, and update our
                        // `snapshotting_persist` state.
                        //
                        // There are two situations in which we're reading a
                        // snapshot:
                        //
                        // 1. Reading the initial snapshot from the source: in
                        //    this case there is no state in our output yet that
                        //    we could read back in and our resume_upper is [0].
                        //    We know we're done once the persist_upper is
                        //    "past" that.
                        // 2. Re-hydrating our state from persist: some instance
                        //    of the source has already read/ingested/written
                        //    down the initial snapshot from the source. We are
                        //    reading a snapshot of our own upsert state back in
                        //    from persist. In this case we read the snapshot at
                        //    a timestamp that is "before" the resume upper, and
                        //    we know that we're done when the persist_upper is
                        //    "at or past" the resume_upper.
                        let last_snapshot =  if resume_upper.less_equal(&G::Timestamp::minimum()) {
                            PartialOrder::less_than(&resume_upper, &persist_upper)
                        } else {
                            PartialOrder::less_equal(&resume_upper, &persist_upper)
                        };

                        // Sort by ts and only present updates that are not
                        // beyond the resume upper.
                        persist_stash.sort_unstable_by(|a, b| {
                            let (_, _, ts1, _) = a;
                            let (_, _, ts2, _) = b;
                            Ord::cmp(
                                ts1,
                                ts2,
                            )
                        });

                        // Find the prefix that we can ingest.
                        let idx = persist_stash.partition_point(|(_, _, ts, _)| {
                            let first_source_snapshot = ts == &G::Timestamp::minimum();
                            let rehydration_snapshot = !resume_upper.less_equal(ts);
                            first_source_snapshot || rehydration_snapshot
                        });

                        tracing::debug!(persist_stash = %persist_stash.len(), %idx, %last_snapshot, ?resume_upper, ?persist_upper, "ingesting persist snapshot chunk");

                        // Check for errors in the prefix that we will ingest.
                        for (_, value, _ts, diff) in persist_stash.iter_mut().take(idx) {
                            if let Err(UpsertError::Value(ref mut err)) = value {
                                // If we receive a legacy error in the snapshot we will keep a note of it but
                                // insert a non-legacy error in our state. This is so that if this error is
                                // ever retracted we will correctly retract the non-legacy version because by
                                // that time we will have emitted the error correction, which happens before
                                // processing any of the new source input.
                                if err.is_legacy_dont_touch_it {
                                    legacy_errors_to_correct.push((err.clone(), diff.clone()));
                                    err.is_legacy_dont_touch_it = false;
                                }
                            }
                        }


                        snapshotting_persist = !last_snapshot;

                        match state
                            .consolidate_snapshot_chunk(
                                persist_stash
                                    .drain(..idx)
                                    .map(|(key, val, _ts, diff)| (key, val, diff)),
                                last_snapshot,
                            )
                            .await
                        {
                            Ok(_) => {
                                let _ = snapshot_cap.downgrade(persist_upper.iter());
                            }
                            Err(e) => {
                                // Make sure our persist source can shut down.
                                persist_token.take();
                                UpsertErrorEmitter::<G>::emit(
                                    &mut error_emitter,
                                    "Failed to rehydrate state".to_string(),
                                    e,
                                )
                                .await;
                            }
                        }


                        if last_snapshot {
                            // Now it's time to emit the error corrections. It doesn't matter at what timestamp we emit
                            // them at because all they do is change the representation. The error count at any
                            // timestamp remains constant.
                            upsert_metrics
                                .legacy_value_errors
                                .set(u64::cast_from(legacy_errors_to_correct.len()));
                            if !legacy_errors_to_correct.is_empty() {
                                tracing::error!(
                                    "unexpected legacy error representation. Found {} occurences",
                                    legacy_errors_to_correct.len()
                                );
                            }
                            consolidation::consolidate(&mut legacy_errors_to_correct);
                            for (mut error, diff) in legacy_errors_to_correct.drain(..) {
                                assert!(
                                    error.is_legacy_dont_touch_it,
                                    "attempted to correct non-legacy error"
                                );
                                tracing::info!("correcting legacy error {error:?} with diff {diff}");
                                let time = output_cap.time().clone();
                                let retraction = Err(UpsertError::Value(error.clone()));
                                error.is_legacy_dont_touch_it = false;
                                let insertion = Err(UpsertError::Value(error));
                                output_handle.give(&output_cap, (retraction, time.clone(), -diff));
                                output_handle.give(&output_cap, (insertion, time, diff));
                            }

                            tracing::info!(
                                "timely-{} upsert source {} finished rehydration",
                                source_config.worker_id,
                                source_config.id
                            );

                            let _ = snapshot_cap.downgrade(&[]);
                        }

                    }

                    // We don't have an else here, because we might notice we're
                    // done snapshotting but already have some more persist
                    // updates staged. We have to work them off eagerly here.
                    if !snapshotting_persist {
                        tracing::debug!(persist_stash = %persist_stash.len(), ?persist_upper, "ingesting state updates from persist");

                        ingest_state_updates::<_, G, _, _, _>(
                            &mut persist_stash,
                            &persist_upper,
                            &mut error_emitter,
                            &mut state,
                            ).await;
                    }
                }
                input_event = input.next() => {
                    let Some(input_event) = input_event else {
                        tracing::debug!("input exhausted, shutting down");
                        break;
                    };
                    // Buffer as many events as possible. This should be bounded, as new data can't be
                    // produced in this worker until we yield to timely.
                    let events = [input_event]
                        .into_iter()
                        .chain(std::iter::from_fn(|| input.next().now_or_never().flatten()))
                        .enumerate();

                    for (i, event) in events {
                        tracing::trace!(?event, "source input");

                        match event {
                            AsyncEvent::Data(cap, mut data) => {
                                tracing::trace!(
                                    time=?cap.time(),
                                    updates=%data.len(),
                                    "received data in upsert"
                                );

                                let event_time = cap.time().clone();

                                stage_input(
                                    &mut stash,
                                    cap,
                                    &mut data,
                                    &input_upper,
                                    &resume_upper,
                                    upsert_config.shrink_upsert_unused_buffers_by_ratio,
                                );

                                // For the very first snapshot from the source,
                                // we allow emitting partial updates without yet
                                // seeing all updates for that timestamp. For
                                // this, we use an ephemeral upsert state to run
                                // our upserting logic that we throw away once
                                // we receive the first updates on the persist
                                // input.
                                //
                                // We can only do this for the very first
                                // snapshot, because all subsequent emitted
                                // updates need to have a consistent view of
                                // upsert state as ingested from the persist
                                // input.
                                //
                                // This is a load-bearing optimization, as it is
                                // required to avoid buffering the entire source
                                // snapshot in the `stash`.
                                if prevent_snapshot_buffering && resume_upper.less_equal(&G::Timestamp::minimum()) && event_time == G::Timestamp::minimum() {
                                    tracing::info!(?event_time, ?resume_upper, ?output_cap, "partial drain not implemented, yet");
                                }
                            }
                            AsyncEvent::Progress(upper) => {
                                tracing::trace!(?upper, "received progress in upsert");

                                // Ignore progress updates before the `resume_upper`, which is our initial
                                // capability post-snapshotting.
                                if PartialOrder::less_than(&upper, &resume_upper) {
                                    tracing::trace!(?upper, ?resume_upper, "ignoring progress updates before resume_upper");
                                    continue;
                                }

                                if let Some(ts) = upper.as_option() {
                                    tracing::trace!(?ts, "downgrading output capability");
                                    let _ = output_cap.try_downgrade(ts);
                                }
                                input_upper = upper;
                            }
                        }
                        let events_processed = i + 1;
                        if let Some(max) = snapshot_buffering_max {
                            if events_processed >= max {
                                break;
                            }
                        }
                    }
                }
            };

            // We try and drain from our stash every time we go through the
            // loop. More of our stash can become eligible for draining both
            // when the source-input frontier advances or when the persist
            // frontier advances.

            drain_staged_input::<_, G, _, _, _>(
                &mut stash,
                &mut commands_state,
                &mut output_updates,
                &mut multi_get_scratch,
                DrainStyle::ToUpper{input_upper: &input_upper, persist_upper: &persist_upper},
                &mut error_emitter,
                &mut state,
            )
            .await;

            tracing::trace!(?output_updates, "output updates for complete timestamp");

            // TODO: Feels somewhat inefficient to be peeling
            // of and creating new vecs per group of
            // timestamp. Especially since at steady state
            // we're expecting to only have one group...
            while !output_updates.is_empty() {
                let cap = output_updates[0].1.clone();
                let ts = cap.time();

                let partition_point = output_updates
                    .partition_point(|update| update.1.time() == ts);

                let mut ts_updates: Vec<_> = output_updates.drain(0..partition_point)
                    .map(|(update, cap, diff)| (update, cap.time().clone(), diff))
                    .collect();

                output_handle.give_container(&cap,&mut ts_updates);
            }
        }
    });

    (
        output.as_collection().map(|result| match result {
            Ok(ok) => Ok(ok),
            Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(err))),
        }),
        health_stream,
        snapshot_stream,
        shutdown_button.press_on_drop(),
    )
}

#[async_trait::async_trait(?Send)]
pub(crate) trait UpsertErrorEmitter<G> {
    async fn emit(&mut self, context: String, e: anyhow::Error);
}

#[async_trait::async_trait(?Send)]
impl<G: Scope> UpsertErrorEmitter<G>
    for (
        &mut AsyncOutputHandle<
            <G as ScopeParent>::Timestamp,
            CapacityContainerBuilder<Vec<(OutputIndex, HealthStatusUpdate)>>,
            Tee<<G as ScopeParent>::Timestamp, Vec<(OutputIndex, HealthStatusUpdate)>>,
        >,
        &Capability<<G as ScopeParent>::Timestamp>,
    )
{
    async fn emit(&mut self, context: String, e: anyhow::Error) {
        process_upsert_state_error::<G>(context, e, self.0, self.1).await
    }
}

/// Emit the given error, and stall till the dataflow is restarted.
async fn process_upsert_state_error<G: Scope>(
    context: String,
    e: anyhow::Error,
    health_output: &AsyncOutputHandle<
        <G as ScopeParent>::Timestamp,
        CapacityContainerBuilder<Vec<(OutputIndex, HealthStatusUpdate)>>,
        Tee<<G as ScopeParent>::Timestamp, Vec<(OutputIndex, HealthStatusUpdate)>>,
    >,
    health_cap: &Capability<<G as ScopeParent>::Timestamp>,
) {
    let update = HealthStatusUpdate::halting(e.context(context).to_string_with_causes(), None);
    health_output.give(health_cap, (0, update));
    std::future::pending::<()>().await;
    unreachable!("pending future never returns");
}
