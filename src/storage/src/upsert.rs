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
use std::path::PathBuf;
use std::sync::Arc;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use futures::future::FutureExt;
use indexmap::map::Entry;
use itertools::Itertools;
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
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
use timely::dataflow::operators::{Capability, InputCapability, Operator};
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::storage_state::StorageInstanceContext;
use crate::upsert_continual_feedback;
use types::{
    BincodeOpts, StateValue, UpsertState, UpsertStateBackend, Value, consolidating_merge_function,
    upsert_bincode_opts,
};

pub mod memory;
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

use self::types::ValueMetadata;

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
            %worker_id,
            source_id = %id,
            "upsert source has downgraded past the resume upper ({resume_upper:?}) across all workers",
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
    source_config: crate::source::SourceExportCreationConfig,
    instance_context: &StorageInstanceContext,
    storage_configuration: &StorageConfiguration,
    dataflow_paramters: &crate::internal_control::DataflowParameters,
    backpressure_metrics: Option<BackpressureMetrics>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (Option<GlobalId>, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder + Sync,
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
    FromTime: Timestamp + Sync,
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

    let tuning = dataflow_paramters.upsert_rocksdb_tuning_config.clone();

    // When running RocksDB in memory, the file system is emulated, so the path doesn't
    // matter. However, we still need to pick one that exists on the host because of
    // https://github.com/rust-rocksdb/rust-rocksdb/issues/1015.  RocksDB will still
    // create a lock file at this path, so we need to ensure that whatever path is used
    // will be unique per worker.
    let rocksdb_dir = instance_context
        .scratch_directory
        .clone()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("storage")
        .join("upsert")
        .join(source_config.id.to_string())
        .join(source_config.worker_id.to_string());

    tracing::info!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        ?rocksdb_dir,
        ?tuning,
        ?rocksdb_use_native_merge_operator,
        "rendering upsert source"
    );

    let rocksdb_shared_metrics = Arc::clone(&upsert_metrics.rocksdb_shared);
    let rocksdb_instance_metrics = Arc::clone(&upsert_metrics.rocksdb_instance_metrics);

    let env = instance_context.rocksdb_env.clone();

    // A closure that will initialize and return a configured RocksDB instance
    let rocksdb_init_fn = move || async move {
        let merge_operator = if rocksdb_use_native_merge_operator {
            Some((
                        "upsert_state_snapshot_merge_v1".to_string(),
                        |a: &[u8],
                         b: ValueIterator<
                            BincodeOpts,
                            StateValue<G::Timestamp, Option<FromTime>>,
                        >| {
                            consolidating_merge_function::<G::Timestamp, Option<FromTime>>(
                                a.into(),
                                b,
                            )
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
            .unwrap(),
        )
    };

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

// A shim so we can dispatch based on the dyncfg that tells us which upsert
// operator to use.
fn upsert_operator<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    persist_input: Collection<G, Result<Row, DataflowError>, Diff>,
    persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
    state: F,
    upsert_config: UpsertConfig,
    _storage_configuration: &StorageConfiguration,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (Option<GlobalId>, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder + Sync,
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + Sync,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<G::Timestamp, Option<FromTime>>,
    FromTime: Debug + timely::ExchangeData + Ord + Sync,
{
    // Hard-coded to true because classic UPSERT cannot be used safely with
    // concurrent ingestions, which we need for both 0dt upgrades and
    // multi-replica ingestions.
    let use_continual_feedback_upsert = true;

    tracing::info!(id = %source_config.id, %use_continual_feedback_upsert, "upsert operator implementation");

    if use_continual_feedback_upsert {
        upsert_continual_feedback::upsert_inner(
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
            move |input, output| {
                while let Some((cap, data)) = input.next() {
                    assert!(
                        data.iter().all(|(_, _, diff)| diff.is_positive()),
                        "invalid upsert input"
                    );
                    updates.append(data);
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
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, FromTime), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
    storage_shrink_upsert_unused_buffers_by_ratio: usize,
) where
    T: PartialOrder,
    FromTime: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff.is_positive(), "invalid upsert input");
        (time, key, Reverse(order), value)
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
    ToUpper(&'a Antichain<T>),
    AtTime(T),
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
async fn drain_staged_input<S, G, T, FromTime, E>(
    stash: &mut Vec<(T, UpsertKey, Reverse<FromTime>, Option<UpsertValue>)>,
    commands_state: &mut indexmap::IndexMap<
        UpsertKey,
        types::UpsertValueAndSize<T, Option<FromTime>>,
    >,
    output_updates: &mut Vec<(Result<Row, UpsertError>, T, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    drain_style: DrainStyle<'_, T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S, T, Option<FromTime>>,
    source_config: &crate::source::SourceExportCreationConfig,
) where
    S: UpsertStateBackend<T, Option<FromTime>>,
    G: Scope,
    T: PartialOrder + Ord + Clone + Send + Sync + Serialize + Debug + 'static,
    FromTime: timely::ExchangeData + Ord + Sync,
    E: UpsertErrorEmitter<G>,
{
    stash.sort_unstable();

    // Find the prefix that we can emit
    let idx = stash.partition_point(|(ts, _, _, _)| match &drain_style {
        DrainStyle::ToUpper(upper) => !upper.less_equal(ts),
        DrainStyle::AtTime(time) => ts <= time,
    });

    tracing::trace!(?drain_style, updates = idx, "draining stash in upsert");

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
            cs.ensure_decoded(bincode_opts, source_config.id);
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
                if let Some(old_value) = existing_value.replace(StateValue::finalized_value(
                    value.clone(),
                    Some(from_time.0.clone()),
                )) {
                    if let Value::FinalizedValue(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts.clone(), Diff::MINUS_ONE));
                    }
                }
                output_updates.push((value, ts, Diff::ONE));
            }
            None => {
                if let Some(old_value) = existing_value.take() {
                    if let Value::FinalizedValue(old_value, _) = old_value.into_decoded() {
                        output_updates.push((old_value, ts, Diff::MINUS_ONE));
                    }
                }

                // Record a tombstone for deletes.
                *existing_value = Some(StateValue::tombstone(Some(from_time.0.clone())));
            }
        }
    }

    match state
        .multi_put(
            true, // Do update per-update stats.
            commands_state.drain(..).map(|(k, cv)| {
                (
                    k,
                    types::PutValue {
                        value: cv.value.map(|cv| cv.into_decoded()),
                        previous_value_metadata: cv.metadata.map(|v| ValueMetadata {
                            size: v.size.try_into().expect("less than i64 size"),
                            is_tombstone: v.is_tombstone,
                        }),
                    },
                )
            }),
        )
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

fn upsert_classic<G: Scope, FromTime, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
    state: F,
    upsert_config: UpsertConfig,
    prevent_snapshot_buffering: bool,
    snapshot_buffering_max: Option<usize>,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (Option<GlobalId>, HealthStatusUpdate)>,
    Stream<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder + Sync,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend<G::Timestamp, Option<FromTime>>,
    FromTime: timely::ExchangeData + Ord + Sync,
{
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    // We only care about UpsertValueError since this is the only error that we can retract
    let previous = previous.flat_map(move |result| {
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

    let mut previous = builder.new_input_for(
        &previous.inner,
        Exchange::new(|((key, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);
    let shutdown_button = builder.build(move |caps| async move {
        let [mut output_cap, mut snapshot_cap, health_cap]: [_; 3] = caps.try_into().unwrap();

        // The order key of the `UpsertState` is `Option<FromTime>`, which implements `Default`
        // (as required for `consolidate_chunk`), with slightly more efficient serialization
        // than a default `Partitioned`.
        let mut state = UpsertState::<_, _, Option<FromTime>>::new(
            state().await,
            upsert_shared_metrics,
            &upsert_metrics,
            source_config.source_statistics.clone(),
            upsert_config.shrink_upsert_unused_buffers_by_ratio,
        );
        let mut events = vec![];
        let mut snapshot_upper = Antichain::from_elem(Timestamp::minimum());

        let mut stash = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);

        tracing::info!(
            ?resume_upper,
            ?snapshot_upper,
            "timely-{} upsert source {} starting rehydration",
            source_config.worker_id,
            source_config.id
        );
        // Read and consolidate the snapshot from the 'previous' input until it
        // reaches the `resume_upper`.
        while !PartialOrder::less_equal(&resume_upper, &snapshot_upper) {
            previous.ready().await;
            while let Some(event) = previous.next_sync() {
                match event {
                    AsyncEvent::Data(_cap, data) => {
                        events.extend(data.into_iter().filter_map(|((key, value), ts, diff)| {
                            if !resume_upper.less_equal(&ts) {
                                Some((key, value, diff))
                            } else {
                                None
                            }
                        }))
                    }
                    AsyncEvent::Progress(upper) => {
                        snapshot_upper = upper;
                    }
                };
            }

            match state
                .consolidate_chunk(
                    events.drain(..),
                    PartialOrder::less_equal(&resume_upper, &snapshot_upper),
                )
                .await
            {
                Ok(_) => {
                    if let Some(ts) = snapshot_upper.clone().into_option() {
                        // As we shutdown, we could ostensibly get data from later than the
                        // `resume_upper`, which we ignore above. We don't want our output capability to make
                        // it further than the `resume_upper`.
                        if !resume_upper.less_equal(&ts) {
                            snapshot_cap.downgrade(&ts);
                            output_cap.downgrade(&ts);
                        }
                    }
                }
                Err(e) => {
                    UpsertErrorEmitter::<G>::emit(
                        &mut error_emitter,
                        "Failed to rehydrate state".to_string(),
                        e,
                    )
                    .await;
                }
            }
        }

        drop(events);
        drop(previous_token);
        drop(snapshot_cap);

        // Exchaust the previous input. It is expected to immediately reach the empty
        // antichain since we have dropped its token.
        //
        // Note that we do not need to also process the `input` during this, as the dropped token
        // will shutdown the `backpressure` operator
        while let Some(_event) = previous.next().await {}

        // After snapshotting, our output frontier is exactly the `resume_upper`
        if let Some(ts) = resume_upper.as_option() {
            output_cap.downgrade(ts);
        }

        tracing::info!(
            "timely-{} upsert source {} finished rehydration",
            source_config.worker_id,
            source_config.id
        );

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<
            _,
            types::UpsertValueAndSize<G::Timestamp, Option<FromTime>>,
        > = indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // Now can can resume consuming the collection
        let mut output_updates = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        while let Some(event) = input.next().await {
            // Buffer as many events as possible. This should be bounded, as new data can't be
            // produced in this worker until we yield to timely.
            let events = [event]
                .into_iter()
                .chain(std::iter::from_fn(|| input.next().now_or_never().flatten()))
                .enumerate();

            let mut partial_drain_time = None;
            for (i, event) in events {
                match event {
                    AsyncEvent::Data(cap, mut data) => {
                        tracing::trace!(
                            time=?cap.time(),
                            updates=%data.len(),
                            "received data in upsert"
                        );
                        stage_input(
                            &mut stash,
                            &mut data,
                            &input_upper,
                            &resume_upper,
                            upsert_config.shrink_upsert_unused_buffers_by_ratio,
                        );

                        let event_time = cap.time();
                        // If the data is at _exactly_ the output frontier, we can preemptively drain it into the state.
                        // Data within this set events strictly beyond this time are staged as
                        // normal.
                        //
                        // This is a load-bearing optimization, as it is required to avoid buffering
                        // the entire source snapshot in the `stash`.
                        if prevent_snapshot_buffering && output_cap.time() == event_time {
                            partial_drain_time = Some(event_time.clone());
                        }
                    }
                    AsyncEvent::Progress(upper) => {
                        tracing::trace!(?upper, "received progress in upsert");
                        // Ignore progress updates before the `resume_upper`, which is our initial
                        // capability post-snapshotting.
                        if PartialOrder::less_than(&upper, &resume_upper) {
                            continue;
                        }

                        // Disable the partial drain as this progress event covers
                        // the `output_cap` time.
                        partial_drain_time = None;
                        drain_staged_input::<_, G, _, _, _>(
                            &mut stash,
                            &mut commands_state,
                            &mut output_updates,
                            &mut multi_get_scratch,
                            DrainStyle::ToUpper(&upper),
                            &mut error_emitter,
                            &mut state,
                            &source_config,
                        )
                        .await;

                        output_handle.give_container(&output_cap, &mut output_updates);

                        if let Some(ts) = upper.as_option() {
                            output_cap.downgrade(ts);
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

            // If there were staged events that occurred at the capability time, drain
            // them. This is safe because out-of-order updates to the same key that are
            // drained in separate calls to `drain_staged_input` are correctly ordered by
            // their `FromTime` in `drain_staged_input`.
            //
            // Note also that this may result in more updates in the output collection than
            // the minimum. However, because the frontier only advances on `Progress` updates,
            // the collection always accumulates correctly for all keys.
            if let Some(partial_drain_time) = partial_drain_time {
                drain_staged_input::<_, G, _, _, _>(
                    &mut stash,
                    &mut commands_state,
                    &mut output_updates,
                    &mut multi_get_scratch,
                    DrainStyle::AtTime(partial_drain_time),
                    &mut error_emitter,
                    &mut state,
                    &source_config,
                )
                .await;

                output_handle.give_container(&output_cap, &mut output_updates);
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
            CapacityContainerBuilder<Vec<(Option<GlobalId>, HealthStatusUpdate)>>,
            Tee<<G as ScopeParent>::Timestamp, Vec<(Option<GlobalId>, HealthStatusUpdate)>>,
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
        CapacityContainerBuilder<Vec<(Option<GlobalId>, HealthStatusUpdate)>>,
        Tee<<G as ScopeParent>::Timestamp, Vec<(Option<GlobalId>, HealthStatusUpdate)>>,
    >,
    health_cap: &Capability<<G as ScopeParent>::Timestamp>,
) {
    let update = HealthStatusUpdate::halting(e.context(context).to_string_with_causes(), None);
    health_output.give(health_cap, (None, update));
    std::future::pending::<()>().await;
    unreachable!("pending future never returns");
}
