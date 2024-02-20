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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use differential_dataflow::consolidation;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::{AsCollection, Collection};
use futures::future::FutureExt;
use itertools::Itertools;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, DatumVec, Diff, Row};
use mz_storage_operators::metrics::BackpressureMetrics;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_storage_types::sources::envelope::UpsertEnvelope;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pushers::TeeCore;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, ScopeParent, Stream};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::render::sources::OutputIndex;
use crate::render::upsert::autospill::{AutoSpillBackend, RocksDBParams};
use crate::render::upsert::memory::InMemoryHashMap;
use crate::render::upsert::types::{upsert_bincode_opts, UpsertState, UpsertStateBackend};
use crate::storage_state::StorageInstanceContext;

mod autospill;
mod memory;
mod rocksdb;
mod types;

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

thread_local! {
    /// A thread-local datum cache used to calculate hashes
    pub static KEY_DATUMS: RefCell<DatumVec> = RefCell::new(DatumVec::new());
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

    pub fn from_value(value: Result<&Row, &UpsertError>, mut key_indices: &[usize]) -> Self {
        Self::from_iter(value.map(|value| {
            value.iter().enumerate().flat_map(move |(idx, datum)| {
                let key_idx = key_indices.get(0)?;
                if idx == *key_idx {
                    key_indices = &key_indices[1..];
                    Some(datum)
                } else {
                    None
                }
            })
        }))
    }

    pub fn from_iter<'a, 'b>(
        key: Result<impl Iterator<Item = Datum<'a>> + 'b, &UpsertError>,
    ) -> Self {
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
pub(crate) fn upsert<G: Scope, O: timely::ExchangeData + Ord>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, O), Diff>,
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
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
{
    let upsert_metrics = source_config.metrics.get_upsert_metrics(
        source_config.id,
        source_config.worker_id,
        backpressure_metrics,
    );

    // If we are configured to delay raw sources till we rehydrate, we do so. Otherwise, skip
    // this, to prevent unnecessary work.
    let wait_for_input_resumption = storage_configuration
        .parameters
        .delay_sources_past_rehydration;
    let upsert_config = UpsertConfig {
        wait_for_input_resumption,
        shrink_upsert_unused_buffers_by_ratio: storage_configuration
            .parameters
            .shrink_upsert_unused_buffers_by_ratio,
    };

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
        let legacy_rocksdb_dir = scratch_directory
            .join(source_config.id.to_string())
            .join(source_config.worker_id.to_string());

        let env = instance_context.rocksdb_env.clone();

        let rocksdb_in_use_metric = Arc::clone(&upsert_metrics.rocksdb_autospill_in_use);

        if allow_auto_spill {
            upsert_inner(
                input,
                upsert_envelope.key_indices,
                resume_upper,
                previous,
                previous_token,
                upsert_metrics,
                source_config,
                move || async move {
                    AutoSpillBackend::new(
                        RocksDBParams {
                            instance_path: rocksdb_dir,
                            legacy_instance_path: legacy_rocksdb_dir,
                            env,
                            tuning_config: tuning,
                            shared_metrics: rocksdb_shared_metrics,
                            instance_metrics: rocksdb_instance_metrics,
                        },
                        spill_threshold,
                        rocksdb_in_use_metric,
                    )
                },
                upsert_config,
            )
        } else {
            upsert_inner(
                input,
                upsert_envelope.key_indices,
                resume_upper,
                previous,
                previous_token,
                upsert_metrics,
                source_config,
                move || async move {
                    rocksdb::RocksDB::new(
                        mz_rocksdb::RocksDBInstance::new(
                            &rocksdb_dir,
                            &legacy_rocksdb_dir,
                            mz_rocksdb::InstanceOptions::defaults_with_env(env),
                            tuning,
                            rocksdb_shared_metrics,
                            rocksdb_instance_metrics,
                            // For now, just use the same config as the one used for
                            // merging snapshots.
                            upsert_bincode_opts(),
                        )
                        .await
                        .unwrap(),
                    )
                },
                upsert_config,
            )
        }
    } else {
        tracing::info!(
            "timely-{} rendering {} with memory-backed upsert state",
            source_config.worker_id,
            source_config.id
        );
        upsert_inner(
            input,
            upsert_envelope.key_indices,
            resume_upper,
            previous,
            previous_token,
            upsert_metrics,
            source_config,
            || async { InMemoryHashMap::default() },
            upsert_config,
        )
    }
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
fn stage_input<T, O>(
    stash: &mut Vec<(T, UpsertKey, Reverse<O>, Option<UpsertValue>)>,
    data: &mut Vec<((UpsertKey, Option<UpsertValue>, O), T, Diff)>,
    input_upper: &Antichain<T>,
    resume_upper: &Antichain<T>,
    storage_shrink_upsert_unused_buffers_by_ratio: usize,
) where
    T: PartialOrder,
    O: Ord,
{
    if PartialOrder::less_equal(input_upper, resume_upper) {
        data.retain(|(_, ts, _)| resume_upper.less_equal(ts));
    }

    stash.extend(data.drain(..).map(|((key, value, order), time, diff)| {
        assert!(diff > 0, "invalid upsert input");
        (time, key, Reverse(order), value)
    }));

    if storage_shrink_upsert_unused_buffers_by_ratio > 0 {
        let reduced_capacity = stash.capacity() / storage_shrink_upsert_unused_buffers_by_ratio;
        if reduced_capacity > stash.len() {
            stash.shrink_to(reduced_capacity);
        }
    }
}

/// Helper method for `upsert_inner` used to stage `data` updates
/// from the input timely edge.
async fn drain_staged_input<S, G, T, O, E>(
    stash: &mut Vec<(T, UpsertKey, Reverse<O>, Option<UpsertValue>)>,
    commands_state: &mut indexmap::IndexMap<UpsertKey, types::UpsertValueAndSize>,
    output_updates: &mut Vec<(Result<Row, UpsertError>, T, Diff)>,
    multi_get_scratch: &mut Vec<UpsertKey>,
    upper: &Antichain<T>,
    error_emitter: &mut E,
    state: &mut UpsertState<'_, S>,
) where
    S: UpsertStateBackend,
    G: Scope,
    T: PartialOrder + Ord + Clone,
    O: Ord,
    E: UpsertErrorEmitter<G>,
{
    stash.sort_unstable();

    // Find the prefix that we can emit
    let idx = stash.partition_point(|(ts, _, _, _)| !upper.less_equal(ts));

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
    // key) group. This is achieved by wrapping order in `Reverse(order)` above.;
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
    // `merge_snapshot_chunk` implementation, minimizing gets and puts on
    // the `UpsertStateBackend` implementations. In some sense, its "upsert all the way down".
    while let Some((ts, key, _, value)) = commands.next() {
        let command_state = commands_state
            .get_mut(&key)
            .expect("key missing from commands_state");

        if let Some(cs) = command_state.value.as_mut() {
            cs.ensure_decoded(bincode_opts);
        }

        match value {
            Some(value) => {
                if let Some(old_value) = command_state.value.replace(value.clone().into()) {
                    output_updates.push((old_value.to_decoded(), ts.clone(), -1));
                }
                output_updates.push((value, ts, 1));
            }
            None => {
                if let Some(old_value) = command_state.value.take() {
                    output_updates.push((old_value.to_decoded(), ts, -1));
                }
            }
        }
    }

    match state
        .multi_put(commands_state.drain(..).map(|(k, cv)| {
            (
                k,
                types::PutValue {
                    value: cv.value.map(|cv| cv.to_decoded()),
                    previous_persisted_size: cv
                        .size
                        .map(|v| v.try_into().expect("less than i64 size")),
                },
            )
        }))
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
struct UpsertConfig {
    // Whether or not to wait for the `input` to reach the `resumption_frontier`
    // before we finalize `rehydration`.
    wait_for_input_resumption: bool,
    shrink_upsert_unused_buffers_by_ratio: usize,
}

fn upsert_inner<G: Scope, O: timely::ExchangeData + Ord, F, Fut, US>(
    input: &Collection<G, (UpsertKey, Option<UpsertValue>, O), Diff>,
    mut key_indices: Vec<usize>,
    resume_upper: Antichain<G::Timestamp>,
    previous: Collection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::RawSourceCreationConfig,
    state: F,
    upsert_config: UpsertConfig,
) -> (
    Collection<G, Result<Row, DataflowError>, Diff>,
    Stream<G, (OutputIndex, HealthStatusUpdate)>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder,
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = US>,
    US: UpsertStateBackend,
{
    // Sort key indices to ensure we can construct the key by iterating over the datums of the row
    key_indices.sort_unstable();

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
    let (mut output_handle, output) = builder.new_output();
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
        let [mut output_cap, health_cap]: [_; 2] = caps.try_into().unwrap();

        let mut state = UpsertState::new(
            state().await,
            upsert_shared_metrics,
            &upsert_metrics,
            source_config.source_statistics,
            upsert_config.shrink_upsert_unused_buffers_by_ratio,
        );
        let mut events = vec![];
        let mut snapshot_upper = Antichain::from_elem(Timestamp::minimum());

        let mut stash = vec![];
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());
        let mut legacy_errors_to_correct = vec![];

        let mut error_emitter = (&mut health_output, &health_cap);

        while !PartialOrder::less_equal(&resume_upper, &snapshot_upper)
            || (upsert_config.wait_for_input_resumption
                && !PartialOrder::less_equal(&resume_upper, &input_upper))
        {
            let previous_event = tokio::select! {
                // Note that these are both cancel-safe. The reason we drain the `input` is to
                // ensure the `output_frontier` (and therefore flow control on `previous`) make
                // progress.
                previous_event = previous.next(), if !PartialOrder::less_equal(
                    &resume_upper,
                    &snapshot_upper,
                ) => {
                    previous_event
                }
                input_event = input.next() => {
                    match input_event {
                        Some(AsyncEvent::Data(_cap, mut data)) => {
                            stage_input(
                                &mut stash,
                                &mut data,
                                &input_upper,
                                &resume_upper,
                                upsert_config.shrink_upsert_unused_buffers_by_ratio
                            );
                        }
                        Some(AsyncEvent::Progress(upper)) => {
                            input_upper = upper;
                        }
                        None => {
                            input_upper = Antichain::new();
                        }
                    }
                    continue;
                }
            };
            match previous_event {
                Some(AsyncEvent::Data(_cap, data)) => {
                    events.extend(data.into_iter().filter_map(|((key, value), ts, diff)| {
                        if !resume_upper.less_equal(&ts) {
                            Some((key, value, diff))
                        } else {
                            None
                        }
                    }))
                }
                Some(AsyncEvent::Progress(upper)) => snapshot_upper = upper,
                None => snapshot_upper = Antichain::new(),
            };
            while let Some(event) = previous.next().now_or_never() {
                match event {
                    Some(AsyncEvent::Data(_cap, data)) => {
                        events.extend(data.into_iter().filter_map(|((key, value), ts, diff)| {
                            if !resume_upper.less_equal(&ts) {
                                Some((key, value, diff))
                            } else {
                                None
                            }
                        }))
                    }
                    Some(AsyncEvent::Progress(upper)) => snapshot_upper = upper,
                    None => {
                        snapshot_upper = Antichain::new();
                        break;
                    }
                }
            }

            for (_, value, diff) in events.iter_mut() {
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

            match state
                .merge_snapshot_chunk(
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
        for (mut error, diff) in legacy_errors_to_correct {
            assert!(
                error.is_legacy_dont_touch_it,
                "attempted to correct non-legacy error"
            );
            tracing::info!("correcting legacy error {error:?} with diff {diff}");
            let time = output_cap.time().clone();
            let retraction = Err(UpsertError::Value(error.clone()));
            error.is_legacy_dont_touch_it = false;
            let insertion = Err(UpsertError::Value(error));
            output_handle
                .give(&output_cap, (retraction, time.clone(), -diff))
                .await;
            output_handle
                .give(&output_cap, (insertion, time, diff))
                .await;
        }

        tracing::info!(
            "timely-{} upsert source {} finished rehydration",
            source_config.worker_id,
            source_config.id
        );

        // A re-usable buffer of changes, per key. This is an `IndexMap` because it has to be `drain`-able
        // and have a consistent iteration order.
        let mut commands_state: indexmap::IndexMap<_, types::UpsertValueAndSize> =
            indexmap::IndexMap::new();
        let mut multi_get_scratch = Vec::new();

        // Now can can resume consuming the collection
        let mut output_updates = vec![];
        let mut post_snapshot = true;
        while let Some(event) = {
            // Synthesize a `Progress` event that allows us to drain the `stash` of values
            // obtained during snapshotting.
            if post_snapshot {
                post_snapshot = false;
                Some(AsyncEvent::Progress(input_upper.clone()))
            } else {
                input.next().await
            }
        } {
            match event {
                AsyncEvent::Data(cap, mut data) if output_cap.time().less_equal(cap.time()) => {
                    stage_input(
                        &mut stash,
                        &mut data,
                        &input_upper,
                        &resume_upper,
                        upsert_config.shrink_upsert_unused_buffers_by_ratio,
                    );
                }
                AsyncEvent::Data(_cap, mut data) => {
                    stage_input(
                        &mut stash,
                        &mut data,
                        &input_upper,
                        &resume_upper,
                        upsert_config.shrink_upsert_unused_buffers_by_ratio,
                    );
                }
                AsyncEvent::Progress(upper) => {
                    // Ignore progress updates before the `resume_upper`, which is our initial
                    // capability post-snapshotting.
                    if PartialOrder::less_than(&upper, &resume_upper) {
                        continue;
                    }

                    drain_staged_input::<_, G, _, _, _>(
                        &mut stash,
                        &mut commands_state,
                        &mut output_updates,
                        &mut multi_get_scratch,
                        &upper,
                        &mut error_emitter,
                        &mut state,
                    )
                    .await;

                    // Emit the _consolidated_ changes to the output.
                    output_handle
                        .give_container(&output_cap, &mut output_updates)
                        .await;
                    if let Some(ts) = upper.as_option() {
                        output_cap.downgrade(ts);
                    }
                    input_upper = upper;
                }
            }
        }
    });

    (
        output.as_collection().map(|result| match result {
            Ok(ok) => Ok(ok),
            Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(err))),
        }),
        health_stream,
        shutdown_button.press_on_drop(),
    )
}

#[async_trait::async_trait(?Send)]
trait UpsertErrorEmitter<G> {
    async fn emit(&mut self, context: String, e: anyhow::Error);
}

#[async_trait::async_trait(?Send)]
impl<G: Scope> UpsertErrorEmitter<G>
    for (
        &mut AsyncOutputHandle<
            <G as ScopeParent>::Timestamp,
            Vec<(OutputIndex, HealthStatusUpdate)>,
            TeeCore<<G as ScopeParent>::Timestamp, Vec<(OutputIndex, HealthStatusUpdate)>>,
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
    health_output: &mut AsyncOutputHandle<
        <G as ScopeParent>::Timestamp,
        Vec<(OutputIndex, HealthStatusUpdate)>,
        TeeCore<<G as ScopeParent>::Timestamp, Vec<(OutputIndex, HealthStatusUpdate)>>,
    >,
    health_cap: &Capability<<G as ScopeParent>::Timestamp>,
) {
    let update = HealthStatusUpdate::halting(e.context(context).to_string_with_causes(), None);
    health_output.give(health_cap, (0, update)).await;
    std::future::pending::<()>().await;
    unreachable!("pending future never returns");
}
