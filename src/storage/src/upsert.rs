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

use differential_dataflow::{AsCollection, Collection};
use futures::StreamExt;
use itertools::Itertools;
use mz_ore::error::ErrorExt;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_rocksdb::ValueIterator;
use mz_storage_operators::metrics::BackpressureMetrics;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::dyncfgs;
use mz_storage_types::errors::{DataflowError, UpsertError};
use mz_storage_types::sources::envelope::UpsertEnvelope;
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
use autospill::AutoSpillBackend;
use memory::InMemoryHashMap;
use types::{
    consolidating_merge_function, upsert_bincode_opts, BincodeOpts, StateValue,
    UpsertStateBackend,
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
            worker_id = %source_config.worker_id,
            source_id = %source_config.id,
            ?tuning,
            ?storage_configuration.parameters.upsert_auto_spill_config,
            ?rocksdb_use_native_merge_operator,
            "rendering upsert source with rocksdb-backed upsert state"
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
            let merge_operator =
                if rocksdb_use_native_merge_operator {
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
            worker_id = %source_config.worker_id,
            source_id = %source_config.id,
            "rendering upsert source with memory-backed upsert state",
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
    // Since we've removed the classic implementation, we can just directly call the continual feedback implementation
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
                        data.iter().all(|(_, _, diff)| *diff > 0),
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

// Created a struct to hold the configs for upserts.
// So that new configs don't require a new method parameter.
pub(crate) struct UpsertConfig {
    pub shrink_upsert_unused_buffers_by_ratio: usize,
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
