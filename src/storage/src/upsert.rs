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

use differential_dataflow::{AsCollection, VecCollection};
use itertools::Itertools;
use mz_repr::{Datum, DatumVec, Diff, GlobalId, Row};
use mz_storage_operators::metrics::BackpressureMetrics;
use mz_storage_types::errors::{DataflowError, UpsertError};
use mz_storage_types::sources::envelope::UpsertEnvelope;
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{InputCapability, Operator};
use timely::dataflow::{Scope, StreamVec};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::upsert_continual_feedback;

pub type UpsertValue = Result<Row, Box<UpsertError>>;

#[derive(Copy, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UpsertKey([u8; 32]);

impl Debug for UpsertKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for byte in self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl AsRef<[u8]> for UpsertKey {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for UpsertKey {
    fn from(bytes: &[u8]) -> Self {
        UpsertKey(bytes.try_into().expect("invalid key length"))
    }
}

type KeyHash = Sha256;

impl UpsertKey {
    pub fn from_key(key: Result<&Row, &UpsertError>) -> Self {
        Self::from_iter(key.map(|r| r.iter()))
    }

    pub fn from_value(value: Result<&Row, &UpsertError>, key_indices: &[usize]) -> Self {
        thread_local! {
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
            static KEY_DATUMS: RefCell<DatumVec> = RefCell::new(DatumVec::new());
        }
        KEY_DATUMS.with(|key_datums| {
            let mut key_datums = key_datums.borrow_mut();
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
use futures::StreamExt;

/// This leaf operator drops `token` after the input reaches the `resume_upper`.
pub fn rehydration_finished<G, T>(
    scope: G,
    source_config: &crate::source::RawSourceCreationConfig,
    token: impl std::any::Any + 'static,
    resume_upper: Antichain<T>,
    input: StreamVec<G, Infallible>,
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

/// Entry point: build the upsert dataflow subgraph.
pub(crate) fn upsert<G: Scope, FromTime>(
    input: VecCollection<G, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    upsert_envelope: UpsertEnvelope,
    resume_upper: Antichain<G::Timestamp>,
    previous: VecCollection<G, Result<Row, DataflowError>, Diff>,
    previous_token: Option<Vec<PressOnDropButton>>,
    source_config: crate::source::SourceExportCreationConfig,
    backpressure_metrics: Option<BackpressureMetrics>,
) -> (
    VecCollection<G, Result<Row, DataflowError>, Diff>,
    StreamVec<G, (Option<GlobalId>, HealthStatusUpdate)>,
    StreamVec<G, Infallible>,
    PressOnDropButton,
)
where
    G::Timestamp: TotalOrder + Sync,
    G::Timestamp: Refines<mz_repr::Timestamp> + TotalOrder + differential_dataflow::lattice::Lattice + Sync,
    FromTime: Timestamp + Clone + Sync,
{
    let upsert_metrics = source_config.metrics.get_upsert_metrics(
        source_config.id,
        source_config.worker_id,
        backpressure_metrics,
    );

    let thin_input = upsert_thinning(input);

    tracing::info!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        "rendering upsert source (btreemap backend)"
    );

    upsert_continual_feedback::upsert_inner(
        thin_input,
        upsert_envelope.key_indices,
        resume_upper,
        previous,
        previous_token,
        upsert_metrics,
        source_config,
    )
}

/// Pre-exchange thinning: for each (key, time) keep only the update with the
/// highest from_time, discarding the rest.
fn upsert_thinning<G, K, V, FromTime>(
    input: VecCollection<G, (K, V, FromTime), Diff>,
) -> VecCollection<G, (K, V, FromTime), Diff>
where
    G: Scope,
    G::Timestamp: TotalOrder,
    K: timely::ExchangeData + Clone + Eq + Ord,
    V: timely::ExchangeData + Clone,
    FromTime: Timestamp,
{
    input
        .inner
        .unary(Pipeline, "UpsertThinning", |_, _| {
            let mut capability: Option<InputCapability<G::Timestamp>> = None;
            let mut updates = Vec::new();
            move |input, output| {
                input.for_each(|cap, data| {
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
                });
                if let Some(capability) = capability.take() {
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
