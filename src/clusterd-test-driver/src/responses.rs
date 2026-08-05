// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Owns the receive side of the CTP connection: a background task that pumps
//! `ComputeResponse`s into per-id frontier watches, per-uuid peek channels, and
//! a raw broadcast. The mechanism does not curate which responses or which
//! frontier fields a use case may observe: frontier watches keep the full
//! merged `FrontiersResponse`, and the raw broadcast carries every response.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::{
    ComputeResponse, FrontiersResponse, PeekResponse, SubscribeResponse,
};
use mz_repr::{GlobalId, Row, Timestamp};
use mz_service::client::GenericClient;
use timely::progress::Antichain;
use tokio::sync::{broadcast, oneshot, watch};

use crate::ctp::ComputeCtpClient;

type FrontierTx = watch::Sender<FrontiersResponse>;
type FrontierRx = watch::Receiver<FrontiersResponse>;

/// The first error a subscribe reported, and the batch it arrived in.
///
/// A subscribe is *poisoned* by its first error: `SubscribeProtocol` records it and
/// returns the same error in every later batch, whether or not the error is
/// retracted afterwards. An error is therefore a property of the subscribe from
/// some point onwards, not of the collection at one timestamp, and a caller
/// comparing a subscribe against another export has to account for that or it will
/// read intended behaviour as a divergence.
///
/// The batch bounds are kept because the protocol's `Err` variant carries no
/// timestamps. All that is known is that the error belongs to some timestamp in
/// `[lower, upper)`: a replica is free to batch several timestamps together, and
/// then which one first errored is not recoverable from the response.
#[derive(Debug, Clone)]
pub struct SubscribePoison {
    /// The lower bound of the batch carrying the first error.
    pub lower: Option<Timestamp>,
    /// The upper bound of that batch. `None` for an empty (final) upper.
    pub upper: Option<Timestamp>,
    /// The error, as the replica spelled it.
    pub message: String,
}

/// What a subscribe produced: its accumulated updates and its poison, if any.
#[derive(Debug, Clone, Default)]
pub struct SubscribeOutcome {
    /// Updates from every batch before the subscribe was poisoned.
    pub updates: Vec<(Row, Timestamp, i64)>,
    /// The first error, if the subscribe was poisoned.
    pub poison: Option<SubscribePoison>,
}

/// Buffered state for one subscribe sink: its accumulated updates, a watch on its
/// upper frontier (so a waiter can block until it reaches a target), and its poison.
///
/// Subscribe batches arrive asynchronously and out of band of the command that
/// created the sink, so the pump accumulates them here as they land; the
/// `await-subscribe` command drains them once the upper reaches its target.
struct SubscribeState {
    updates: Vec<(Row, Timestamp, i64)>,
    upper_tx: watch::Sender<Antichain<Timestamp>>,
    poison: Option<SubscribePoison>,
}

impl SubscribeState {
    fn new() -> Self {
        SubscribeState {
            updates: Vec::new(),
            upper_tx: watch::channel(Antichain::from_elem(Timestamp::default())).0,
            poison: None,
        }
    }
}

struct Shared {
    frontiers: BTreeMap<GlobalId, FrontierTx>,
    peeks: BTreeMap<uuid::Uuid, oneshot::Sender<PeekResponse>>,
    subscribes: BTreeMap<GlobalId, SubscribeState>,
    raw: broadcast::Sender<ComputeResponse>,
}

/// Handle to the response side. Cloneable view onto frontier watches, peek
/// routing, and the raw response broadcast.
#[derive(Clone)]
pub struct Responses {
    shared: Arc<Mutex<Shared>>,
}

impl Responses {
    /// Spawns the pump task that owns the client's receive half.
    pub fn spawn(mut client: ComputeCtpClient) -> (Self, ComputeSender) {
        let (raw_tx, _) = broadcast::channel(1024);
        let shared = Arc::new(Mutex::new(Shared {
            frontiers: BTreeMap::new(),
            peeks: BTreeMap::new(),
            subscribes: BTreeMap::new(),
            raw: raw_tx,
        }));
        let pump_shared = Arc::clone(&shared);
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<ComputeCommand>();
        mz_ore::task::spawn(|| "compute_response_pump", async move {
            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => match cmd {
                        // Log a send failure: callers waiting on frontiers/peeks
                        // would otherwise see only a misleading timeout.
                        Some(cmd) => {
                            if let Err(e) = client.send(cmd).await {
                                tracing::error!("compute command send failed: {e}");
                                break;
                            }
                        }
                        None => break,
                    },
                    resp = client.recv() => match resp {
                        Ok(Some(resp)) => Self::dispatch(&pump_shared, resp),
                        // Distinguish a clean EOF from a transport error so that
                        // an e2e hang has a breadcrumb rather than silent death.
                        Ok(None) => {
                            tracing::warn!("clusterd closed the compute connection");
                            break;
                        }
                        Err(e) => {
                            tracing::error!("compute response recv failed: {e}");
                            break;
                        }
                    },
                }
            }
        });
        (Responses { shared }, ComputeSender { tx: cmd_tx })
    }

    fn dispatch(shared: &Arc<Mutex<Shared>>, resp: ComputeResponse) {
        let mut g = shared.lock().expect("lock");
        let _ = g.raw.send(resp.clone());
        match resp {
            ComputeResponse::Frontiers(id, f) => {
                let tx = g
                    .frontiers
                    .entry(id)
                    .or_insert_with(|| watch::channel(FrontiersResponse::default()).0);
                let mut cur = tx.borrow().clone();
                if f.write_frontier.is_some() {
                    cur.write_frontier = f.write_frontier;
                }
                if f.input_frontier.is_some() {
                    cur.input_frontier = f.input_frontier;
                }
                if f.output_frontier.is_some() {
                    cur.output_frontier = f.output_frontier;
                }
                // `send_replace`, not `send`: `send` fails when no receiver
                // exists yet and, crucially, leaves the stored value unchanged.
                // Responses routinely arrive before anything awaits them (a
                // dataflow can hydrate while the caller is still reading another
                // export), and dropping those updates makes a later waiter block
                // on a frontier the replica already reported. `send_replace`
                // always stores, so a late subscriber sees the latest value.
                let _ = tx.send_replace(cur);
            }
            ComputeResponse::PeekResponse(uuid, pr, _otel) => {
                if let Some(tx) = g.peeks.remove(&uuid) {
                    let _ = tx.send(pr);
                }
            }
            ComputeResponse::SubscribeResponse(id, sr) => {
                let state = g.subscribes.entry(id).or_insert_with(SubscribeState::new);
                let upper = match sr {
                    SubscribeResponse::Batch(batch) => {
                        match batch.updates {
                            Ok(collections) => {
                                for collection in collections {
                                    for (row, ts, diff) in collection.iter() {
                                        state.updates.push((
                                            row.to_owned(),
                                            *ts,
                                            diff.into_inner(),
                                        ));
                                    }
                                }
                            }
                            // Record the first error along with the batch it arrived
                            // in, which is as precisely as it can be placed in time.
                            // Later batches repeat it (the sink is poisoned), so only
                            // the first one carries information.
                            Err(e) => {
                                if state.poison.is_none() {
                                    state.poison = Some(SubscribePoison {
                                        lower: batch.lower.as_option().copied(),
                                        upper: batch.upper.as_option().copied(),
                                        message: e,
                                    });
                                }
                            }
                        }
                        batch.upper
                    }
                    // A drop leaves later updates unspecified; treat the drop frontier
                    // as the final upper so a waiter unblocks.
                    SubscribeResponse::DroppedAt(frontier) => frontier,
                };
                // `send_replace` for the same reason as the frontier above: a
                // subscribe that completes before `await_subscribe` is called
                // would otherwise leave its final upper unrecorded, and the
                // waiter would block forever on an already-finished subscribe.
                let _ = state.upper_tx.send_replace(upper);
            }
            _ => {}
        }
    }

    /// Returns a watch receiver for an id's full (merged) frontiers, created
    /// lazily. Use cases read whichever of write/input/output they need.
    pub fn frontier(&self, id: GlobalId) -> FrontierRx {
        let mut g = self.shared.lock().expect("lock");
        g.frontiers
            .entry(id)
            .or_insert_with(|| watch::channel(FrontiersResponse::default()).0)
            .subscribe()
    }

    /// Subscribes to every `ComputeResponse` the replica sends.
    pub fn subscribe_raw(&self) -> broadcast::Receiver<ComputeResponse> {
        self.shared.lock().expect("lock").raw.subscribe()
    }

    /// Registers interest in a peek's response before the Peek command is sent.
    pub fn register_peek(&self, uuid: uuid::Uuid) -> oneshot::Receiver<PeekResponse> {
        let (tx, rx) = oneshot::channel();
        self.shared.lock().expect("lock").peeks.insert(uuid, tx);
        rx
    }

    /// Ensures a subscribe buffer exists for `id` and returns a watch receiver for
    /// its upper frontier, created lazily. Call this before scheduling the sink so
    /// the upper watch is observable; the pump accumulates batches regardless.
    pub fn ensure_subscribe(&self, id: GlobalId) -> watch::Receiver<Antichain<Timestamp>> {
        let mut g = self.shared.lock().expect("lock");
        g.subscribes
            .entry(id)
            .or_insert_with(SubscribeState::new)
            .upper_tx
            .subscribe()
    }

    /// Drains the buffered updates for subscribe `id`, returning them as
    /// `(row, time, diff)` triples. Errors if the replica reported a subscribe
    /// error (e.g. a result-size overflow), so the assertion fails loudly rather
    /// than on a silently truncated batch.
    pub fn drain_subscribe(&self, id: GlobalId) -> anyhow::Result<Vec<(Row, Timestamp, i64)>> {
        let outcome = self.drain_subscribe_result(id)?;
        if let Some(poison) = outcome.poison {
            anyhow::bail!("subscribe {id} reported an error: {}", poison.message);
        }
        Ok(outcome.updates)
    }

    /// Like [`Self::drain_subscribe`], but reports a subscribe error as a value
    /// rather than as a failure, with the batch bounds needed to place it in time.
    ///
    /// A caller comparing against a reference needs to see the error: a computation
    /// over erroring input *should* error. See [`SubscribePoison`] for why it comes
    /// with bounds rather than as a plain message.
    pub fn drain_subscribe_result(&self, id: GlobalId) -> anyhow::Result<SubscribeOutcome> {
        let mut g = self.shared.lock().expect("lock");
        let state = g
            .subscribes
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("no subscribe registered for {id}"))?;
        Ok(SubscribeOutcome {
            updates: std::mem::take(&mut state.updates),
            poison: state.poison.clone(),
        })
    }
}

/// Send half: forwards commands into the pump task that owns the client.
#[derive(Clone)]
pub struct ComputeSender {
    tx: tokio::sync::mpsc::UnboundedSender<ComputeCommand>,
}

impl ComputeSender {
    pub fn send(&self, cmd: ComputeCommand) -> anyhow::Result<()> {
        self.tx
            .send(cmd)
            .map_err(|_| anyhow::anyhow!("pump task gone"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::Timestamp;
    use timely::progress::Antichain;

    fn empty_shared() -> Arc<Mutex<Shared>> {
        let (raw, _) = broadcast::channel(16);
        Arc::new(Mutex::new(Shared {
            frontiers: BTreeMap::new(),
            peeks: BTreeMap::new(),
            subscribes: BTreeMap::new(),
            raw,
        }))
    }

    #[mz_ore::test]
    fn dispatch_merges_frontier_and_broadcasts() {
        let shared = empty_shared();
        let id = GlobalId::User(1);
        let rx = {
            let mut g = shared.lock().unwrap();
            let (tx, rx) = watch::channel(FrontiersResponse::default());
            g.frontiers.insert(id, tx);
            rx
        };
        let mut raw_rx = shared.lock().unwrap().raw.subscribe();

        Responses::dispatch(
            &shared,
            ComputeResponse::Frontiers(
                id,
                FrontiersResponse {
                    output_frontier: Some(Antichain::from_elem(Timestamp::from(5))),
                    ..Default::default()
                },
            ),
        );
        assert_eq!(
            rx.borrow().output_frontier,
            Some(Antichain::from_elem(Timestamp::from(5)))
        );
        Responses::dispatch(
            &shared,
            ComputeResponse::Frontiers(
                id,
                FrontiersResponse {
                    input_frontier: Some(Antichain::from_elem(Timestamp::from(3))),
                    ..Default::default()
                },
            ),
        );
        assert_eq!(
            rx.borrow().output_frontier,
            Some(Antichain::from_elem(Timestamp::from(5)))
        );
        assert_eq!(
            rx.borrow().input_frontier,
            Some(Antichain::from_elem(Timestamp::from(3)))
        );
        assert!(raw_rx.try_recv().is_ok());
    }

    /// A frontier that arrives before anything subscribes must still be visible
    /// to a later subscriber.
    ///
    /// This is the ordering the existing tests miss: they create the receiver
    /// first, which is the easy case. A real run routinely dispatches a frontier
    /// while the caller is still reading a different export, and with
    /// `watch::Sender::send` that update was dropped on the floor (send fails
    /// with no receivers and leaves the value unchanged), so the later waiter
    /// blocked forever on a frontier the replica had already reported.
    #[mz_ore::test]
    fn frontier_dispatched_before_subscribe_is_retained() {
        let shared = empty_shared();
        let id = GlobalId::User(7);

        // Dispatch with no receiver in existence.
        Responses::dispatch(
            &shared,
            ComputeResponse::Frontiers(
                id,
                FrontiersResponse {
                    write_frontier: None,
                    input_frontier: None,
                    output_frontier: Some(Antichain::from_elem(Timestamp::from(5))),
                },
            ),
        );

        // Subscribe afterwards, as `expect_frontier` does.
        let responses = Responses { shared };
        let mut rx = responses.frontier(id);
        assert_eq!(
            rx.borrow_and_update().output_frontier,
            Some(Antichain::from_elem(Timestamp::from(5))),
            "a frontier reported before anyone subscribed must not be lost"
        );
    }

    /// The same ordering hazard for a subscribe's upper: a subscribe that
    /// completes before `await_subscribe` is called must still report its final
    /// upper, or the waiter blocks on an already-finished subscribe.
    #[mz_ore::test]
    fn subscribe_upper_dispatched_before_subscribe_is_retained() {
        use mz_compute_client::protocol::response::SubscribeBatch;

        let shared = empty_shared();
        let id = GlobalId::User(8);

        Responses::dispatch(
            &shared,
            ComputeResponse::SubscribeResponse(
                id,
                SubscribeResponse::Batch(SubscribeBatch {
                    lower: Antichain::from_elem(Timestamp::from(0)),
                    // The empty antichain: the subscribe completed.
                    upper: Antichain::new(),
                    updates: Ok(Vec::new()),
                }),
            ),
        );

        let responses = Responses { shared };
        let mut rx = responses.ensure_subscribe(id);
        assert!(
            rx.borrow_and_update().is_empty(),
            "a completed subscribe's final upper must not be lost"
        );
    }
}
