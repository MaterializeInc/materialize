// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Drains native owner and query responses in a background task that pumps
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
use timely::progress::Antichain;
use tokio::sync::{broadcast, oneshot, watch};

use crate::native::NativeCompute;

type FrontierTx = watch::Sender<FrontiersResponse>;
type FrontierRx = watch::Receiver<FrontiersResponse>;

/// Buffered state for one subscribe sink: its accumulated updates, a watch on its
/// upper frontier (so a waiter can block until it reaches a target), and the first
/// error the replica reported, if any.
///
/// Subscribe batches arrive asynchronously and out of band of the command that
/// created the sink, so the pump accumulates them here as they land; the
/// `await-subscribe` command drains them once the upper reaches its target.
struct SubscribeState {
    updates: Vec<(Row, Timestamp, i64)>,
    upper_tx: watch::Sender<Antichain<Timestamp>>,
    error: Option<String>,
}

impl SubscribeState {
    fn new() -> Self {
        SubscribeState {
            updates: Vec::new(),
            upper_tx: watch::channel(Antichain::from_elem(Timestamp::default())).0,
            error: None,
        }
    }
}

struct Shared {
    frontiers: BTreeMap<GlobalId, FrontierTx>,
    peeks: BTreeMap<uuid::Uuid, oneshot::Sender<PeekResponse>>,
    dataflows: BTreeMap<uuid::Uuid, oneshot::Sender<Result<(), String>>>,
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
    pub(crate) fn spawn(client: NativeCompute) -> (Self, ComputeSender) {
        // These owners have process lifetime. Dropping a failed/cancelled pump
        // must not join native workers that intentionally never return.
        let mut client = std::mem::ManuallyDrop::new(client);
        let (raw_tx, _) = broadcast::channel(1024);
        let shared = Arc::new(Mutex::new(Shared {
            frontiers: BTreeMap::new(),
            peeks: BTreeMap::new(),
            dataflows: BTreeMap::new(),
            subscribes: BTreeMap::new(),
            raw: raw_tx,
        }));
        let pump_shared = Arc::clone(&shared);
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<ComputeCommand>();
        mz_ore::task::spawn(|| "compute_response_pump", async move {
            let mut commands_open = true;
            loop {
                tokio::select! {
                    cmd = cmd_rx.recv(), if commands_open => match cmd {
                        // Log a send failure: callers waiting on frontiers/peeks
                        // would otherwise see only a misleading timeout.
                        Some(cmd) => {
                            if let Err(e) = client.send(cmd).await {
                                mz_ore::halt!("native driver command failed: {e}");
                            }
                        }
                        // Native owners live until process exit, not until the
                        // last query/script sender closes. Keep draining progress.
                        None => commands_open = false,
                    },
                    resp = client.recv() => match resp {
                        Ok(Some(resp)) => Self::dispatch(&pump_shared, resp),
                        // Distinguish a clean EOF from a transport error so that
                        // an e2e hang has a breadcrumb rather than silent death.
                        Ok(None) => {
                            mz_ore::halt!("native driver lost its compute connection");
                        }
                        Err(e) => {
                            mz_ore::halt!("native driver response failed: {e}");
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
            ComputeResponse::QueryDataflowResponse { request_id, error } => {
                if let Some(tx) = g.dataflows.remove(&request_id) {
                    let _ = tx.send(error.map_or(Ok(()), Err));
                }
            }
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
                tx.send_replace(cur);
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
                            // Record the first error; the size-limit / internal error
                            // path replaces the updates with a message.
                            Err(e) => {
                                if state.error.is_none() {
                                    state.error = Some(e);
                                }
                            }
                        }
                        batch.upper
                    }
                    // A drop leaves later updates unspecified; treat the drop frontier
                    // as the final upper so a waiter unblocks.
                    SubscribeResponse::DroppedAt(frontier) => frontier,
                };
                state.upper_tx.send_replace(upper);
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

    /// Register admission before sending the query dataflow to the runtime.
    pub fn register_dataflow(
        &self,
        request_id: uuid::Uuid,
    ) -> oneshot::Receiver<Result<(), String>> {
        let (tx, rx) = oneshot::channel();
        self.shared
            .lock()
            .expect("lock")
            .dataflows
            .insert(request_id, tx);
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
        let mut g = self.shared.lock().expect("lock");
        let state = g
            .subscribes
            .get_mut(&id)
            .ok_or_else(|| anyhow::anyhow!("no subscribe registered for {id}"))?;
        if let Some(e) = &state.error {
            anyhow::bail!("subscribe {id} reported an error: {e}");
        }
        Ok(std::mem::take(&mut state.updates))
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
            dataflows: BTreeMap::new(),
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

    #[mz_ore::test]
    fn progress_survives_gaps_between_waiters() {
        let shared = empty_shared();
        let responses = Responses {
            shared: Arc::clone(&shared),
        };
        let id = GlobalId::Transient(1);
        for timestamp in [5, 7] {
            Responses::dispatch(
                &shared,
                ComputeResponse::Frontiers(
                    id,
                    FrontiersResponse {
                        output_frontier: Some(Antichain::from_elem(Timestamp::from(timestamp))),
                        ..Default::default()
                    },
                ),
            );
            // The previous receiver is gone before the next update arrives.
            assert_eq!(
                responses.frontier(id).borrow().output_frontier,
                Some(Antichain::from_elem(Timestamp::from(timestamp)))
            );
        }
        drop(responses.ensure_subscribe(id));
        Responses::dispatch(
            &shared,
            ComputeResponse::SubscribeResponse(id, SubscribeResponse::DroppedAt(Antichain::new())),
        );
        assert!(responses.ensure_subscribe(id).borrow().is_empty());
    }
}
