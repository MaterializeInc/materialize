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
use mz_compute_client::protocol::response::{ComputeResponse, FrontiersResponse, PeekResponse};
use mz_repr::GlobalId;
use mz_service::client::GenericClient;
use tokio::sync::{broadcast, oneshot, watch};

use crate::ctp::ComputeCtpClient;

type FrontierTx = watch::Sender<FrontiersResponse>;
type FrontierRx = watch::Receiver<FrontiersResponse>;

struct Shared {
    frontiers: BTreeMap<GlobalId, FrontierTx>,
    peeks: BTreeMap<uuid::Uuid, oneshot::Sender<PeekResponse>>,
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
                let _ = tx.send(cur);
            }
            ComputeResponse::PeekResponse(uuid, pr, _otel) => {
                if let Some(tx) = g.peeks.remove(&uuid) {
                    let _ = tx.send(pr);
                }
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
}
