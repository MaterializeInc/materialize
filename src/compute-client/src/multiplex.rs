// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A process-level command/response multiplexer over two compute runtimes.
//!
//! A clusterd process can host two compute runtimes: a `Maintenance` runtime that renders durable,
//! maintained work, and an `Interactive` runtime that serves ephemeral peeks. The compute
//! controller still connects to a single endpoint. [`Multiplexer`] bridges the two: it presents one
//! [`ComputeClient`] to the controller, routes each command to the runtime that owns the referenced
//! work, and merges the two response streams back into one.
//!
//! Routing is derived entirely from command contents (see [`Multiplexer::send`]).
//!
//! The split would otherwise lose one invariant: an index's `since` must not pass the `as_of` of a
//! dataflow importing it. A single command stream ordered the create against every later compaction.
//! Routing the two commands to different runtimes loses that, so `AllowCompaction` for a
//! maintenance-owned collection is *broadcast*: interactive sees it too, applies it as a standing hold
//! on the shared arrangement, and the publisher compacts only as far as the slower of the two runtimes
//! has applied. Interactive therefore has the create and the compactions that follow it back on one
//! ordered stream. See `doc/developer/design/20260720_two_runtime_compute/broadcast-compaction.md`.
//!
//! The multiplexer therefore does not modify compaction frontiers, and it holds no per-dataflow
//! state for the invariant. What keeps the arrangement readable is derived from the importing
//! runtime's own stream position rather than from anything tracked here, so a runtime that is
//! arbitrarily behind, or that never processes the create at all, cannot break it.
//!
//! State is therefore only which runtime renders each transient collection (`transient_owner`). It is
//! per-connection and discarded by `Hello`, see `Multiplexer::reset`.
//!
//! The multiplexer does not deduplicate peek responses. The exactly-one-`PeekResponse`-per-uuid
//! contract is already upheld below and above it: the per-worker `PartitionedComputeState` inside
//! each process collapses a cancel-versus-complete split across that process's workers into one
//! response, and the controller's per-process `PartitionedComputeState` merges one response per
//! process. Peeks route only to the interactive runtime, so the multiplexer receives exactly one
//! `PeekResponse` per uuid and forwards it verbatim. A multiplexer on a non-zero process never
//! observes the originating `Peek` command anyway (commands other than `Hello`/`UpdateConfiguration`
//! are sent to process 0 only, reaching other processes' workers through the intra-runtime command
//! channel), so it cannot gate responses on having seen the command.

use std::collections::BTreeSet;

use async_trait::async_trait;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::ComputeResponse;
use crate::service::ComputeClient;

/// Which of a process's two compute runtimes a piece of work lives on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Runtime {
    /// The runtime that renders durable, maintained collections.
    Maintenance,
    /// The runtime that serves ephemeral, interactive peeks.
    Interactive,
}

/// A single [`ComputeClient`] presented to the controller over two compute runtimes.
///
/// See the module documentation for the routing and merge policy.
#[derive(Debug)]
pub struct Multiplexer {
    /// The runtime that renders durable, maintained collections.
    maintenance: Box<dyn ComputeClient>,
    /// The runtime that serves ephemeral, interactive peeks.
    interactive: Box<dyn ComputeClient>,
    /// The transient collections rendered by the interactive runtime, learned from `CreateDataflow`.
    ///
    /// Only interactive-owned transient ids are recorded. Maintenance is the default in `owner_of`,
    /// so this is a set rather than a map. An entry is evicted when the collection's
    /// `AllowCompaction` reaches the empty frontier, so the set does not grow without bound.
    transient_owner: BTreeSet<GlobalId>,
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
            transient_owner: BTreeSet::new(),
        }
    }

    /// Discards all per-connection routing state.
    ///
    /// A `Hello` opens a new protocol epoch: the controller then replays its command history, which
    /// re-establishes ownership from the replayed `CreateDataflow`s.
    fn reset(&mut self) {
        self.transient_owner.clear();
    }

    /// The runtime that owns `id`. A recorded transient owner wins, otherwise maintenance.
    fn owner_of(&self, id: GlobalId) -> Runtime {
        if self.transient_owner.contains(&id) {
            Runtime::Interactive
        } else {
            Runtime::Maintenance
        }
    }

    /// A mutable handle to the client for `runtime`.
    fn client_mut(&mut self, runtime: Runtime) -> &mut Box<dyn ComputeClient> {
        match runtime {
            Runtime::Maintenance => &mut self.maintenance,
            Runtime::Interactive => &mut self.interactive,
        }
    }

    /// Decides whether a response received from `source` is forwarded to the controller.
    ///
    /// Only `Frontiers` reports are filtered; every other response forwards verbatim.
    ///
    /// Each runtime reports frontiers only for collections it exclusively hosts, so the two streams
    /// never overlap on a collection id:
    ///
    /// * The maintenance runtime hosts every durable, maintained collection, plus the internally
    ///   created logging/introspection indexes, and owns their frontiers. Its transient collections
    ///   are subscribes and copy-tos, which do not emit `Frontiers` (they report through
    ///   `SubscribeResponse`/`CopyToResponse`). So maintenance reports frontiers only for
    ///   non-transient ids.
    /// * The interactive runtime hosts only wholly-transient query dataflows. It installs empty
    ///   copies of maintenance's introspection indexes but does not report their frontiers (see
    ///   `report_frontiers`, which reports only transient collections on the interactive runtime). So
    ///   interactive reports frontiers only for transient ids.
    ///
    /// Filtering on `id.is_transient()` for the interactive source captures that split exactly. It
    /// deliberately does not consult `transient_owner`: that map is evicted when a collection's
    /// `AllowCompaction{empty}` drop is forwarded, which races ahead of the collection's final
    /// (empty) frontier reports. Gating on ownership would drop those trailing reports, so the
    /// controller would never observe the collection's frontiers reach the empty antichain, would
    /// never run `cleanup_collections` for it, and would strand its read holds on its inputs (a stale
    /// `since` on any upstream index/MV the transient read). Forwarding on `is_transient()` delivers
    /// every frontier report for the collections interactive owns, terminal or not.
    fn filter_response(
        &self,
        source: Runtime,
        response: ComputeResponse,
    ) -> Option<ComputeResponse> {
        match response {
            ComputeResponse::Frontiers(id, frontiers) => {
                let forward = match source {
                    Runtime::Maintenance => true,
                    Runtime::Interactive => id.is_transient(),
                };
                forward.then_some(ComputeResponse::Frontiers(id, frontiers))
            }
            other => Some(other),
        }
    }
}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for Multiplexer {
    async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
        use ComputeCommand::*;

        match command {
            // Lifecycle commands drive both runtimes. Send to maintenance first, then interactive.
            // A failure on either surfaces via `?` rather than being swallowed.
            cmd @ Hello { .. } => {
                self.reset();
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            cmd @ (CreateInstance(_) | InitializationComplete | UpdateConfiguration(_)) => {
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            CreateDataflow(desc) => {
                // Interactive serves a dataflow only when it is wholly transient, has a bounded
                // (non-empty) `until`, and carries no subscribe or copy-to sink. Transience is
                // required, not just a finite `until`: a durable dataflow can also get a finite
                // `until` (a `REFRESH AT` materialized view sets it to the last refresh, see
                // `create_materialized_view.rs`), and `filter_response` forwards interactive's
                // frontier reports only for transient ids. Routing such a dataflow to interactive
                // would make its frontier reports get dropped by that gate, so it must stay on
                // maintenance regardless of `until`. A finite `until` alone marks the dataflow as
                // an ephemeral read that stops on its own, safe to render outside the durable,
                // reconciled maintenance runtime. Subscribes stay on maintenance regardless of
                // `until`. Copy-to is transient and finite-until too, but it drives an S3 sink and
                // is refused by reconciliation, so it is excluded here for that reason, not a
                // frontier one.
                let to_interactive = desc.is_transient()
                    && !desc.until.is_empty()
                    && desc.subscribe_ids().next().is_none()
                    && desc.copy_to_ids().next().is_none();
                if to_interactive {
                    for id in desc.export_ids() {
                        self.transient_owner.insert(id);
                    }
                    self.interactive.send(CreateDataflow(desc)).await?;
                } else {
                    self.maintenance.send(CreateDataflow(desc)).await?;
                }
            }
            Schedule(id) => {
                let runtime = self.owner_of(id);
                self.client_mut(runtime).send(Schedule(id)).await?;
            }
            AllowWrites(id) => {
                let runtime = self.owner_of(id);
                self.client_mut(runtime).send(AllowWrites(id)).await?;
            }
            AllowCompaction { id, frontier } => {
                let runtime = self.owner_of(id);
                // The empty frontier drops the collection. Evict its ownership after forwarding so
                // `transient_owner` does not grow without bound.
                let dropping = frontier.is_empty();
                let evict = dropping && self.transient_owner.contains(&id);

                // Forwarded verbatim. The frontier is never modified: an importing dataflow's read is
                // protected by the standing hold the broadcast below advances, not by withholding
                // compaction here. That is also what removes the regression hazard a cap carries,
                // since the command history derives a dataflow's effective `as_of` from the last
                // frontier seen per export.
                self.client_mut(runtime)
                    .send(AllowCompaction {
                        id,
                        frontier: frontier.clone(),
                    })
                    .await?;

                // Broadcast to interactive as well, where the frontier advances the standing hold on
                // the shared arrangement rather than compacting a local trace. This is what puts the
                // create and the compactions that follow it on one ordered stream for the runtime that
                // renders the importing dataflow, so a compaction interactive has not applied cannot
                // advance the arrangement's `since` past the `as_of` of a create still queued there.
                //
                // Only for the collections interactive can import, which are the non-transient ones
                // maintenance publishes. Maintenance also owns transient collections, its subscribes
                // and copy-tos, and those are sinks with no arrangement for anything to import. Sending
                // one to interactive would hand it a frontier for a collection it has never installed,
                // and the drop in that sequence would ask it to drop what it does not have.
                if runtime == Runtime::Maintenance && !id.is_transient() {
                    self.interactive
                        .send(AllowCompaction { id, frontier })
                        .await?;
                }

                if evict {
                    self.transient_owner.remove(&id);
                }
            }
            Peek(peek) => {
                // Every peek is served by interactive.
                self.interactive.send(Peek(peek)).await?;
            }
            CancelPeek { uuid } => {
                // The peek lives on interactive, so its cancellation goes there too.
                self.interactive.send(CancelPeek { uuid }).await?;
            }
        }

        Ok(())
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. It `select!`s over the two inner `recv`s, each of which is
    /// cancel safe: dropping the non-selected branch loses no message, and dropping the whole
    /// future (the caller cancelling us) drops both inner futures without loss. The only value
    /// taken from an inner client is returned or dropped synchronously, with no intervening await,
    /// so a cancellation can never strand a response.
    ///
    /// This method never sends, so nothing here can be stranded half-done by a cancellation.
    async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
        loop {
            let (source, response) = tokio::select! {
                r = self.maintenance.recv() => (Runtime::Maintenance, r?),
                r = self.interactive.recv() => (Runtime::Interactive, r?),
            };
            match response {
                // Either runtime terminating ends the multiplexed endpoint. The caller must then
                // drop this client, matching the process's all-or-nothing runtime lifecycle.
                None => return Ok(None),
                Some(response) => {
                    if let Some(forward) = self.filter_response(source, response) {
                        return Ok(Some(forward));
                    }
                    // A dropped duplicate `PeekResponse` or a non-owner frontier report. Poll again
                    // for the next response.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
