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
//! Routing the two commands to different runtimes loses that, so `AllowCompaction` for an index
//! maintenance publishes is *broadcast*: interactive sees it too, applies it as a standing hold
//! on the shared arrangement, and the publisher compacts only as far as the slower of the two runtimes
//! has applied. Interactive therefore has the create and the compactions that follow it back on one
//! ordered stream, and the multiplexer never modifies a frontier.
//!
//! The state is the collections the controller has declared and not yet dropped, with the runtime
//! that hosts each and whether interactive may import it (`collections`). It is per-connection and
//! discarded by `Hello`, see `Multiplexer::reset`.
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

use std::collections::BTreeMap;

use async_trait::async_trait;
use mz_repr::GlobalId;
use mz_service::client::GenericClient;

use crate::protocol::command::{ComputeCommand, PeekTarget};
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

/// A collection the controller declared, through a `CreateDataflow` export or a `CreateInstance`
/// logging index.
#[derive(Clone, Copy, Debug)]
struct Collection {
    /// The runtime that hosts it.
    runtime: Runtime,
    /// Whether maintenance publishes it as an index, so that interactive may import it and must see
    /// its compactions.
    published: bool,
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
    /// The collections declared and not yet dropped. An entry is removed when the collection's
    /// `AllowCompaction` reaches the empty frontier, after which the protocol mentions it no more.
    collections: BTreeMap<GlobalId, Collection>,
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
            collections: BTreeMap::new(),
        }
    }

    /// Discards all per-connection routing state.
    ///
    /// A `Hello` opens a new protocol epoch: the controller then replays its command history, which
    /// re-establishes the state from the replayed `CreateInstance` and `CreateDataflow`s.
    fn reset(&mut self) {
        self.collections.clear();
    }

    /// Records a collection the controller declared.
    fn declare(&mut self, id: GlobalId, runtime: Runtime, published: bool) {
        let previous = self
            .collections
            .insert(id, Collection { runtime, published });
        mz_ore::soft_assert_or_log!(previous.is_none(), "collection {id} declared twice");
    }

    /// The declared collection `id`, which a command other than its declaration names.
    ///
    /// An undeclared id is a protocol violation. It is reported and treated as a collection on
    /// maintenance, where the command fails loudly against a collection the runtime does not know.
    fn collection(&self, id: GlobalId, command: &str) -> Collection {
        let collection = self.collections.get(&id).copied();
        mz_ore::soft_assert_or_log!(
            collection.is_some(),
            "{command} names collection {id}, which is undeclared or dropped",
        );
        collection.unwrap_or(Collection {
            runtime: Runtime::Maintenance,
            published: false,
        })
    }

    /// A mutable handle to the client for `runtime`.
    fn client_mut(&mut self, runtime: Runtime) -> &mut dyn ComputeClient {
        match runtime {
            Runtime::Maintenance => &mut *self.maintenance,
            Runtime::Interactive => &mut *self.interactive,
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
            CreateInstance(config) => {
                for id in config.logging.index_logs.values() {
                    self.declare(*id, Runtime::Maintenance, true);
                }
                self.maintenance
                    .send(CreateInstance(config.clone()))
                    .await?;
                self.interactive.send(CreateInstance(config)).await?;
            }
            cmd @ (InitializationComplete | UpdateConfiguration(_)) => {
                self.maintenance.send(cmd.clone()).await?;
                self.interactive.send(cmd).await?;
            }
            CreateDataflow(desc) => {
                // Interactive serves the dataflows that exist only to answer one peek. Each clause
                // of `is_peek_dataflow` earns its place here: transience because `recv` asserts that
                // interactive reports frontiers only for transient ids; single-time because the
                // shared-index import is a snapshot bounded one step past `as_of` and cannot feed a
                // dataflow that runs further; no subscribe because a subscribe never stops; no
                // copy-to because reconciliation refuses its S3 sink.
                if desc.is_peek_dataflow() {
                    // A peek dataflow reads published maintenance indexes, never another temporary
                    // collection, so routing needs to consider only this description. Nothing
                    // enforces that: were a peek dataflow ever to import a transient id, its
                    // producer might sit on the other runtime, where this import cannot reach it.
                    // Fail loudly rather than render something that silently finds no input.
                    //
                    // TODO(CPU-216): the durable fix is for the control plane to name the runtime
                    // in the dataflow description, since placement is its concern. That surfaces
                    // placement in the protocol, so it is deferred rather than folded in here.
                    mz_ore::soft_assert_or_log!(
                        desc.import_ids().all(|id| !id.is_transient()),
                        "peek dataflow imports a transient collection: exports={} imports={}",
                        desc.display_export_ids(),
                        desc.display_import_ids(),
                    );
                    for id in desc.export_ids() {
                        self.declare(id, Runtime::Interactive, false);
                    }
                    self.interactive.send(CreateDataflow(desc)).await?;
                } else {
                    for id in desc.export_ids() {
                        let published = desc.index_exports.contains_key(&id);
                        self.declare(id, Runtime::Maintenance, published);
                    }
                    self.maintenance.send(CreateDataflow(desc)).await?;
                }
            }
            Schedule(id) => {
                let runtime = self.collection(id, "Schedule").runtime;
                self.client_mut(runtime).send(Schedule(id)).await?;
            }
            AllowWrites(id) => {
                let runtime = self.collection(id, "AllowWrites").runtime;
                self.client_mut(runtime).send(AllowWrites(id)).await?;
            }
            AllowCompaction { id, frontier } => {
                let Collection { runtime, published } = self.collection(id, "AllowCompaction");
                // The empty frontier drops the collection.
                let dropping = frontier.is_empty();

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
                // Only for the indexes maintenance publishes, the collections interactive can import.
                // Materialized views, sinks, subscribes, and copy-tos have no arrangement to import.
                if published {
                    mz_ore::soft_assert_or_log!(
                        runtime == Runtime::Maintenance,
                        "published collection {id} is hosted on {runtime:?}",
                    );
                    self.interactive
                        .send(AllowCompaction { id, frontier })
                        .await?;
                }

                if dropping {
                    self.collections.remove(&id);
                }
            }
            Peek(peek) => {
                // A persist peek names a storage collection, which compute does not declare.
                if let PeekTarget::Index { id } = &peek.target {
                    self.collection(*id, "Peek");
                }
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
    /// taken from an inner client is returned synchronously, with no intervening await,
    /// so a cancellation can never strand a response.
    ///
    /// This method never sends, so nothing here can be stranded half-done by a cancellation.
    async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
        // `GenericClient::recv` is cancellation safe by invariant, so the losing branch drops no
        // message.
        let (source, response) = tokio::select! {
            r = self.maintenance.recv() => (Runtime::Maintenance, r?),
            r = self.interactive.recv() => (Runtime::Interactive, r?),
        };
        // Either runtime terminating ends the multiplexed endpoint. The caller must then drop this
        // client, matching the process's all-or-nothing runtime lifecycle.
        let Some(response) = response else {
            return Ok(None);
        };
        // The two runtimes host disjoint collections, so their frontier reports never overlap.
        // Interactive installs no logging dataflow and renders only peek dataflows, whose exports
        // are transient. A report from it for any other id means the runtimes disagree about who
        // hosts a collection, and forwarding it could regress the frontier the controller sees.
        if let ComputeResponse::Frontiers(id, _) = &response {
            mz_ore::soft_assert_or_log!(
                source == Runtime::Maintenance || id.is_transient(),
                "interactive runtime reported frontiers for non-transient collection {id}",
            );
        }
        Ok(Some(response))
    }
}

#[cfg(test)]
mod tests;
