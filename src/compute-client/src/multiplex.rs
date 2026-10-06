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
//! A clusterd process can host two compute runtimes: a `Maintenance` runtime that renders
//! maintained work, and an `Interactive` runtime that renders one-shot reads and serves peeks. The
//! compute controller still connects to a single endpoint. [`Multiplexer`] presents one
//! [`ComputeClient`] over the two and decides nothing about placement.
//!
//! Every command is forwarded to both runtimes, maintenance first, except `Peek` and `CancelPeek`,
//! which go to the interactive runtime, because in a two-runtime process it serves every peek.
//! A runtime decides from a `CreateDataflow`'s `DataflowClass` whether it renders the dataflow, so
//! placement is the controller's decision. Apart from peeks, both runtimes receive the same
//! commands in the same order, and a command means the same thing on either.
//!
//! Responses from both runtimes are merged. Each runtime reports frontiers only for the
//! collections it renders, which are disjoint, so frontier reports are forwarded verbatim.
//!
//! The multiplexer does not deduplicate peek responses. The exactly-one-`PeekResponse`-per-uuid
//! contract is already upheld below and above it: the per-worker `PartitionedComputeState` inside
//! each process collapses a cancel-versus-complete split across that process's workers into one
//! response, and the controller's per-process `PartitionedComputeState` merges one response per
//! process. Peeks reach only the interactive runtime, so the multiplexer receives exactly one
//! `PeekResponse` per uuid and forwards it verbatim.

use async_trait::async_trait;
use mz_service::client::GenericClient;

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::ComputeResponse;
use crate::service::ComputeClient;

/// A single [`ComputeClient`] presented to the controller over two compute runtimes.
///
/// See the module documentation for what it forwards where.
#[derive(Debug)]
pub struct Multiplexer {
    /// The runtime that renders maintained work.
    maintenance: Box<dyn ComputeClient>,
    /// The runtime that renders one-shot reads and serves peeks.
    interactive: Box<dyn ComputeClient>,
}

impl Multiplexer {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: Box<dyn ComputeClient>, interactive: Box<dyn ComputeClient>) -> Self {
        Self {
            maintenance,
            interactive,
        }
    }
}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for Multiplexer {
    async fn send(&mut self, command: ComputeCommand) -> Result<(), anyhow::Error> {
        match command {
            command @ (ComputeCommand::Peek(_) | ComputeCommand::CancelPeek { .. }) => {
                self.interactive.send(command).await
            }
            command => {
                self.maintenance.send(command.clone()).await?;
                self.interactive.send(command).await
            }
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. It `select!`s over the two inner `recv`s, each of which is
    /// cancel safe: dropping the non-selected branch loses no message, and dropping the whole
    /// future (the caller cancelling us) drops both inner futures without loss. The only value
    /// taken from an inner client is returned synchronously, with no intervening await,
    /// so a cancellation can never strand a response.
    async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
        // Either runtime terminating ends the multiplexed endpoint. The caller must then drop this
        // client, matching the process's all-or-nothing runtime lifecycle.
        tokio::select! {
            r = self.maintenance.recv() => r,
            r = self.interactive.recv() => r,
        }
    }
}

#[cfg(test)]
mod tests;
