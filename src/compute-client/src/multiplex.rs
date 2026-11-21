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
//! compute controller still connects to a single endpoint. [`Multiplexer`] presents one compute
//! client over the two and decides nothing about placement.
//!
//! Every command is forwarded to both runtimes, except `Peek` and `CancelPeek`, which go to the
//! interactive runtime, because in a two-runtime process it serves every peek. Each runtime
//! consumes its commands at its own rate, so the order of the two sends guarantees nothing.
//! A runtime decides from a `CreateDataflow`'s `DataflowClass` whether it renders the dataflow, so
//! placement is the controller's decision. Apart from peeks, both runtimes receive the same
//! commands in the same order, and a command means the same thing on either.
//!
//! Responses from both runtimes are merged. Each runtime reports frontiers only for the
//! collections it renders, which are disjoint, so frontier reports are forwarded verbatim.
//!
//! The multiplexer does not deduplicate peek responses. The exactly-one-`PeekResponse`-per-uuid
//! contract is already upheld below and above it: the `PartitionedComputeState` inside
//! each process collapses a cancel-versus-complete split across that process's workers into one
//! response, and the controller's per-process `PartitionedComputeState` merges one response per
//! process. Peeks reach only the interactive runtime, so the multiplexer receives exactly one
//! `PeekResponse` per uuid and forwards it verbatim.

use std::task::{Context, Poll};

use mz_service::client::GenericClient;

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::ComputeResponse;

/// A single compute client presented to the controller over two compute runtimes.
///
/// See the module documentation for what it forwards where.
#[derive(Debug)]
pub struct Multiplexer<M, I> {
    /// The runtime that renders maintained work.
    maintenance: M,
    /// The runtime that renders one-shot reads and serves peeks.
    interactive: I,
    /// Whether `poll_recv` polls the interactive runtime first, flipped on every response so
    /// that a runtime with a steady supply of responses cannot starve the other.
    interactive_first: bool,
}

impl<M, I> Multiplexer<M, I> {
    /// Wraps a maintenance and an interactive compute client into one multiplexed client.
    pub fn new(maintenance: M, interactive: I) -> Self {
        Self {
            maintenance,
            interactive,
            interactive_first: false,
        }
    }
}

impl<M, I> GenericClient<ComputeCommand, ComputeResponse> for Multiplexer<M, I>
where
    M: GenericClient<ComputeCommand, ComputeResponse>,
    I: GenericClient<ComputeCommand, ComputeResponse>,
{
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

    /// Polls both runtimes and returns the first response either has ready.
    ///
    /// Cancel safe, as `recv` requires: a response is returned in the same poll that takes it
    /// from an inner client, so no response is held across a suspension point.
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<ComputeResponse>, anyhow::Error>> {
        // Either runtime terminating ends the multiplexed endpoint. The caller must then drop this
        // client, matching the process's all-or-nothing runtime lifecycle.
        let polled = if self.interactive_first {
            match self.interactive.poll_recv(cx) {
                Poll::Pending => self.maintenance.poll_recv(cx),
                ready @ Poll::Ready(_) => ready,
            }
        } else {
            match self.maintenance.poll_recv(cx) {
                Poll::Pending => self.interactive.poll_recv(cx),
                ready @ Poll::Ready(_) => ready,
            }
        };
        if polled.is_ready() {
            self.interactive_first = !self.interactive_first;
        }
        polled
    }
}

#[cfg(test)]
mod tests;
