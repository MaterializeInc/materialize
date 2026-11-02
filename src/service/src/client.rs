// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Traits for client–server communication independent of transport layer.
//!
//! These traits are designed for servers where where commands must be sharded
//! among several worker threads or processes.

use std::fmt;
use std::task::{Context, Poll};

use itertools::Itertools;
use tracing::trace;

/// A generic client to a server that receives commands and asynchronously
/// produces responses.
pub trait GenericClient<C, R>: fmt::Debug + Send {
    /// Sends a command to the dataflow server.
    ///
    /// The command can error for various reasons.
    fn send(&mut self, cmd: C) -> impl Future<Output = Result<(), anyhow::Error>> + Send;

    /// Receives the next response from the dataflow server.
    ///
    /// This method blocks until the next response is available.
    ///
    /// A return value of `Ok(Some(_))` transmits a response.
    ///
    /// A return value of `Ok(None)` indicates graceful termination of the
    /// connection. The owner of the client should not call `recv` again.
    ///
    /// A return value of `Err(_)` indicates an unrecoverable error. After
    /// observing an error, the owner of the client must drop the client.
    ///
    /// Implementations of this method **must** be [cancellation safe]. That
    /// means that work must not be lost if the future returned by this method
    /// is dropped.
    ///
    /// [cancellation safe]: https://docs.rs/tokio/latest/tokio/macro.select.html#cancellation-safety
    fn recv(&mut self) -> impl Future<Output = Result<Option<R>, anyhow::Error>> + Send {
        std::future::poll_fn(|cx| self.poll_recv(cx))
    }

    /// Polls for the next response from the dataflow server.
    ///
    /// Returns `Poll::Pending` and schedules `cx` for a wakeup if no response
    /// is available. Ready values have the same meaning as for
    /// [`GenericClient::recv`].
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<R>, anyhow::Error>>;
}

/// A client whose implementation is partitioned across a number of other
/// clients.
///
/// Such a client needs to broadcast (partitioned) commands to all of its
/// clients, and await responses from each of the client partitions before it
/// can respond.
#[derive(Debug)]
pub struct Partitioned<P, C, R>
where
    (C, R): Partitionable<C, R>,
{
    /// The individual partitions representing per-worker clients.
    pub parts: Vec<P>,
    /// The partitioned state.
    state: <(C, R) as Partitionable<C, R>>::PartitionedState,
    /// The part that `poll_recv` polls first.
    next_part: usize,
}

impl<P, C, R> Partitioned<P, C, R>
where
    (C, R): Partitionable<C, R>,
{
    /// Create a client partitioned across multiple client shards.
    pub fn new(parts: Vec<P>) -> Self {
        Self {
            state: <(C, R) as Partitionable<C, R>>::new(parts.len()),
            parts,
            next_part: 0,
        }
    }
}

impl<P, C, R> GenericClient<C, R> for Partitioned<P, C, R>
where
    P: GenericClient<C, R>,
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        trace!(command = ?cmd, "splitting command");
        let cmd_parts = self.state.split_command(cmd);
        for (index, (shard, cmd_part)) in self.parts.iter_mut().zip_eq(cmd_parts).enumerate() {
            if let Some(cmd) = cmd_part {
                trace!(shard = ?index, command = ?cmd, "sending command");
                shard.send(cmd).await?;
            }
        }
        Ok(())
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<R>, anyhow::Error>> {
        let parts = self.parts.len();
        loop {
            let mut received = false;
            let mut all_done = true;
            // Start each pass after the part polled last, so that a part with a steady supply of
            // responses cannot starve the others.
            for _ in 0..parts {
                let index = self.next_part;
                self.next_part = (index + 1) % parts;
                match self.parts[index].poll_recv(cx) {
                    Poll::Pending => all_done = false,
                    Poll::Ready(Ok(None)) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(Some(response))) => {
                        received = true;
                        all_done = false;
                        trace!(shard = ?index, response = ?response, "received response");
                        if let Some(response) = self.state.absorb_response(index, response) {
                            trace!(response = ?response, "returning response");
                            return Poll::Ready(response.map(Some));
                        }
                    }
                }
            }
            if all_done {
                // Indicate completion of the communication.
                return Poll::Ready(Ok(None));
            }
            if !received {
                // Every part that is not done returned `Pending` and registered `cx`.
                return Poll::Pending;
            }
        }
    }
}

/// A trait for command–response pairs that can be partitioned across multiple
/// workers via [`Partitioned`].
pub trait Partitionable<C, R> {
    /// The type which functions as the state machine for the partitioning.
    type PartitionedState: PartitionedState<C, R>;

    /// Construct a [`PartitionedState`] for the command–response pair.
    fn new(parts: usize) -> Self::PartitionedState;
}

/// A state machine for a partitioned client that partitions commands across and
/// amalgamates responses from multiple partitions.
pub trait PartitionedState<C, R>: fmt::Debug + Send {
    /// Splits a command into multiple partitions.
    fn split_command(&mut self, command: C) -> Vec<Option<C>>;

    /// Absorbs a response from a single partition.
    ///
    /// If responses from all partitions have been absorbed, returns an
    /// amalgamated response.
    fn absorb_response(&mut self, shard_id: usize, response: R)
    -> Option<Result<R, anyhow::Error>>;
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use anyhow::anyhow;
    use futures::task::noop_waker;

    use super::*;

    /// A part that yields scripted responses, then `Ok(None)` if `done`, else `Pending`.
    #[derive(Debug)]
    struct ScriptedPart {
        responses: VecDeque<Result<i32, String>>,
        done: bool,
    }

    impl ScriptedPart {
        fn new(responses: impl IntoIterator<Item = Result<i32, String>>, done: bool) -> Self {
            Self {
                responses: responses.into_iter().collect(),
                done,
            }
        }
    }

    impl GenericClient<(), i32> for ScriptedPart {
        async fn send(&mut self, _cmd: ()) -> Result<(), anyhow::Error> {
            Ok(())
        }

        fn poll_recv(&mut self, _cx: &mut Context<'_>) -> Poll<Result<Option<i32>, anyhow::Error>> {
            match self.responses.pop_front() {
                Some(Ok(response)) => Poll::Ready(Ok(Some(response))),
                Some(Err(error)) => Poll::Ready(Err(anyhow!(error))),
                None if self.done => Poll::Ready(Ok(None)),
                None => Poll::Pending,
            }
        }
    }

    /// Passes through responses, except that it swallows negative ones.
    #[derive(Debug)]
    pub struct PassthroughState;

    impl PartitionedState<(), i32> for PassthroughState {
        fn split_command(&mut self, _command: ()) -> Vec<Option<()>> {
            unimplemented!()
        }

        fn absorb_response(
            &mut self,
            _shard_id: usize,
            response: i32,
        ) -> Option<Result<i32, anyhow::Error>> {
            (response >= 0).then_some(Ok(response))
        }
    }

    impl Partitionable<(), i32> for ((), i32) {
        type PartitionedState = PassthroughState;

        fn new(_parts: usize) -> PassthroughState {
            PassthroughState
        }
    }

    fn poll(client: &mut Partitioned<ScriptedPart, (), i32>) -> Poll<Result<Option<i32>, String>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        client.poll_recv(&mut cx).map_err(|error| error.to_string())
    }

    #[mz_ore::test]
    fn partitioned_poll_recv_round_robins_parts() {
        let mut client = Partitioned::new(vec![
            ScriptedPart::new([Ok(0), Ok(2), Ok(4)], false),
            ScriptedPart::new([Ok(1), Ok(3)], false),
        ]);
        let received: Vec<_> = std::iter::from_fn(|| match poll(&mut client) {
            Poll::Ready(response) => Some(response),
            Poll::Pending => None,
        })
        .collect();
        assert_eq!(
            received,
            vec![
                Ok(Some(0)),
                Ok(Some(1)),
                Ok(Some(2)),
                Ok(Some(3)),
                Ok(Some(4))
            ]
        );
    }

    #[mz_ore::test]
    fn partitioned_poll_recv_keeps_polling_after_absorbed_responses() {
        let mut client = Partitioned::new(vec![
            ScriptedPart::new([Ok(-1), Ok(-2)], false),
            ScriptedPart::new([Ok(-3), Ok(7)], false),
        ]);
        assert_eq!(poll(&mut client), Poll::Ready(Ok(Some(7))));
        assert_eq!(poll(&mut client), Poll::Pending);
    }

    #[mz_ore::test]
    fn partitioned_poll_recv_completes_when_all_parts_complete() {
        let mut client = Partitioned::new(vec![
            ScriptedPart::new([], true),
            ScriptedPart::new([Ok(5)], false),
        ]);
        assert_eq!(poll(&mut client), Poll::Ready(Ok(Some(5))));
        assert_eq!(poll(&mut client), Poll::Pending, "one part still open");
        client.parts[1].done = true;
        assert_eq!(poll(&mut client), Poll::Ready(Ok(None)));
        let mut empty = Partitioned::<ScriptedPart, (), i32>::new(vec![]);
        assert_eq!(poll(&mut empty), Poll::Ready(Ok(None)));
    }

    #[mz_ore::test]
    fn partitioned_poll_recv_returns_part_errors() {
        let mut client = Partitioned::new(vec![
            ScriptedPart::new([], false),
            ScriptedPart::new([Err("boom".into())], false),
        ]);
        assert_eq!(poll(&mut client), Poll::Ready(Err("boom".into())));
    }
}
