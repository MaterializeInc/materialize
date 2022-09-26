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
use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use tokio_stream::StreamMap;

/// A generic client to a server that receives commands and asynchronously
/// produces responses.
#[async_trait]
pub trait GenericClient<C, R>: fmt::Debug + Send {
    /// Sends a command to the dataflow server.
    ///
    /// The command can error for various reasons.
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error>;

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
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error>;

    /// Returns an adapter that treats the client as a stream.
    ///
    /// The stream produces the responses that would be produced by repeated
    /// calls to `recv`.
    fn as_stream<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<R, anyhow::Error>> + Send + 'a>>
    where
        R: Send + 'a,
    {
        Box::pin(async_stream::stream!({
            loop {
                match self.recv().await {
                    Ok(Some(response)) => yield Ok(response),
                    Err(error) => yield Err(error),
                    Ok(None) => {
                        return;
                    }
                }
            }
        }))
    }
}

#[async_trait]
impl<C, R> GenericClient<C, R> for Box<dyn GenericClient<C, R>>
where
    C: Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        (**self).recv().await
    }
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
        }
    }
}

#[async_trait]
impl<P, C, R> GenericClient<C, R> for Partitioned<P, C, R>
where
    P: GenericClient<C, R>,
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        let cmd_parts = self.state.split_command(cmd);
        for (shard, cmd_part) in self.parts.iter_mut().zip(cmd_parts) {
            if let Some(cmd) = cmd_part {
                shard.send(cmd).await?;
            }
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        let mut stream: StreamMap<_, _> = self
            .parts
            .iter_mut()
            .map(|shard| shard.as_stream())
            .enumerate()
            .collect();
        while let Some((index, response)) = stream.next().await {
            match response {
                Err(e) => {
                    return Err(e);
                }
                Ok(response) => {
                    if let Some(response) = self.state.absorb_response(index, response) {
                        return response.map(Some);
                    }
                }
            }
        }
        // Indicate completion of the communication.
        Ok(None)
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
