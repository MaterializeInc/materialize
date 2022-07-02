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
use std::time::{Duration, Instant};

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
    /// This method blocks until the next response is available, or, if the
    /// dataflow server has been shut down, returns `None`.
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

/// Trait for clients that can be disconnected and reconnected.
#[async_trait]
pub trait Reconnect {
    fn disconnect(&mut self);
    async fn reconnect(&mut self);
}

/// Trait for clients that can connect to an address
pub trait FromAddr {
    fn from_addr(addr: String) -> Self;
}

#[derive(Debug)]
struct ReconnectionState {
    /// Why we are trying to reconnect
    reason: anyhow::Error,
    /// When we should actually begin reconnecting
    backoff_expiry: Instant,
    /// How many parts we have successfully reconnected already
    parts_reconnected: usize,
}

const PARTITIONED_INITIAL_BACKOFF: Duration = Duration::from_millis(1);

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
    /// When the current successful connection (if any) began
    last_successful_connection: Option<Instant>,
    /// The current backoff, for preventing crash loops
    backoff: Duration,
    /// `Some` if we are in the process of reconnecting to the parts
    reconnect: Option<ReconnectionState>,
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
            last_successful_connection: None,
            backoff: PARTITIONED_INITIAL_BACKOFF,
            reconnect: None,
        }
    }
}

impl<P, C, R> Partitioned<P, C, R>
where
    (C, R): Partitionable<C, R>,
    P: Reconnect,
{
    async fn try_reconnect(&mut self) -> anyhow::Error {
        // Having received an error from one part, we can assume all of them are dead, because of the shared-fate architecture.
        // Disconnect from all of them and try again.
        let reconnect_state = self
            .reconnect
            .as_mut()
            .expect("Must set `self.reconnect` before calling this function.");

        // We need to back off here, in case we're crashing because we were accidentally connected
        // to two different generations of the cluster -- e.g., reconnected some
        // partitions to a cluster that was already crashing, and some other ones
        // to the new one that was being booted up to replace it.
        //
        // The time such a state can persist for is assumed to be bounded, so the
        // backoff ensures we will eventually converge to a valid state.

        tokio::time::sleep_until(reconnect_state.backoff_expiry.into()).await;
        let prc = reconnect_state.parts_reconnected;
        for part in &mut self.parts[prc..] {
            part.reconnect().await;
            reconnect_state.parts_reconnected += 1;
        }

        self.last_successful_connection = Some(Instant::now());
        // 60 seconds is arbitrarily chosen as a maximum plausible backoff.
        // If Timely processes aren't realizing their buddies are down within that time,
        // something is seriously hosed with the network anyway and its unlikely things will work.
        self.backoff = (self.backoff * 2).min(Duration::from_secs(60));

        let ReconnectionState { reason, .. } = self
            .reconnect
            .take()
            .expect("Asserted to exist at the beginning of the function");
        reason
    }
}

#[async_trait]
impl<P, C, R> GenericClient<C, R> for Partitioned<P, C, R>
where
    P: GenericClient<C, R> + Reconnect,
    (C, R): Partitionable<C, R>,
    C: fmt::Debug + Send,
    R: fmt::Debug + Send,
{
    async fn send(&mut self, cmd: C) -> Result<(), anyhow::Error> {
        if self.reconnect.is_some() {
            anyhow::bail!("client is reconnecting");
        }
        let cmd_parts = self.state.split_command(cmd);
        for (shard, cmd_part) in self.parts.iter_mut().zip(cmd_parts) {
            shard.send(cmd_part).await?;
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<R>, anyhow::Error> {
        if self.reconnect.is_some() {
            return Err(self.try_reconnect().await);
        }
        let mut stream: StreamMap<_, _> = self
            .parts
            .iter_mut()
            .map(|shard| shard.as_stream())
            .enumerate()
            .collect();
        while let Some((index, response)) = stream.next().await {
            match response {
                Err(e) => {
                    drop(stream);
                    // If we were previously up for long enough (60 seconds chosen arbitrarily), we consider the previous connection to have
                    // been successful and reset the backoff.
                    if let Some(prev) = self.last_successful_connection {
                        if Instant::now() - prev > Duration::from_secs(60) {
                            self.backoff = PARTITIONED_INITIAL_BACKOFF;
                        }
                    }

                    for p in &mut self.parts {
                        p.disconnect();
                    }

                    self.reconnect = Some(ReconnectionState {
                        reason: e,
                        backoff_expiry: Instant::now() + self.backoff,
                        parts_reconnected: 0,
                    });
                    return Err(self.try_reconnect().await);
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
    fn split_command(&mut self, command: C) -> Vec<C>;

    /// Absorbs a response from a single partition.
    ///
    /// If responses from all partitions have been absorbed, returns an
    /// amalgamated response.
    fn absorb_response(&mut self, shard_id: usize, response: R)
        -> Option<Result<R, anyhow::Error>>;
}
