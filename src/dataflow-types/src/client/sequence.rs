// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client that injects `CommandFrontier` messages.

use anyhow::Error;
use timely::progress::ChangeBatch;

use mz_ore::now::{EpochMillis, NowFn};

use crate::client::{ComputeClient, ComputeCommand, ComputeResponse, GenericClient};

#[derive(Debug)]
pub struct Sequenced<C> {
    /// A function returning the current time.
    now: NowFn,
    /// The last time that was announced, implicitly starts at the minimum time.
    last_time: EpochMillis,
    /// The underlying client.
    client: C,
}

impl<C> Sequenced<C> {
    /// Construct a new [Sequenced] from a client and a [NowFn].
    pub fn new(client: C, now: NowFn) -> Self {
        let last_time = 0;
        Self {
            client,
            now,
            last_time,
        }
    }

    /// Immutable access to the inner client.
    pub fn inner(&self) -> &C {
        &self.client
    }

    /// Mutable access to the inner client.
    pub fn immer_mut(&mut self) -> &mut C {
        &mut self.client
    }
}

#[async_trait::async_trait]
impl<C: ComputeClient<T>, T> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for Sequenced<C>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + std::fmt::Debug,
{
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), Error> {
        let now = (self.now)();
        if self.last_time < now {
            let mut changes = ChangeBatch::with_capacity(2);
            changes.extend([(self.last_time, -1), (now, 1)].into_iter());
            self.last_time = now;
            self.client
                .send(ComputeCommand::CommandFrontier(changes))
                .await?;
        }

        self.client.send(cmd).await
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, Error> {
        self.client.recv().await
    }
}
