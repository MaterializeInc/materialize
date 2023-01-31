// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits required to setup clusters.

use std::sync::Arc;

use timely::worker::Worker as TimelyWorker;

use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_persist_client::cache::PersistClientCache;

/// A trait for letting specific server implementations hook
/// into handling of `CreateTimely` commands. Usually implemented by
/// the config object that are specific to the implementation.
pub trait AsRunnableWorker<C, R> {
    /// The `Activatable` type this server needs to be activated
    /// when being send new commands.
    // TODO(guswynn): cluster-unification: currently compute
    // and storage have different ways of interacting with the timely
    // threads from the grpc server. When the disparate internal
    // command flow techniques are merged, this type should go away.
    type Activatable: mz_service::local::Activatable + Send;

    /// Build and continuously run a worker. Called on each timely
    /// thread.
    fn build_and_run<A: timely::communication::Allocate>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<C>,
            tokio::sync::mpsc::UnboundedSender<R>,
            crossbeam_channel::Sender<Self::Activatable>,
        )>,
        persist_clients: Arc<PersistClientCache>,
    );
}

/// A trait for specific cluster commands that can be unpacked into
/// `CreateTimely` variants.
pub trait TryIntoTimelyConfig {
    /// Attempt to unpack `self` into a `(TimelyConfig, ClusterStartupEpoch)`. Otherwise,
    /// fail and return `self` back.
    fn try_into_timely_config(self) -> Result<(TimelyConfig, ClusterStartupEpoch), Self>
    where
        Self: Sized;
}

impl TryIntoTimelyConfig for mz_compute_client::protocol::command::ComputeCommand {
    fn try_into_timely_config(self) -> Result<(TimelyConfig, ClusterStartupEpoch), Self> {
        match self {
            mz_compute_client::protocol::command::ComputeCommand::CreateTimely {
                config,
                epoch,
            } => Ok((config, epoch)),
            cmd => Err(cmd),
        }
    }
}

impl TryIntoTimelyConfig for mz_storage_client::client::StorageCommand {
    fn try_into_timely_config(self) -> Result<(TimelyConfig, ClusterStartupEpoch), Self> {
        match self {
            mz_storage_client::client::StorageCommand::CreateTimely { config, epoch } => {
                Ok((config, epoch))
            }
            cmd => Err(cmd),
        }
    }
}
