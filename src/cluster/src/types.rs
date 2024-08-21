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

use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_txn_wal::operator::TxnsContext;
use timely::worker::Worker as TimelyWorker;

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
    fn build_and_run<A: timely::communication::Allocate + 'static>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<C>,
            tokio::sync::mpsc::UnboundedSender<R>,
            tokio::sync::mpsc::UnboundedSender<Self::Activatable>,
        )>,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
    );
}
