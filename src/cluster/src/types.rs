// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types and traits required to setup clusters.

use mz_service::local::LocalActivator;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

/// A trait for letting specific server implementations hook
/// into handling of `CreateTimely` commands. Usually implemented by
/// the config object that are specific to the implementation.
pub trait AsRunnableWorker<C, R> {
    /// Build and continuously run a worker. Called on each timely
    /// thread.
    fn build_and_run<A: timely::communication::Allocate + 'static>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<C>,
            mpsc::UnboundedSender<R>,
            mpsc::UnboundedSender<LocalActivator>,
        )>,
    );
}
