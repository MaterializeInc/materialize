// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive cluster server.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread::Thread;

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use differential_dataflow::trace::ExertionLogic;
use futures::future;
use mz_cluster_client::client::{TimelyConfig, TryIntoTimelyConfig};
use mz_ore::error::ErrorExt;
use mz_ore::halt;
use mz_service::client::{GenericClient, Partitionable, Partitioned};
use mz_service::local::LocalClient;
use timely::WorkerConfig;
use timely::communication::Allocate;
use timely::communication::allocator::GenericBuilder;
use timely::communication::allocator::zero_copy::bytes_slab::BytesRefill;
use timely::communication::initialize::WorkerGuards;
use timely::execute::execute_from;
use timely::worker::Worker as TimelyWorker;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::communication::initialize_networking;

type PartitionedClient<C, R> = Partitioned<LocalClient<C, R>, C, R>;

/// A client managing access to the local portion of a Timely cluster
pub struct ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    /// The actual client to talk to the cluster
    inner: Option<PartitionedClient<C::Command, C::Response>>,
    /// The running timely instance
    timely_container: TimelyContainerRef<C>,
    /// The handle to the Tokio runtime.
    tokio_handle: tokio::runtime::Handle,
    cluster_spec: C,
}

/// Metadata about timely workers in this process.
pub struct TimelyContainer<C: ClusterSpec> {
    /// The current timely config in use
    config: TimelyConfig,
    /// Channels over which to send endpoints for wiring up a new Client
    client_txs: Vec<
        crossbeam_channel::Sender<(
            crossbeam_channel::Receiver<C::Command>,
            mpsc::UnboundedSender<C::Response>,
        )>,
    >,
    /// Thread guards that keep worker threads alive
    worker_guards: WorkerGuards<()>,
}

impl<C: ClusterSpec> TimelyContainer<C> {
    fn worker_threads(&self) -> Vec<Thread> {
        self.worker_guards
            .guards()
            .iter()
            .map(|h| h.thread().clone())
            .collect()
    }
}

impl<C: ClusterSpec> Drop for TimelyContainer<C> {
    fn drop(&mut self) {
        panic!("Timely container must never drop");
    }
}

/// Threadsafe reference to an optional TimelyContainer
type TimelyContainerRef<C> = Arc<tokio::sync::Mutex<Option<TimelyContainer<C>>>>;

impl<C> ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    /// Create a new `ClusterClient`.
    pub fn new(
        timely_container: TimelyContainerRef<C>,
        tokio_handle: tokio::runtime::Handle,
        cluster_spec: C,
    ) -> Self {
        Self {
            timely_container,
            inner: None,
            tokio_handle,
            cluster_spec,
        }
    }

    async fn build(&mut self, config: TimelyConfig) -> Result<(), Error> {
        let workers = config.workers;

        // Check if we can reuse the existing timely instance.
        // We currently do not support reinstantiating timely, we simply panic if another config is
        // requested. This code must panic before dropping the worker guards contained in
        // timely_container. As we don't terminate timely workers, the thread join would hang
        // forever, possibly creating a fair share of confusion in the orchestrator.

        let mut timely_container = self.timely_container.lock().await;
        match &*timely_container {
            Some(existing) => {
                if config != existing.config {
                    info!(new = ?config, old = ?existing.config, "TimelyConfig mismatch");
                    halt!("new timely configuration does not match existing timely configuration");
                }
                info!("Timely already initialized; re-using.",);
            }
            None => {
                let timely = self
                    .cluster_spec
                    .build_cluster(config, self.tokio_handle.clone())
                    .await
                    .inspect_err(|e| {
                        warn!("timely initialization failed: {}", e.display_with_causes())
                    })?;

                *timely_container = Some(timely);
            }
        };

        let timely = timely_container.as_ref().expect("set above");

        let mut command_txs = Vec::with_capacity(workers);
        let mut response_rxs = Vec::with_capacity(workers);
        for client_tx in &timely.client_txs {
            let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded();
            let (resp_tx, resp_rx) = mpsc::unbounded_channel();

            client_tx
                .send((cmd_rx, resp_tx))
                .expect("worker not dropped");

            command_txs.push(cmd_tx);
            response_rxs.push(resp_rx);
        }

        self.inner = Some(LocalClient::new_partitioned(
            response_rxs,
            command_txs,
            timely.worker_threads(),
        ));
        Ok(())
    }
}

impl<C> fmt::Debug for ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterClient")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<C> GenericClient<C::Command, C::Response> for ClusterClient<C>
where
    C: ClusterSpec,
    (C::Command, C::Response): Partitionable<C::Command, C::Response>,
{
    async fn send(&mut self, cmd: C::Command) -> Result<(), Error> {
        // Changing this debug statement requires changing the replica-isolation test
        tracing::debug!("ClusterClient send={:?}", &cmd);
        match cmd.try_into_timely_config() {
            Ok((config, _epoch)) => self.build(config).await,
            Err(cmd) => self.inner.as_mut().expect("initialized").send(cmd).await,
        }
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<C::Response>, Error> {
        if let Some(client) = self.inner.as_mut() {
            // `Partitioned::recv` is documented as cancel safe.
            client.recv().await
        } else {
            future::pending().await
        }
    }
}

/// Specification for a Timely cluster to which a [`ClusterClient`] connects.
///
/// This trait is used to make the [`ClusterClient`] generic over the compute and storage cluster
/// implementations.
#[async_trait]
pub trait ClusterSpec: Clone + Send + Sync + 'static {
    /// The cluster command type.
    type Command: fmt::Debug + Send + TryIntoTimelyConfig;
    /// The cluster response type.
    type Response: fmt::Debug + Send;

    /// Run the given Timely worker.
    fn run_worker<A: Allocate + 'static>(
        &self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<Self::Command>,
            mpsc::UnboundedSender<Self::Response>,
        )>,
    );

    /// Build a Timely cluster using the given config.
    async fn build_cluster(
        &self,
        config: TimelyConfig,
        tokio_executor: Handle,
    ) -> Result<TimelyContainer<Self>, Error> {
        info!("Building timely container with config {config:?}");
        let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

        let refill = if config.enable_zero_copy_lgalloc {
            BytesRefill {
                logic: Arc::new(|size| Box::new(alloc::lgalloc_refill(size))),
                limit: config.zero_copy_limit,
            }
        } else {
            BytesRefill {
                logic: Arc::new(|size| Box::new(vec![0; size])),
                limit: config.zero_copy_limit,
            }
        };

        let (builders, other) = if config.enable_zero_copy {
            use timely::communication::allocator::zero_copy::allocator_process::ProcessBuilder;
            initialize_networking::<ProcessBuilder>(
                config.workers,
                config.process,
                config.addresses.clone(),
                refill,
                GenericBuilder::ZeroCopyBinary,
            )
            .await?
        } else {
            initialize_networking::<timely::communication::allocator::Process>(
                config.workers,
                config.process,
                config.addresses.clone(),
                refill,
                GenericBuilder::ZeroCopy,
            )
            .await?
        };

        let mut worker_config = WorkerConfig::default();

        // We set a custom exertion logic for proportionality > 0. A proportionality value of 0
        // means that no arrangement merge effort is exerted and merging occurs only in response to
        // updates.
        if config.arrangement_exert_proportionality > 0 {
            let merge_effort = Some(1000);

            // ExertionLogic defines a function to determine if a spine is sufficiently tidied.
            // Its arguments are an iterator over the index of a layer, the count of batches in the
            // layer and the length of batches at the layer. The iterator enumerates layers from the
            // largest to the smallest layer.

            let arc: ExertionLogic = Arc::new(move |layers| {
                let mut prop = config.arrangement_exert_proportionality;

                // Layers are ordered from largest to smallest.
                // Skip to the largest occupied layer.
                let layers = layers
                    .iter()
                    .copied()
                    .skip_while(|(_idx, count, _len)| *count == 0);

                let mut first = true;
                for (_idx, count, len) in layers {
                    if count > 1 {
                        // Found an in-progress merge that we should continue.
                        return merge_effort;
                    }

                    if !first && prop > 0 && len > 0 {
                        // Found a non-empty batch within `arrangement_exert_proportionality` of
                        // the largest one.
                        return merge_effort;
                    }

                    first = false;
                    prop /= 2;
                }

                None
            });
            worker_config.set::<ExertionLogic>("differential/default_exert_logic".to_string(), arc);
        }

        let spec = self.clone();
        let worker_guards = execute_from(builders, other, worker_config, move |timely_worker| {
            let timely_worker_index = timely_worker.index();
            let _tokio_guard = tokio_executor.enter();
            let client_rx = client_rxs.lock().unwrap()[timely_worker_index % config.workers]
                .take()
                .unwrap();
            spec.run_worker(timely_worker, client_rx);
        })
        .map_err(|e| anyhow!(e))?;

        Ok(TimelyContainer {
            config,
            client_txs,
            worker_guards,
        })
    }
}

mod alloc {
    /// A Timely communication refill function that uses lgalloc.
    ///
    /// Returns a handle to lgalloc'ed memory if lgalloc can handle
    /// the request, otherwise we fall back to a heap allocation. In either case, the handle must
    /// be dropped to free the memory.
    pub(crate) fn lgalloc_refill(size: usize) -> LgallocHandle {
        match lgalloc::allocate::<u8>(size) {
            Ok((pointer, capacity, handle)) => {
                let handle = Some(handle);
                LgallocHandle {
                    handle,
                    pointer,
                    capacity,
                }
            }
            Err(_) => {
                // Allocate memory
                let mut alloc = vec![0_u8; size];
                // Ensure that the length matches the capacity.
                alloc.shrink_to_fit();
                // Get a pointer to the allocated memory.
                let pointer = std::ptr::NonNull::new(alloc.as_mut_ptr()).unwrap();
                // Forget the vector to avoid dropping it. We'll free the memory in `drop`.
                std::mem::forget(alloc);
                LgallocHandle {
                    handle: None,
                    pointer,
                    capacity: size,
                }
            }
        }
    }

    /// A handle to memory allocated by lgalloc. This can either be memory allocated by lgalloc or
    /// memory allocated by Vec. If the handle is set, it's lgalloc memory. If the handle is None,
    /// it's a regular heap allocation.
    pub(crate) struct LgallocHandle {
        /// Lgalloc handle, set if the memory was allocated by lgalloc.
        handle: Option<lgalloc::Handle>,
        /// Pointer to the allocated memory. Always well-aligned, but can be dangling.
        pointer: std::ptr::NonNull<u8>,
        /// Capacity of the allocated memory in bytes.
        capacity: usize,
    }

    impl std::ops::Deref for LgallocHandle {
        type Target = [u8];
        #[inline(always)]
        fn deref(&self) -> &Self::Target {
            unsafe { std::slice::from_raw_parts(self.pointer.as_ptr(), self.capacity) }
        }
    }

    impl std::ops::DerefMut for LgallocHandle {
        #[inline(always)]
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { std::slice::from_raw_parts_mut(self.pointer.as_ptr(), self.capacity) }
        }
    }

    impl Drop for LgallocHandle {
        fn drop(&mut self) {
            // If we have a handle, it's lgalloc memory. Otherwise, it's a heap allocation.
            if let Some(handle) = self.handle.take() {
                lgalloc::deallocate(handle);
            } else {
                unsafe { Vec::from_raw_parts(self.pointer.as_ptr(), 0, self.capacity) };
            }
            // Update pointer and capacity such that we don't double-free.
            self.pointer = std::ptr::NonNull::dangling();
            self.capacity = 0;
        }
    }
}
